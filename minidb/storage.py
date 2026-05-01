"""Row storage for a single table.

Design choice: rows live in memory as a Python list; disk state is a
JSON-Lines file (one row per line). This is simple to read, simple to
debug (you can ``cat`` the file), and good enough to explain how a DB
separates its logical state from its persistent state.

On ``load`` we read the file into the list. On ``flush`` we rewrite the
file. Between those two points all changes are purely in memory.

Concurrency
-----------
Storage is concurrency-safe via a small Read-Copy-Update (RCU) scheme.

* Readers (``scan``, ``get``, ``live_rows``) take a *snapshot* — they
  bump a reader counter on the current ``rows`` list and read from it.
  They never hold a lock during the scan, so they don't block writers.

* Writers (``insert``, ``update``, ``delete``) serialize on a single
  write lock. To make a change, a writer copies the rows list, mutates
  the copy, and atomically swaps it in. The old list is kept alive
  until every reader that started before the swap has released it.

This means readers always see a *consistent* snapshot — even while a
writer is partway through a sequence of mutations on another thread.

The ``dirty`` flag tells ``flush`` whether anything actually changed
since the last flush, so an idle table never touches disk.
"""

from __future__ import annotations

import json
import os
import threading
from typing import Iterator, List, Optional, Tuple

from .exceptions import OperationalError


def _data_path(db_dir: str, table: str) -> str:
    return os.path.join(db_dir, f"{table}.data")


class Table:
    """In-memory rows backed by a JSONL file on disk.

    ``rows`` is a list of dicts keyed by column name. Each row's position
    in the list is its "row id" — a stable integer within a session that
    the index uses to point into the table.
    """

    def __init__(self, db_dir: str, name: str):
        self.db_dir = db_dir
        self.name = name
        self.rows: List[Optional[dict]] = []  # None = deleted tombstone
        self.dirty = False

        # RCU state
        self._write_lock = threading.Lock()
        self._rcu_lock = threading.Lock()
        self._rcu_cond = threading.Condition(self._rcu_lock)
        self._read_counts = {}

        self._load()
        self._read_counts[id(self.rows)] = 0

    # ---- RCU primitives -------------------------------------------
    def _acquire_read(self) -> List[Optional[dict]]:
        with self._rcu_lock:
            current_rows = self.rows
            self._read_counts[id(current_rows)] = (
                self._read_counts.get(id(current_rows), 0) + 1
            )
            return current_rows

    def _release_read(self, rows: List[Optional[dict]]):
        with self._rcu_lock:
            self._read_counts[id(rows)] -= 1
            if self._read_counts[id(rows)] == 0:
                self._rcu_cond.notify_all()

    def _synchronize_rcu(self, old_rows: List[Optional[dict]]):
        """Wait for readers of old_rows to finish, then forget it."""
        with self._rcu_lock:
            while self._read_counts.get(id(old_rows), 0) > 0:
                self._rcu_cond.wait()
            if id(old_rows) in self._read_counts:
                del self._read_counts[id(old_rows)]
        # Local del so the snapshot can be GC'd; explicit but optional.
        del old_rows

    # --- disk I/O ---------------------------------------------------
    @property
    def path(self) -> str:
        return _data_path(self.db_dir, self.name)

    def _load(self, target_list=None):
        """Read the JSONL file into ``target_list`` (default self.rows)."""
        if target_list is None:
            target_list = self.rows
        if not os.path.exists(self.path):
            # No file yet => fresh table, mark dirty so flush() will
            # actually create the file (handy for empty tables).
            self.dirty = True
            return
        try:
            with open(self.path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    target_list.append(json.loads(line))
        except OSError as e:
            raise OperationalError(f"reading {self.path}: {e}") from e

    def flush(self):
        """Atomically write all live rows back to the JSONL file.

        Skips disk I/O entirely if the table hasn't been modified since
        the last flush.
        """
        # Atomically check dirty, capture snapshot, and reset dirty.
        with self._write_lock:
            if not self.dirty:
                return
            current_rows = self.rows
            self.dirty = False
            # Pin the snapshot under the RCU lock so it can't be GC'd.
            with self._rcu_lock:
                self._read_counts[id(current_rows)] = (
                    self._read_counts.get(id(current_rows), 0) + 1
                )

        try:
            tmp = self.path + ".tmp"
            try:
                with open(tmp, "w") as f:
                    for row in current_rows:
                        if row is None:  # tombstone — drop it on flush
                            continue
                        f.write(json.dumps(row))
                        f.write("\n")
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, self.path)
            except OSError as e:
                # Disk write failed — restore dirty so retries do work.
                self.dirty = True
                raise OperationalError(f"writing {self.path}: {e}") from e
        finally:
            self._release_read(current_rows)

    def reload(self):
        """Discard in-memory state, re-read from disk. Used by rollback."""
        with self._write_lock:
            old_rows = self.rows
            new_rows: List[Optional[dict]] = []
            self.dirty = False
            self._load(target_list=new_rows)
            with self._rcu_lock:
                self.rows = new_rows
                self._read_counts[id(new_rows)] = 0
            self._synchronize_rcu(old_rows)

    def remove_file(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    # --- mutations --------------------------------------------------
    def insert(self, row: dict) -> int:
        """Append a row. Return its row id."""
        with self._write_lock:
            old_rows = self.rows
            new_rows = old_rows.copy()
            new_rows.append(row)

            with self._rcu_lock:
                self.rows = new_rows
                self._read_counts[id(new_rows)] = 0

            self.dirty = True
            row_id = len(new_rows) - 1

        # Wait for readers of old_rows OUTSIDE the write lock so other
        # writers aren't blocked.
        self._synchronize_rcu(old_rows)
        return row_id

    def delete(self, row_id: int):
        """Mark a row as deleted (tombstone). Keeps other row ids stable."""
        old_rows = None
        with self._write_lock:
            old_rows = self.rows
            if not 0 <= row_id < len(old_rows):
                raise OperationalError(f"bad row id {row_id}")
            if old_rows[row_id] is not None:
                new_rows = old_rows.copy()
                new_rows[row_id] = None

                with self._rcu_lock:
                    self.rows = new_rows
                    self._read_counts[id(new_rows)] = 0

                self.dirty = True
            else:
                old_rows = None  # nothing changed, no need to wait

        if old_rows is not None:
            self._synchronize_rcu(old_rows)

    def update(self, row_id: int, new_row: dict):
        old_rows = None
        with self._write_lock:
            old_rows = self.rows
            if not 0 <= row_id < len(old_rows):
                raise OperationalError(f"bad row id {row_id}")
            if old_rows[row_id] is None:
                raise OperationalError(f"row {row_id} is deleted")
            if old_rows[row_id] != new_row:
                new_rows = old_rows.copy()
                new_rows[row_id] = new_row

                with self._rcu_lock:
                    self.rows = new_rows
                    self._read_counts[id(new_rows)] = 0

                self.dirty = True
            else:
                old_rows = None  # nothing changed

        if old_rows is not None:
            self._synchronize_rcu(old_rows)

    # --- reads ------------------------------------------------------
    def get(self, row_id: int) -> Optional[dict]:
        current_rows = self._acquire_read()
        try:
            if not 0 <= row_id < len(current_rows):
                return None
            return current_rows[row_id]
        finally:
            self._release_read(current_rows)

    def scan(self) -> Iterator[Tuple[int, dict]]:
        """Yield (row_id, row) for every live row in a stable snapshot."""
        current_rows = self._acquire_read()
        try:
            for rid, row in enumerate(current_rows):
                if row is not None:
                    yield rid, row
        finally:
            self._release_read(current_rows)

    def live_rows(self) -> List[dict]:
        current_rows = self._acquire_read()
        try:
            return [r for r in current_rows if r is not None]
        finally:
            self._release_read(current_rows)
