"""Row storage for a single table.

Design choice: rows live in memory as a Python list; disk state is a
JSON-Lines file (one row per line). This is simple to read, simple to
debug (you can ``cat`` the file), and good enough to explain how a DB
separates its logical state from its persistent state.

On ``load`` we read the file into the list. On ``flush`` (called from
``Connection.commit``) we rewrite the file. Between those two points all
changes are purely in memory — this gives us transactional semantics
(rollback = re-load, commit = flush) for free.
"""

from __future__ import annotations

import json
import os
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
        self._load()

    # --- disk I/O ---------------------------------------------------
    @property
    def path(self) -> str:
        return _data_path(self.db_dir, self.name)

    def _load(self):
        """Read the JSONL file into ``self.rows``."""
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    self.rows.append(json.loads(line))
        except OSError as e:
            raise OperationalError(f"reading {self.path}: {e}") from e

    def flush(self):
        """Atomically write all live rows back to the JSONL file."""
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w") as f:
                for row in self.rows:
                    if row is None:  # tombstone — drop it on flush
                        continue
                    f.write(json.dumps(row))
                    f.write("\n")
            os.replace(tmp, self.path)
        except OSError as e:
            raise OperationalError(f"writing {self.path}: {e}") from e

    def reload(self):
        """Discard in-memory state, re-read from disk. Used by rollback."""
        self.rows = []
        self._load()

    def remove_file(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    # --- mutations --------------------------------------------------
    def insert(self, row: dict) -> int:
        """Append a row. Return its row id."""
        self.rows.append(row)
        return len(self.rows) - 1

    def delete(self, row_id: int):
        """Mark a row as deleted (tombstone). Keeps other row ids stable."""
        if not 0 <= row_id < len(self.rows):
            raise OperationalError(f"bad row id {row_id}")
        self.rows[row_id] = None

    def update(self, row_id: int, new_row: dict):
        if not 0 <= row_id < len(self.rows):
            raise OperationalError(f"bad row id {row_id}")
        if self.rows[row_id] is None:
            raise OperationalError(f"row {row_id} is deleted")
        self.rows[row_id] = new_row

    # --- reads ------------------------------------------------------
    def get(self, row_id: int) -> Optional[dict]:
        if not 0 <= row_id < len(self.rows):
            return None
        return self.rows[row_id]

    def scan(self) -> Iterator[Tuple[int, dict]]:
        """Yield (row_id, row) for every live row."""
        for rid, row in enumerate(self.rows):
            if row is not None:
                yield rid, row

    def live_rows(self) -> List[dict]:
        return [r for r in self.rows if r is not None]
