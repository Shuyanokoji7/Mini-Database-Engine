"""PEP 249 Connection and Cursor.

This is the layer users actually interact with. If your code already
knows how to talk to sqlite3, it knows how to talk to minidb::

    conn = minidb.connect("mydb")
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE id = ?", (1,))
    print(cur.fetchone())
    conn.commit()
    conn.close()

Transactions
------------
DML statements take effect in memory immediately, so SELECTs see your
own writes. ``commit()`` writes a redo log to the WAL, fsyncs it,
flushes the data files, and truncates the WAL. ``rollback()`` walks the
in-memory undo log in reverse to restore prior state.

Crash recovery
--------------
On every ``connect()`` we read any leftover WAL records: committed
transactions are replayed on top of the data files, and uncommitted
records are discarded.
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence

from . import parser as p
from .engine import Database, Engine
from .exceptions import InterfaceError, NotSupportedError, ProgrammingError
from .index import HashIndex
from .wal import WAL


class Connection:
    """A PEP 249 Connection.

    Holds the Database (catalog + tables + indexes), a WAL, and the
    in-memory undo/redo logs for the active transaction. Hands out
    cursors. Writes are visible immediately to the same connection but
    aren't durable until ``commit()`` flushes them.
    """

    def __init__(self, database: str):
        self._db_path = database
        self._db = Database(database)
        self._closed = False
        self._cursors: List["Cursor"] = []

        # WAL + transaction state
        self.wal = WAL(database)
        self._undo_log: List[dict] = []   # for rollback
        self._redo_log: List[dict] = []   # written to WAL on commit
        self.txid = 0

        # Recovery: replay any committed-but-not-flushed records.
        self._recover()

    # =================================================================
    # PEP 249 lifecycle
    # =================================================================
    def close(self):
        if self._closed:
            return
        # PEP 249: a close with uncommitted work should NOT auto-commit.
        # Match that — drop pending changes, close cursors, close WAL.
        if self._undo_log or self._redo_log:
            try:
                self.rollback()
            except Exception:
                pass
        for c in self._cursors:
            c._closed = True
        try:
            self.wal.close()
        except Exception:
            pass
        self._closed = True

    def commit(self):
        self._check_open()
        if not self._redo_log:
            return  # nothing to commit

        self.txid += 1

        # 1) Append every redo record + a COMMIT marker, then fsync.
        for rec in self._redo_log:
            self.wal.log({**rec, "txid": self.txid})
        self.wal.log({"txid": self.txid, "op": "COMMIT"})
        self.wal.flush()  # WAL is now durable

        # 2) Flush the data files: catalog + every dirty table.
        self._db.commit()

        # 3) The WAL records are no longer needed (data is durable).
        self.wal.clear()

        # 4) Reset the in-memory transaction state.
        self._undo_log.clear()
        self._redo_log.clear()

    def rollback(self):
        self._check_open()
        # Walk the undo log in reverse, restoring the state.
        for record in reversed(self._undo_log):
            self._undo_apply(record)
        self._undo_log.clear()
        self._redo_log.clear()

    def cursor(self) -> "Cursor":
        self._check_open()
        cur = Cursor(self)
        self._cursors.append(cur)
        return cur

    # --- context manager sugar (common in Python DB drivers) ---------
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        # On clean exit, commit and close. On exception, rollback and close.
        # (sqlite3 leaves the connection open; we close it because in this
        # codebase the typical pattern is `with minidb.connect(path) as c:`.)
        if self._closed:
            return False
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()
        return False

    # =================================================================
    # Internal helpers used by the engine
    # =================================================================
    def _record_change(self, undo: dict, redo: dict):
        """Engine calls this after applying a DML change to memory."""
        self._undo_log.append(undo)
        self._redo_log.append(redo)

    def _discard_pending_txn_logs(self):
        """Forget the in-memory undo/redo state.

        Called by the engine after DDL (which auto-commits): a DDL
        operation makes any prior DML records non-replayable on top of
        the new schema, so we drop them rather than risk corrupted
        rollback.
        """
        self._undo_log.clear()
        self._redo_log.clear()

    def _check_open(self):
        if self._closed:
            raise InterfaceError("connection is closed")

    # =================================================================
    # Crash recovery
    # =================================================================
    def _recover(self):
        """Read the WAL; replay every COMMITted transaction.

        Records without a matching COMMIT marker are uncommitted (the
        previous process died mid-transaction) and discarded.
        """
        records = self.wal.read_all()
        if not records:
            return

        # Group records by txid; remember which txids were committed.
        groups: dict = {}
        committed: set = set()
        for rec in records:
            txid = rec.get("txid")
            if not txid:
                continue
            groups.setdefault(txid, []).append(rec)
            if rec.get("op") == "COMMIT":
                committed.add(txid)

        # Track the largest txid we've ever seen so future commits
        # don't reuse a number.
        if committed:
            self.txid = max(self.txid, max(committed))

        # Replay only committed txns, in txid order. Use idempotent
        # apply so repeated replays don't double-insert.
        replayed_anything = False
        for txid in sorted(committed):
            for rec in groups[txid]:
                if rec["op"] == "COMMIT":
                    continue
                self._apply_redo_safe(rec)
                replayed_anything = True

        # If we replayed anything, flush to disk and truncate the WAL.
        if replayed_anything:
            self._db.commit()
            self.wal.clear()
        else:
            # WAL only contained uncommitted junk — clear it.
            self.wal.clear()

    # =================================================================
    # Apply redo + undo against in-memory storage. These are the
    # operations the WAL records describe; they're written to be
    # idempotent so they're safe to replay during recovery.
    # =================================================================
    def _apply_redo_safe(self, record: dict):
        """Idempotently apply a single redo record."""
        table_name = record.get("table")
        if table_name is None or table_name not in self._db.storage:
            return
        table = self._db.storage[table_name]
        schema = self._db.catalog.get(table_name)
        pk = schema.primary_key
        idx = self._db.indexes.get(table_name)

        op = record["op"]
        if op == "INSERT":
            row = record["row"]
            # If a row with this PK already exists, skip (idempotent).
            if pk is not None and idx is not None and idx.contains(row.get(pk)):
                return
            row_id = table.insert(row)
            if pk is not None and idx is not None and row.get(pk) is not None:
                idx.insert(row[pk], row_id)

        elif op == "UPDATE":
            pk_val = record.get("pk")
            new_row = record["new_row"]
            if pk is None or idx is None:
                # Fallback: linear search — rare since most tables have a PK.
                for row_id, row in list(table.scan()):
                    if row == record["old_row"]:
                        table.update(row_id, new_row)
                        return
                return
            # Locate the row by old PK (or new PK if it's already moved).
            row_id = idx.find(pk_val)
            if row_id is None:
                row_id = idx.find(new_row.get(pk))
            if row_id is None:
                return
            current = table.get(row_id)
            if current is None:
                return
            # Reindex if the PK changed, then write the new row.
            if current.get(pk) != new_row.get(pk):
                idx.remove(current[pk])
                idx.insert(new_row[pk], row_id)
            table.update(row_id, new_row)

        elif op == "DELETE":
            pk_val = record.get("pk")
            if pk is not None and idx is not None and pk_val is not None:
                row_id = idx.find(pk_val)
                if row_id is None:
                    return
                idx.remove(pk_val)
                table.delete(row_id)
            else:
                target = record.get("row")
                for row_id, row in list(table.scan()):
                    if row == target:
                        table.delete(row_id)
                        return

    def _undo_apply(self, record: dict):
        """Apply a single undo record."""
        table_name = record.get("table")
        if table_name is None or table_name not in self._db.storage:
            return
        table = self._db.storage[table_name]
        schema = self._db.catalog.get(table_name)
        pk = schema.primary_key
        idx = self._db.indexes.get(table_name)

        op = record["op"]
        if op == "INSERT_UNDO":
            # We inserted a row at this row_id. Tombstone it and
            # remove its PK from the index.
            row_id = record["row_id"]
            current = table.get(row_id)
            if current is None:
                # Already gone (rare - should not happen normally)
                pk_val = record.get("pk_value")
                if pk_val is not None and idx is not None:
                    idx.remove(pk_val)
                return
            if pk is not None and idx is not None:
                idx.remove(current.get(pk))
            table.delete(row_id)

        elif op == "UPDATE_UNDO":
            row_id = record["row_id"]
            old_row = record["old_row"]
            current = table.get(row_id)
            if current is None:
                # Row was deleted later in the same txn — re-insert
                # via the index machinery is awkward, so just leave it
                # tombstoned. (Multiple ops on the same row in one txn
                # will undo in reverse order, so this branch is rare.)
                return
            if pk is not None and idx is not None and current.get(pk) != old_row.get(pk):
                idx.remove(current[pk])
                if old_row.get(pk) is not None:
                    idx.insert(old_row[pk], row_id)
            table.update(row_id, old_row)

        elif op == "DELETE_UNDO":
            # We tombstoned this row; un-tombstone by re-inserting
            # the previous data at the same row_id slot.
            row_id = record["row_id"]
            row = record["row"]
            # Direct in-place replacement of the tombstone. We bypass
            # the public mutators because update() refuses tombstones,
            # and insert() would assign a new row_id.
            with table._write_lock:
                old_rows = table.rows
                if not 0 <= row_id < len(old_rows):
                    return
                if old_rows[row_id] is not None:
                    return  # not a tombstone — nothing to do
                new_rows = old_rows.copy()
                new_rows[row_id] = row
                with table._rcu_lock:
                    table.rows = new_rows
                    table._read_counts[id(new_rows)] = 0
                table.dirty = True
            table._synchronize_rcu(old_rows)
            if pk is not None and idx is not None and row.get(pk) is not None:
                if not idx.contains(row[pk]):
                    idx.insert(row[pk], row_id)


# ============================================================
# Cursor — full PEP 249 surface
# ============================================================

class Cursor:
    """A PEP 249 Cursor.

    A cursor holds the result of the last query. ``execute`` runs a
    statement and fills in ``description`` + the internal result set,
    which the ``fetch*`` methods then consume.
    """

    def __init__(self, connection: Connection):
        self._conn = connection
        self._engine = Engine(connection._db, connection)
        self._closed = False

        # PEP 249 attributes
        self.arraysize: int = 1
        self.rowcount: int = -1
        self.description: Optional[List[tuple]] = None
        self.lastrowid = None

        # Result set state
        self._results: List[tuple] = []
        self._result_pos: int = 0

    # --- PEP 249 methods -------------------------------------------
    def close(self):
        self._closed = True
        self._results = []
        self.description = None

    def execute(self, operation: str, parameters: Sequence[Any] = ()) -> "Cursor":
        self._check_open()
        if parameters is None:
            parameters = ()
        if not isinstance(parameters, (tuple, list)):
            raise ProgrammingError(
                "parameters must be a tuple or list (paramstyle='qmark')"
            )

        ast = p.parse(operation)
        rows, description, rowcount = self._engine.execute(ast, tuple(parameters))

        self._results = rows
        self._result_pos = 0
        self.description = description
        self.rowcount = rowcount
        return self

    def executemany(
        self, operation: str, seq_of_parameters: Sequence[Sequence[Any]]
    ) -> "Cursor":
        """Run the same SQL once per parameter tuple.

        ``rowcount`` afterwards is the sum of affected rows across all
        executions. PEP 249 also forbids this method on statements that
        produce a result set; we enforce that.
        """
        self._check_open()
        total = 0
        for params in seq_of_parameters:
            self.execute(operation, params)
            if self.description is not None:
                raise NotSupportedError(
                    "executemany() cannot be used with statements that return rows"
                )
            if self.rowcount >= 0:
                total += self.rowcount
        self.rowcount = total
        self._results = []
        self._result_pos = 0
        self.description = None
        return self

    # --- fetching ---------------------------------------------------
    def fetchone(self) -> Optional[tuple]:
        self._check_open()
        self._check_has_results()
        if self._result_pos >= len(self._results):
            return None
        row = self._results[self._result_pos]
        self._result_pos += 1
        return row

    def fetchmany(self, size: Optional[int] = None) -> List[tuple]:
        self._check_open()
        self._check_has_results()
        if size is None:
            size = self.arraysize
        end = self._result_pos + size
        batch = self._results[self._result_pos : end]
        self._result_pos = min(end, len(self._results))
        return batch

    def fetchall(self) -> List[tuple]:
        self._check_open()
        self._check_has_results()
        batch = self._results[self._result_pos :]
        self._result_pos = len(self._results)
        return batch

    # --- optional PEP 249 methods that are always no-ops for us ----
    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    # --- iteration protocol (an optional but very handy extension) -
    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    # --- internal ---------------------------------------------------
    def _check_open(self):
        if self._closed:
            raise InterfaceError("cursor is closed")
        if self._conn._closed:
            raise InterfaceError("connection is closed")

    def _check_has_results(self):
        if self.description is None:
            raise ProgrammingError("no result set; call execute() with a SELECT first")
