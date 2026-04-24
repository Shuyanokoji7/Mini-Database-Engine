"""PEP 249 Connection and Cursor.

This is the layer users actually interact with. If your code already
knows how to talk to sqlite3, it knows how to talk to minidb::

    conn = minidb.connect("mydb")
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE id = ?", (1,))
    print(cur.fetchone())
    conn.commit()
    conn.close()

Everything below is shaped by PEP 249's required attributes and methods.
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence, Tuple

from . import parser as p
from .engine import Database, Engine
from .exceptions import InterfaceError, NotSupportedError, ProgrammingError


class Connection:
    """A PEP 249 Connection.

    Holds the Database (catalog + tables + indexes) and hands out
    cursors. Writes are in-memory until ``commit`` flushes them.
    """

    def __init__(self, database: str):
        self._db = Database(database)
        self._closed = False
        self._cursors: List["Cursor"] = []

    # --- PEP 249 methods -------------------------------------------
    def close(self):
        if self._closed:
            return
        # Best-effort: flush on close so data isn't silently lost.
        self._db.commit()
        for c in self._cursors:
            c._closed = True
        self._closed = True

    def commit(self):
        self._check_open()
        self._db.commit()

    def rollback(self):
        self._check_open()
        self._db.rollback()

    def cursor(self) -> "Cursor":
        self._check_open()
        cur = Cursor(self)
        self._cursors.append(cur)
        return cur

    # --- context manager sugar (common in Python DB drivers) -------
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()
        return False

    # --- internal ---------------------------------------------------
    def _check_open(self):
        if self._closed:
            raise InterfaceError("connection is closed")


class Cursor:
    """A PEP 249 Cursor.

    A cursor holds the result of the last query. ``execute`` runs a
    statement and fills in ``description`` + the internal result set,
    which the ``fetch*`` methods then consume.
    """

    def __init__(self, connection: Connection):
        self._conn = connection
        self._engine = Engine(connection._db)
        self._closed = False

        # PEP 249 attributes
        self.arraysize: int = 1
        self.rowcount: int = -1
        self.description: Optional[List[tuple]] = None
        self.lastrowid = None

        # Result set state
        self._results: List[Tuple[Any, ...]] = []
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
