"""minidb — a small PEP 249 compatible database engine for learning.

Usage::

    import minidb

    conn = minidb.connect("mydb")
    cur = conn.cursor()
    cur.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    cur.execute("INSERT INTO users VALUES (?, ?)", (1, "Ada"))
    conn.commit()

    cur.execute("SELECT * FROM users WHERE id = ?", (1,))
    print(cur.fetchone())   # (1, 'Ada')

    conn.close()

The module exposes:

  * ``connect(database)``  -- returns a ``Connection``
  * PEP 249 exception classes (``Error``, ``ProgrammingError``, ...)
  * The three module-level constants PEP 249 requires:
      - ``apilevel``    = "2.0"
      - ``threadsafety`` = 1     (threads may share the module only)
      - ``paramstyle``   = "qmark"  (placeholders are ``?``)
"""

from .api import Connection, Cursor
from .exceptions import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)

# --- PEP 249 module-level constants --------------------------------
apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"


def connect(database: str) -> Connection:
    """Open (or create) a database directory and return a Connection.

    ``database`` is a filesystem path. If it doesn't exist it will be
    created. Inside it you'll find ``_catalog.json`` and one
    ``<table>.data`` file per table.
    """
    return Connection(database)


__all__ = [
    "apilevel", "threadsafety", "paramstyle", "connect",
    "Connection", "Cursor",
    "Warning", "Error", "InterfaceError", "DatabaseError",
    "DataError", "OperationalError", "IntegrityError",
    "InternalError", "ProgrammingError", "NotSupportedError",
]
