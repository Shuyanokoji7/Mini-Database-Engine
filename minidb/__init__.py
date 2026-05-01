"""minidb — a small PEP 249 compatible database engine for learning.

Features:
  * SQL: CREATE/DROP TABLE, INSERT, SELECT, UPDATE, DELETE
  * SELECT extras: WHERE (= != < > <= >= LIKE), AND/OR, ORDER BY, LIMIT,
    DISTINCT, COUNT(*)
  * NOT NULL and PRIMARY KEY constraints
  * Hash index on the primary key for fast WHERE pk = ? lookups
  * ACID transactions: commit / rollback, with an undo log
  * Write-Ahead Log (WAL) for durability and crash recovery
  * Thread-safe storage via Read-Copy-Update (RCU): readers never
    block writers and always see a consistent snapshot
  * Full DB-API 2.0 (PEP 249) surface

Example::

    import minidb

    conn = minidb.connect("mydb")
    cur = conn.cursor()
    cur.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    cur.execute("INSERT INTO users VALUES (?, ?)", (1, "Ada"))
    conn.commit()

    cur.execute("SELECT * FROM users WHERE id = ?", (1,))
    print(cur.fetchone())   # (1, 'Ada')

    conn.close()

Module-level constants required by PEP 249:
  * ``apilevel``    = "2.0"
  * ``threadsafety`` = 1   (threads may share the module only)
  * ``paramstyle``   = "qmark"  (placeholders are ``?``)
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

__version__ = "0.2.0"

# --- PEP 249 module-level constants --------------------------------
apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"


def connect(database: str) -> Connection:
    """Open (or create) a database directory and return a Connection.

    ``database`` is a filesystem path. If it doesn't exist it will be
    created. Inside it you'll find ``_catalog.json``, one
    ``<table>.data`` file per table, and a ``wal.log`` for durability.
    """
    return Connection(database)


__all__ = [
    "apilevel", "threadsafety", "paramstyle", "connect",
    "Connection", "Cursor",
    "Warning", "Error", "InterfaceError", "DatabaseError",
    "DataError", "OperationalError", "IntegrityError",
    "InternalError", "ProgrammingError", "NotSupportedError",
]
