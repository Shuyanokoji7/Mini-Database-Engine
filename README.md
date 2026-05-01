# minidb

A small but real SQL database engine in pure Python. It is the merge of two
earlier project parts:

- **Part 1** contributed RCU (Read‑Copy‑Update) thread‑safe storage and a
  dirty‑flag flush optimisation.
- **Part 2** contributed a write‑ahead log, durable commit, undo‑based
  rollback, and crash recovery.

The unified engine keeps everything from both parts and adds several SQL
features on top.

```
+---------------+        +-----------------+        +-------------------+
|   PEP 249 API |  --->  |   SQL Engine    |  --->  |  RCU Storage      |
|  (api.py)     |        |  (engine.py)    |        |  (storage.py)     |
+---------------+        +-----------------+        +-------------------+
       |                         |                          |
       |                         |                          v
       v                         v                   data files (per table)
   WAL + undo log          parser + AST              ^
   (wal.py)                (parser.py)               |
                                                  flush on commit
```

## Features

### SQL surface

| Feature                                      | Example                                                   |
| -------------------------------------------- | --------------------------------------------------------- |
| `CREATE TABLE [IF NOT EXISTS]`               | `CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)`   |
| `DROP TABLE [IF EXISTS]`                     | `DROP TABLE IF EXISTS t`                                  |
| `INTEGER`, `TEXT`, `REAL` columns            | `name TEXT NOT NULL`                                      |
| Primary keys (auto‑indexed, hash)            | `id INTEGER PRIMARY KEY`                                  |
| `NOT NULL` constraint                        | `email TEXT NOT NULL`                                     |
| `INSERT INTO ... VALUES (...)`               | `INSERT INTO t VALUES (?, ?)`                             |
| `UPDATE ... SET ... WHERE ...`               | `UPDATE t SET n = ? WHERE id = ?`                         |
| `DELETE FROM ... WHERE ...`                  | `DELETE FROM t WHERE n > ?`                               |
| `SELECT ... FROM ... WHERE ...`              | `SELECT id, name FROM t WHERE id = ?`                     |
| `SELECT *`                                   | `SELECT * FROM t`                                         |
| `WHERE` with `AND` / `OR`, comparisons       | `WHERE n >= ? AND name = ?`                               |
| `LIKE` with `%` and `_` wildcards            | `WHERE email LIKE '%@example.com'`                        |
| `ORDER BY col [ASC|DESC]`, multi‑column      | `ORDER BY age DESC, name ASC`                             |
| `LIMIT n`                                    | `SELECT * FROM t LIMIT 10`                                |
| `DISTINCT`                                   | `SELECT DISTINCT status FROM orders`                      |
| `COUNT(*)`                                   | `SELECT COUNT(*) FROM users WHERE active = 1`             |
| Explicit `BEGIN` / `COMMIT` / `ROLLBACK`     | `cur.execute("ROLLBACK")`                                 |
| Parameter binding (`?` placeholders)         | `cur.execute("... WHERE id = ?", (5,))`                   |

### Engine

- **PEP 249 (DB‑API 2.0) compatible** — `connect()`, `Connection`, `Cursor`,
  `execute`, `executemany`, `fetchone`, `fetchmany`, `fetchall`, iteration
  over the cursor, context manager support, the standard exception
  hierarchy, and the module‑level `paramstyle = "qmark"`.
- **Hash index on primary keys**, used automatically for `WHERE pk = ?`
  lookups. A full scan is used otherwise.
- **NULL handling**: NULL never equals NULL in comparisons; `ORDER BY` puts
  NULLs first.

### Concurrency (from Part 1)

- **RCU storage**: readers never block writers and writers never block
  readers. A reader takes a snapshot reference of the row list under a tiny
  lock and then iterates the snapshot lock‑free. A writer copies the list,
  swaps it atomically, then waits only for in‑flight readers of the *old*
  snapshot to finish before reclaiming it.
- **Single‑writer per table**: writers serialise on a per‑table write lock.
- **Dirty flag**: `flush()` is a no‑op for tables that haven't been mutated
  since their last flush. The flag is set under the write lock so that
  concurrent writers can never have their changes ignored by a flusher
  that just cleared it.

### Durability and recovery (from Part 2)

- **Write‑ahead log** (`wal.log`): every committed transaction is written
  as JSON line records (`UPDATE_REDO`, `INSERT_REDO`, `DELETE_REDO`)
  followed by a `COMMIT` marker, then `fsync`ed.
- **Commit ordering**:
  ```
  write redo records  →  write COMMIT  →  fsync(WAL)
                      →  flush data files  →  truncate WAL
  ```
  A crash at any point is recoverable.
- **Undo log** (in‑memory): every mutation records an inverse operation on
  the connection so `rollback()` can restore prior state without going to
  disk.
- **Read‑your‑own‑writes**: DML applies to in‑memory storage *immediately*,
  before commit. A `SELECT` in the same transaction sees the
  uncommitted `INSERT`s/`UPDATE`s/`DELETE`s from the same connection.
- **Recovery on connect**: on `connect()`, the engine replays committed
  transactions from the WAL idempotently and discards any tail records
  lacking a `COMMIT` marker (or with a torn final line). After replay the
  data files are flushed and the WAL is truncated.
- **DDL is auto‑commit**: `CREATE TABLE` and `DROP TABLE` finalise their
  changes immediately and discard any pending DML undo/redo on the
  connection.

## Layout

```
minidb/
  __init__.py        package exports + module-level DB-API attributes
  api.py             Connection / Cursor (PEP 249 surface) + WAL/undo
  engine.py          SQL execution: planner + executor
  parser.py          tokeniser + recursive-descent parser → AST
  catalog.py         Schema (column defs, types, PK, NOT NULL)
  types.py           INTEGER / TEXT / REAL with coercion
  index.py           hash index used for PK lookups
  storage.py         RCU thread-safe Table with dirty-flag flush
  wal.py             append-only JSON-line write-ahead log
  exceptions.py      PEP 249 exception hierarchy

tests/
  test_main.py         51 unit tests across the SQL/PEP 249 surface
  test_concurrency.py  RCU snapshot, dirty-flag, and stress tests
demo.py              runnable end-to-end demo of every feature
```

## Quick start

```python
import minidb

conn = minidb.connect("./mydata")
cur  = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id    INTEGER PRIMARY KEY,
        name  TEXT NOT NULL,
        email TEXT
    )
""")

cur.executemany(
    "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
    [(1, "Alice", "a@x.com"), (2, "Bob", "b@x.com")],
)
conn.commit()

cur.execute("SELECT name FROM users WHERE email LIKE ? ORDER BY name", ("%@x.com",))
for (name,) in cur:
    print(name)

conn.close()
```

## Transactions

```python
# Implicit transaction: all DML is buffered into the current txn.
cur.execute("INSERT INTO t VALUES (?, ?)", (1, "Ada"))
cur.execute("UPDATE t SET name = ? WHERE id = ?", ("Bob", 1))
conn.commit()       # durably persisted via WAL + flush

cur.execute("DELETE FROM t WHERE id = ?", (1,))
conn.rollback()     # undone in-memory using the undo log

# Or explicit via SQL:
cur.execute("BEGIN")
cur.execute("INSERT INTO t VALUES (?, ?)", (2, "Cy"))
cur.execute("COMMIT")
```

`with minidb.connect(path) as conn:` commits on clean exit, rolls back on
exception, and closes the connection in either case.

## ACID summary

- **Atomicity**: rollback restores prior state via the undo log; on crash
  recovery, transactions without a `COMMIT` marker are discarded.
- **Consistency**: PK uniqueness and `NOT NULL` are enforced at every
  mutation, including replay.
- **Isolation**: each connection sees its own in‑progress writes (read‑
  your‑own‑writes); other connections do not see uncommitted changes
  *durably* (data files are only updated at commit time). The model is a
  single‑writer‑per‑table version of read‑committed.
- **Durability**: `commit()` writes a `COMMIT` marker and `fsync`s the WAL
  before flushing data files and truncating the WAL.

## Running tests and the demo

```
python tests/test_main.py         # 51 tests
python tests/test_concurrency.py  # 11 tests (~5s, runs real threads)
python demo.py                    # narrated end-to-end demo
```

## Limitations (known and intentional)

- No `JOIN`, no subqueries, no aggregates other than `COUNT(*)`.
- `WHERE` supports `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `LIKE`, plus
  `AND`/`OR`. No `IN (...)`, `BETWEEN`, or `NULL`/`NOT NULL` predicates.
- `ALTER TABLE` is not implemented.
- One database per directory; concurrent processes are not coordinated
  (the WAL is not multi‑process safe). Intra‑process threading is fully
  supported via RCU.
