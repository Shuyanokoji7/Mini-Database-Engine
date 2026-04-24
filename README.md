# minidb

A small database engine built for learning. It implements the
[Python DB-API 2.0 (PEP 249)](https://peps.python.org/pep-0249/)
interface, so the code you write against `minidb` looks identical to
code you'd write against `sqlite3`, `psycopg2`, or any other
DB-API-compliant driver. That means no custom API layer is needed — any
tool that speaks DB-API 2.0 can use this engine.

```python
import minidb

conn = minidb.connect("mydb")
cur = conn.cursor()

cur.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
cur.execute("INSERT INTO users VALUES (?, ?, ?)", (1, "Ada", 36))
conn.commit()

cur.execute("SELECT * FROM users WHERE id = ?", (1,))   # hits the index
print(cur.fetchone())        # (1, 'Ada', 36)

conn.close()
```

## Project layout

Each module does one thing and is meant to be read top-to-bottom.

| File              | Responsibility                                             |
| ----------------- | ---------------------------------------------------------- |
| `exceptions.py`   | The PEP 249 exception hierarchy (`Error`, `IntegrityError`, ...). |
| `types.py`        | The tiny type system: `INTEGER`, `TEXT`, `REAL`.           |
| `catalog.py`      | Table schemas, persisted to `_catalog.json`.               |
| `storage.py`      | Per-table row storage (in-memory list, JSONL on disk).     |
| `index.py`        | Hash index for primary-key lookups.                        |
| `parser.py`       | SQL tokenizer + recursive-descent parser → AST.            |
| `engine.py`       | Executes an AST against catalog + storage + indexes.       |
| `api.py`          | `Connection` and `Cursor` — the PEP 249 surface.           |
| `__init__.py`     | Module-level `connect()`, `apilevel`, `threadsafety`, `paramstyle`. |

## How the pieces fit together

```
user code
    │
    ▼
  Cursor.execute(sql, params)             <--  api.py    (PEP 249 layer)
    │
    ▼
  parser.parse(sql) → AST                 <--  parser.py
    │
    ▼
  Engine.execute(ast, params)             <--  engine.py
    │   ├── reads Catalog (schemas)       <--  catalog.py
    │   ├── reads/writes Table (rows)     <--  storage.py
    │   └── consults HashIndex for WHERE pk = ?   <-- index.py
    ▼
  rows + PEP 249 description
```

## On-disk format

A database is just a directory. After the demo you get:

```
mydb/
├── _catalog.json     # list of tables and their columns
└── users.data        # one JSON object per row, one row per line
```

`users.data` looks like this — you can `cat` it, `grep` it, or edit
it with any text editor:

```
{"id": 1, "name": "Ada",  "age": 36}
{"id": 2, "name": "Alan", "age": 41}
```

## Transaction semantics

All changes are made to in-memory structures until you call
`conn.commit()`, which atomically rewrites the catalog and every
table's data file. `conn.rollback()` discards the in-memory state and
reloads from disk. `Connection` also works as a context manager:

```python
with minidb.connect("mydb") as conn:
    conn.cursor().execute("INSERT ...")
    # commit on normal exit, rollback on exception
```

## What's supported

| Statement | Form                                                              |
| --------- | ----------------------------------------------------------------- |
| CREATE    | `CREATE TABLE t (col TYPE [PRIMARY KEY], ...)`                    |
| DROP      | `DROP TABLE t`                                                    |
| INSERT    | `INSERT INTO t [(cols...)] VALUES (values...)`                    |
| SELECT    | `SELECT * \| col,... FROM t [WHERE cond]`                         |
| UPDATE    | `UPDATE t SET col = val [, col = val ...] [WHERE cond]`           |
| DELETE    | `DELETE FROM t [WHERE cond]`                                      |

**Types:** `INTEGER`, `TEXT`, `REAL`, plus `NULL`.
**Operators in WHERE:** `=`, `!=`, `<`, `>`, `<=`, `>=`, combined with
`AND` / `OR`.
**Placeholders:** `?` (`paramstyle = "qmark"`).

## What's deliberately left out

So the code stays short and readable:

- Joins, subqueries, `GROUP BY`, `ORDER BY`, `LIMIT`.
- Secondary indexes (only primary-key hash index).
- Concurrent writers (single connection at a time).
- B-trees, pages, buffer pools. (Interesting, but they double the
  code. Worth adding once the basics click.)

## Running the demo

```
python demo.py
```

It creates a `demo_db/` directory, exercises every operation, and
prints the resulting tables in a psql-style layout.
