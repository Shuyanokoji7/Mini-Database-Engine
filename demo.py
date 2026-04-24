"""Demo: every code path worth showing, in one runnable file.

Run it with:   python demo.py

It creates a fresh database called ``demo_db`` in the current directory,
does a little of everything, and prints neatly formatted results.
"""

from __future__ import annotations

import os
import shutil

import minidb


def hr(title: str):
    print(f"\n--- {title} ".ljust(60, "-"))


def print_table(cur):
    """Render a cursor's results the way a psql/sqlite prompt would."""
    if cur.description is None:
        print("(no result set)")
        return
    headers = [d[0] for d in cur.description]
    rows = cur.fetchall()
    widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(str(val)))
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print(fmt.format(*(str(v) for v in row)))
    print(f"({len(rows)} row{'s' if len(rows) != 1 else ''})")


def main():
    db_path = "demo_db"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    # ----------------------------------------------------------------
    # PEP 249 metadata -- any DB-API 2.0 driver exposes these.
    # ----------------------------------------------------------------
    hr("PEP 249 module metadata")
    print(f"apilevel     = {minidb.apilevel!r}")
    print(f"threadsafety = {minidb.threadsafety!r}")
    print(f"paramstyle   = {minidb.paramstyle!r}")

    # ----------------------------------------------------------------
    # Connection lifecycle
    # ----------------------------------------------------------------
    conn = minidb.connect(db_path)
    cur = conn.cursor()

    hr("CREATE TABLE")
    cur.execute(
        "CREATE TABLE users ("
        "  id INTEGER PRIMARY KEY,"
        "  name TEXT,"
        "  age INTEGER"
        ")"
    )
    print("created: users")

    hr("INSERT (single row, with parameters)")
    cur.execute("INSERT INTO users VALUES (?, ?, ?)", (1, "Ada", 36))
    print(f"rowcount = {cur.rowcount}")

    hr("executemany (many rows at once)")
    cur.executemany(
        "INSERT INTO users VALUES (?, ?, ?)",
        [
            (2, "Alan",  41),
            (3, "Grace", 85),
            (4, "Linus", 55),
            (5, "Edsger", 72),
        ],
    )
    print(f"total inserted: {cur.rowcount}")

    hr("SELECT * (full scan)")
    cur.execute("SELECT * FROM users")
    print_table(cur)

    hr("SELECT with WHERE (uses hash index: WHERE id = ?)")
    cur.execute("SELECT id, name, age FROM users WHERE id = ?", (3,))
    print_table(cur)

    hr("SELECT with compound WHERE (index + filter)")
    cur.execute(
        "SELECT name, age FROM users WHERE id = ? AND age > ?",
        (5, 50),
    )
    print_table(cur)

    hr("SELECT with WHERE + AND/OR (full scan)")
    cur.execute(
        "SELECT name, age FROM users WHERE age < ? OR name = ?",
        (50, "Grace"),
    )
    print_table(cur)

    hr("UPDATE")
    cur.execute("UPDATE users SET age = ? WHERE id = ?", (37, 1))
    print(f"rowcount = {cur.rowcount}")
    cur.execute("SELECT id, name, age FROM users WHERE id = ?", (1,))
    print_table(cur)

    hr("DELETE")
    cur.execute("DELETE FROM users WHERE age > ?", (70,))
    print(f"rowcount = {cur.rowcount}")
    cur.execute("SELECT * FROM users")
    print_table(cur)

    # ----------------------------------------------------------------
    # Transactional behaviour: rollback undoes uncommitted work.
    # ----------------------------------------------------------------
    hr("Transaction: commit, then rollback uncommitted work")
    conn.commit()
    cur.execute("INSERT INTO users VALUES (?, ?, ?)", (99, "Temp", 1))
    cur.execute("SELECT * FROM users")
    print_table(cur)
    print(">> rolling back")
    conn.rollback()
    cur.execute("SELECT * FROM users")
    print_table(cur)

    # ----------------------------------------------------------------
    # Integrity and programming errors look just like any DB driver.
    # ----------------------------------------------------------------
    hr("Errors")
    try:
        cur.execute("INSERT INTO users VALUES (?, ?, ?)", (1, "Dup", 0))
    except minidb.IntegrityError as e:
        print(f"IntegrityError: {e}")
    try:
        cur.execute("SELECT nope FROM users")
    except minidb.ProgrammingError as e:
        print(f"ProgrammingError: {e}")

    # ----------------------------------------------------------------
    # Iteration protocol -- works just like sqlite3's cursor.
    # ----------------------------------------------------------------
    hr("Cursor iteration")
    cur.execute("SELECT name FROM users")
    for (name,) in cur:
        print(" -", name)

    conn.close()
    print("\nDone. Database files left in:", os.path.abspath(db_path))


if __name__ == "__main__":
    main()
