"""End-to-end demo of minidb features.

Run: python demo.py
"""
import os
import shutil
import tempfile

import minidb


def banner(title):
    print()
    print("=" * 68)
    print(f"  {title}")
    print("=" * 68)


def main():
    tmpdir = tempfile.mkdtemp(prefix="minidb_demo_")
    try:
        run_demo(tmpdir)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def run_demo(data_dir):
    banner("1. Connect, create tables (with NOT NULL + IF NOT EXISTS)")
    conn = minidb.connect(data_dir)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id     INTEGER PRIMARY KEY,
            name   TEXT NOT NULL,
            email  TEXT,
            age    INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id  INTEGER PRIMARY KEY,
            user_id   INTEGER NOT NULL,
            amount    REAL NOT NULL,
            status    TEXT
        )
    """)
    print("Tables created: users, orders")

    banner("2. Bulk insert with executemany() and parameter binding")
    cur.executemany(
        "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)",
        [
            (1, "Alice",   "alice@example.com",   30),
            (2, "Bob",     "bob@example.com",     25),
            (3, "Charlie", "charlie@example.com", 35),
            (4, "Diana",   None,                  28),
            (5, "Eve",     "eve@example.com",     None),
        ],
    )
    cur.executemany(
        "INSERT INTO orders (order_id, user_id, amount, status) VALUES (?, ?, ?, ?)",
        [
            (101, 1, 49.99,  "shipped"),
            (102, 1, 19.95,  "pending"),
            (103, 2, 199.00, "shipped"),
            (104, 3, 5.50,   "cancelled"),
            (105, 3, 75.25,  "shipped"),
        ],
    )
    conn.commit()
    print("Inserted 5 users and 5 orders, committed.")

    banner("3. SELECT * with iterator protocol")
    cur.execute("SELECT * FROM users")
    for row in cur:           # cursor is its own iterator
        print(" ", row)

    banner("4. Indexed primary-key lookup")
    cur.execute("SELECT name, email FROM users WHERE id = ?", (3,))
    print(" ", cur.fetchone())

    banner("5. ORDER BY (multi-column, ASC/DESC, NULLs first)")
    cur.execute("SELECT name, age FROM users ORDER BY age DESC, name ASC")
    for row in cur.fetchall():
        print(" ", row)

    banner("6. LIMIT and DISTINCT")
    cur.execute("SELECT DISTINCT status FROM orders ORDER BY status")
    print("  distinct statuses:", cur.fetchall())
    cur.execute("SELECT name FROM users ORDER BY name LIMIT 3")
    print("  first 3 by name:  ", cur.fetchall())

    banner("7. COUNT(*)")
    cur.execute("SELECT COUNT(*) FROM users")
    print("  total users:", cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM orders WHERE status = ?", ("shipped",))
    print("  shipped orders:", cur.fetchone()[0])

    banner("8. LIKE wildcard search")
    cur.execute("SELECT name, email FROM users WHERE email LIKE ?", ("%@example.com",))
    print("  example.com users:")
    for row in cur:
        print("   ", row)
    cur.execute("SELECT name FROM users WHERE name LIKE ?", ("_li%",))
    print("  names matching _li% :", cur.fetchall())

    banner("9. UPDATE and DELETE")
    cur.execute("UPDATE orders SET status = ? WHERE status = ?", ("delivered", "shipped"))
    print(f"  rows updated: {cur.rowcount}")
    cur.execute("DELETE FROM orders WHERE status = ?", ("cancelled",))
    print(f"  rows deleted: {cur.rowcount}")
    conn.commit()

    banner("10. Transaction rollback (undo log)")
    cur.execute("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", (99, "Temp", 99))
    cur.execute("UPDATE users SET age = ? WHERE id = ?", (999, 1))
    print("  before rollback:")
    cur.execute("SELECT id, name, age FROM users ORDER BY id")
    for r in cur.fetchall():
        print("   ", r)
    conn.rollback()
    print("  after rollback:")
    cur.execute("SELECT id, name, age FROM users ORDER BY id")
    for r in cur.fetchall():
        print("   ", r)

    banner("11. Context-manager transaction (auto-commit on success)")
    # `with conn:` commits on clean exit and closes the connection.
    with minidb.connect(data_dir) as cx:
        curx = cx.cursor()
        curx.execute("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", (10, "Frank", 40))
        curx.execute("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", (11, "Gina",  22))
    # Reopen a fresh connection to verify and continue the demo.
    conn = minidb.connect(data_dir)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    print("  users after commit-on-exit:", cur.fetchone()[0])

    banner("12. Constraint violation")
    try:
        cur.execute("INSERT INTO users (id, name) VALUES (?, ?)", (1, "Dup"))
    except minidb.IntegrityError as e:
        print(f"  IntegrityError caught: {e}")
    try:
        cur.execute("INSERT INTO users (id, name) VALUES (?, ?)", (50, None))
    except minidb.IntegrityError as e:
        print(f"  IntegrityError caught: {e}")

    conn.close()

    banner("13. Persistence + WAL recovery across reconnects")
    conn2 = minidb.connect(data_dir)
    cur2  = conn2.cursor()
    cur2.execute("SELECT COUNT(*) FROM users")
    print("  users after reopen:", cur2.fetchone()[0])
    cur2.execute("SELECT COUNT(*) FROM orders")
    print("  orders after reopen:", cur2.fetchone()[0])

    banner("14. Crash-recovery simulation")
    cur2.execute("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", (200, "Survivor", 50))
    conn2.commit()                    # WAL written + data flushed + WAL truncated
    cur2.execute("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", (201, "Lost", 1))
    # simulate crash: do NOT commit, do NOT close cleanly
    del conn2
    conn3 = minidb.connect(data_dir)
    cur3  = conn3.cursor()
    cur3.execute("SELECT id, name FROM users WHERE id = ? OR id = ?", (200, 201))
    print("  after crash recovery (only id=200 should survive):")
    for r in cur3.fetchall():
        print("   ", r)
    conn3.close()

    print()
    print("Demo finished successfully.")


if __name__ == "__main__":
    main()
