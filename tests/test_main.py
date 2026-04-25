"""Main test suite for minidb.

Covers the day-to-day functionality:
  * CRUD: CREATE / DROP / INSERT / SELECT / UPDATE / DELETE
  * SELECT extras: WHERE, ORDER BY, LIMIT, DISTINCT, COUNT(*), LIKE
  * Constraints: PRIMARY KEY, NOT NULL
  * Transactions: commit, rollback, read-your-own-writes
  * PEP 249 surface: cursor methods, executemany, iteration
  * Crash recovery via the WAL
  * Persistence across connections

Run with:  python tests/test_main.py
"""

from __future__ import annotations

import os
import shutil
import sys
import unittest

# Make the package importable without installation.
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import minidb
from minidb import (
    DataError,
    IntegrityError,
    InterfaceError,
    NotSupportedError,
    ProgrammingError,
)


def _fresh(path: str):
    if os.path.exists(path):
        shutil.rmtree(path)
    return path


class CRUDTest(unittest.TestCase):
    """The basic SQL operations."""

    def setUp(self):
        self.db = _fresh("test_crud_db")
        self.conn = minidb.connect(self.db)
        self.cur = self.conn.cursor()
        self.cur.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"
        )

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(self.db, ignore_errors=True)

    def test_insert_and_select(self):
        self.cur.execute("INSERT INTO users VALUES (?, ?, ?)", (1, "Ada", 36))
        self.assertEqual(self.cur.rowcount, 1)
        self.cur.execute("SELECT * FROM users")
        self.assertEqual(self.cur.fetchall(), [(1, "Ada", 36)])

    def test_insert_named_columns(self):
        self.cur.execute("INSERT INTO users (id, name) VALUES (?, ?)", (1, "Ada"))
        self.cur.execute("SELECT id, name, age FROM users")
        self.assertEqual(self.cur.fetchone(), (1, "Ada", None))

    def test_insert_duplicate_pk(self):
        self.cur.execute("INSERT INTO users VALUES (?, ?, ?)", (1, "Ada", 36))
        with self.assertRaises(IntegrityError):
            self.cur.execute("INSERT INTO users VALUES (?, ?, ?)", (1, "Bob", 0))

    def test_insert_null_pk_rejected(self):
        with self.assertRaises(IntegrityError):
            self.cur.execute("INSERT INTO users VALUES (?, ?, ?)", (None, "x", 1))

    def test_select_pk_uses_index(self):
        for i in range(5):
            self.cur.execute("INSERT INTO users VALUES (?, ?, ?)", (i, f"u{i}", i))
        self.cur.execute("SELECT name FROM users WHERE id = ?", (3,))
        self.assertEqual(self.cur.fetchall(), [("u3",)])

    def test_update(self):
        self.cur.execute("INSERT INTO users VALUES (?, ?, ?)", (1, "Ada", 36))
        self.cur.execute("UPDATE users SET age = ? WHERE id = ?", (37, 1))
        self.assertEqual(self.cur.rowcount, 1)
        self.cur.execute("SELECT age FROM users WHERE id = ?", (1,))
        self.assertEqual(self.cur.fetchone(), (37,))

    def test_update_change_pk(self):
        self.cur.execute("INSERT INTO users VALUES (?, ?, ?)", (1, "Ada", 36))
        self.cur.execute("UPDATE users SET id = ? WHERE id = ?", (10, 1))
        self.cur.execute("SELECT id FROM users")
        self.assertEqual(self.cur.fetchall(), [(10,)])
        # Index is updated: lookup by new PK works
        self.cur.execute("SELECT name FROM users WHERE id = ?", (10,))
        self.assertEqual(self.cur.fetchone(), ("Ada",))

    def test_update_pk_collision_rejected(self):
        self.cur.execute("INSERT INTO users VALUES (?, ?, ?)", (1, "Ada", 36))
        self.cur.execute("INSERT INTO users VALUES (?, ?, ?)", (2, "Bob", 41))
        with self.assertRaises(IntegrityError):
            self.cur.execute("UPDATE users SET id = ? WHERE id = ?", (2, 1))

    def test_delete(self):
        self.cur.execute("INSERT INTO users VALUES (?, ?, ?)", (1, "Ada", 36))
        self.cur.execute("INSERT INTO users VALUES (?, ?, ?)", (2, "Bob", 41))
        self.cur.execute("DELETE FROM users WHERE age < ?", (40,))
        self.assertEqual(self.cur.rowcount, 1)
        self.cur.execute("SELECT id FROM users")
        self.assertEqual(self.cur.fetchall(), [(2,)])

    def test_drop_table(self):
        self.cur.execute("DROP TABLE users")
        with self.assertRaises(ProgrammingError):
            self.cur.execute("SELECT * FROM users")

    def test_drop_table_if_exists(self):
        self.cur.execute("DROP TABLE IF EXISTS missing")  # no error
        self.cur.execute("DROP TABLE IF EXISTS users")
        with self.assertRaises(ProgrammingError):
            self.cur.execute("SELECT * FROM users")

    def test_create_table_if_not_exists(self):
        # Already exists - should not raise.
        self.cur.execute(
            "CREATE TABLE IF NOT EXISTS users "
            "(id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"
        )


class SelectExtrasTest(unittest.TestCase):
    """ORDER BY, LIMIT, DISTINCT, COUNT(*), LIKE."""

    def setUp(self):
        self.db = _fresh("test_extras_db")
        self.conn = minidb.connect(self.db)
        self.cur = self.conn.cursor()
        self.cur.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"
        )
        self.cur.executemany(
            "INSERT INTO t VALUES (?, ?, ?)",
            [
                (1, "Ada", 36),
                (2, "Alan", 41),
                (3, "Grace", 85),
                (4, "Linus", 55),
                (5, "Edsger", 72),
                (6, "Ada", 40),  # duplicate name on purpose
            ],
        )
        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(self.db, ignore_errors=True)

    def test_order_by_asc(self):
        self.cur.execute("SELECT name, age FROM t ORDER BY age")
        self.assertEqual(
            self.cur.fetchall(),
            [("Ada", 36), ("Ada", 40), ("Alan", 41), ("Linus", 55),
             ("Edsger", 72), ("Grace", 85)],
        )

    def test_order_by_desc(self):
        self.cur.execute("SELECT age FROM t ORDER BY age DESC")
        self.assertEqual([r[0] for r in self.cur.fetchall()],
                         [85, 72, 55, 41, 40, 36])

    def test_order_by_multi(self):
        self.cur.execute("SELECT name, age FROM t ORDER BY name ASC, age DESC")
        rows = self.cur.fetchall()
        # Both Adas first, the older one before the younger because of DESC age
        self.assertEqual(rows[0], ("Ada", 40))
        self.assertEqual(rows[1], ("Ada", 36))

    def test_limit(self):
        self.cur.execute("SELECT id FROM t ORDER BY id LIMIT 3")
        self.assertEqual([r[0] for r in self.cur.fetchall()], [1, 2, 3])

    def test_limit_zero(self):
        self.cur.execute("SELECT id FROM t LIMIT 0")
        self.assertEqual(self.cur.fetchall(), [])

    def test_distinct(self):
        self.cur.execute("SELECT DISTINCT name FROM t ORDER BY name")
        # Ada appears twice in the data, only once in output.
        names = [r[0] for r in self.cur.fetchall()]
        self.assertEqual(names, ["Ada", "Alan", "Edsger", "Grace", "Linus"])

    def test_count_all(self):
        self.cur.execute("SELECT COUNT(*) FROM t")
        self.assertEqual(self.cur.fetchone(), (6,))

    def test_count_with_where(self):
        self.cur.execute("SELECT COUNT(*) FROM t WHERE age > ?", (50,))
        self.assertEqual(self.cur.fetchone(), (3,))

    def test_like_percent(self):
        self.cur.execute("SELECT name FROM t WHERE name LIKE ?", ("A%",))
        names = sorted(r[0] for r in self.cur.fetchall())
        self.assertEqual(names, ["Ada", "Ada", "Alan"])

    def test_like_underscore(self):
        # 3-letter names ending in 'a'
        self.cur.execute("SELECT name FROM t WHERE name LIKE ?", ("_da",))
        names = [r[0] for r in self.cur.fetchall()]
        self.assertEqual(sorted(names), ["Ada", "Ada"])


class ConstraintsTest(unittest.TestCase):
    """PRIMARY KEY and NOT NULL behaviour."""

    def setUp(self):
        self.db = _fresh("test_constraints_db")
        self.conn = minidb.connect(self.db)
        self.cur = self.conn.cursor()

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(self.db, ignore_errors=True)

    def test_not_null_on_insert(self):
        self.cur.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
        )
        with self.assertRaises(IntegrityError):
            self.cur.execute("INSERT INTO t (id) VALUES (?)", (1,))

    def test_not_null_on_update(self):
        self.cur.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
        )
        self.cur.execute("INSERT INTO t VALUES (?, ?)", (1, "Ada"))
        with self.assertRaises(IntegrityError):
            self.cur.execute("UPDATE t SET name = NULL WHERE id = ?", (1,))

    def test_pk_primary_key_alias(self):
        # PK column gets indexed automatically.
        self.cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x TEXT)")
        self.cur.executemany(
            "INSERT INTO t VALUES (?, ?)",
            [(i, f"v{i}") for i in range(10)],
        )
        self.cur.execute("SELECT x FROM t WHERE id = ?", (7,))
        self.assertEqual(self.cur.fetchone(), ("v7",))

    def test_type_coercion(self):
        self.cur.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, score REAL, label TEXT)"
        )
        # Strings that look numeric are coerced.
        self.cur.execute("INSERT INTO t VALUES (?, ?, ?)", ("5", "3.14", "ok"))
        self.cur.execute("SELECT id, score, label FROM t")
        self.assertEqual(self.cur.fetchone(), (5, 3.14, "ok"))

    def test_type_rejects_bool_for_int(self):
        self.cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        with self.assertRaises(DataError):
            self.cur.execute("INSERT INTO t VALUES (?)", (True,))


class TransactionTest(unittest.TestCase):
    """commit / rollback / read-your-own-writes / WAL durability."""

    def setUp(self):
        self.db = _fresh("test_txn_db")
        self.conn = minidb.connect(self.db)
        self.cur = self.conn.cursor()
        self.cur.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, n INTEGER)"
        )

    def tearDown(self):
        try:
            self.conn.close()
        except Exception:
            pass
        shutil.rmtree(self.db, ignore_errors=True)

    def _names(self):
        self.cur.execute("SELECT name FROM t ORDER BY id")
        return [r[0] for r in self.cur.fetchall()]

    def test_commit_persists(self):
        self.cur.execute("INSERT INTO t VALUES (?, ?, ?)", (1, "Ada", 1))
        self.conn.commit()
        # New connection sees the change.
        self.conn.close()
        with minidb.connect(self.db) as c2:
            cur2 = c2.cursor()
            cur2.execute("SELECT name FROM t WHERE id = ?", (1,))
            self.assertEqual(cur2.fetchone(), ("Ada",))

    def test_rollback_undoes_insert(self):
        self.cur.execute("INSERT INTO t VALUES (?, ?, ?)", (1, "Ada", 1))
        self.conn.commit()
        self.cur.execute("INSERT INTO t VALUES (?, ?, ?)", (2, "Bob", 2))
        # Visible before rollback (read-your-own-writes!)
        self.assertEqual(self._names(), ["Ada", "Bob"])
        self.conn.rollback()
        self.assertEqual(self._names(), ["Ada"])

    def test_rollback_undoes_update(self):
        self.cur.execute("INSERT INTO t VALUES (?, ?, ?)", (1, "Ada", 1))
        self.conn.commit()
        self.cur.execute("UPDATE t SET name = ? WHERE id = ?", ("Bob", 1))
        # Read-your-own-writes
        self.assertEqual(self._names(), ["Bob"])
        self.conn.rollback()
        self.assertEqual(self._names(), ["Ada"])

    def test_rollback_undoes_delete(self):
        self.cur.execute("INSERT INTO t VALUES (?, ?, ?)", (1, "Ada", 1))
        self.conn.commit()
        self.cur.execute("DELETE FROM t WHERE id = ?", (1,))
        self.assertEqual(self._names(), [])
        self.conn.rollback()
        self.assertEqual(self._names(), ["Ada"])

    def test_rollback_undoes_pk_change(self):
        self.cur.execute("INSERT INTO t VALUES (?, ?, ?)", (1, "Ada", 1))
        self.conn.commit()
        self.cur.execute("UPDATE t SET id = ? WHERE id = ?", (99, 1))
        # Index updated to find new PK
        self.cur.execute("SELECT name FROM t WHERE id = ?", (99,))
        self.assertEqual(self.cur.fetchone(), ("Ada",))
        self.conn.rollback()
        # After rollback, original PK lookup works again
        self.cur.execute("SELECT name FROM t WHERE id = ?", (1,))
        self.assertEqual(self.cur.fetchone(), ("Ada",))
        self.cur.execute("SELECT name FROM t WHERE id = ?", (99,))
        self.assertIsNone(self.cur.fetchone())

    def test_rollback_multi_op(self):
        # Insert-then-update-then-delete the same row, then rollback.
        self.cur.execute("INSERT INTO t VALUES (?, ?, ?)", (1, "A", 1))
        self.conn.commit()
        self.cur.execute("UPDATE t SET n = ? WHERE id = ?", (10, 1))
        self.cur.execute("UPDATE t SET n = ? WHERE id = ?", (20, 1))
        self.cur.execute("INSERT INTO t VALUES (?, ?, ?)", (2, "B", 2))
        self.conn.rollback()
        self.cur.execute("SELECT id, name, n FROM t ORDER BY id")
        self.assertEqual(self.cur.fetchall(), [(1, "A", 1)])

    def test_close_does_not_autocommit(self):
        # Insert + commit, then insert again without commit, then close.
        self.cur.execute("INSERT INTO t VALUES (?, ?, ?)", (1, "Ada", 1))
        self.conn.commit()
        self.cur.execute("INSERT INTO t VALUES (?, ?, ?)", (2, "Lost", 2))
        self.conn.close()
        with minidb.connect(self.db) as c2:
            cur2 = c2.cursor()
            cur2.execute("SELECT name FROM t ORDER BY id")
            self.assertEqual([r[0] for r in cur2.fetchall()], ["Ada"])

    def test_context_manager_commit_on_success(self):
        with minidb.connect(self.db) as c2:
            cur2 = c2.cursor()
            cur2.execute("INSERT INTO t VALUES (?, ?, ?)", (1, "Ctx", 1))
            # exits cleanly -> commit
        with minidb.connect(self.db) as c3:
            cur3 = c3.cursor()
            cur3.execute("SELECT name FROM t")
            self.assertEqual(cur3.fetchall(), [("Ctx",)])

    def test_context_manager_rollback_on_exception(self):
        try:
            with minidb.connect(self.db) as c2:
                cur2 = c2.cursor()
                cur2.execute("INSERT INTO t VALUES (?, ?, ?)", (1, "Lost", 1))
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        with minidb.connect(self.db) as c3:
            cur3 = c3.cursor()
            cur3.execute("SELECT name FROM t")
            self.assertEqual(cur3.fetchall(), [])

    def test_explicit_commit_rollback_via_sql(self):
        self.cur.execute("BEGIN")
        self.cur.execute("INSERT INTO t VALUES (?, ?, ?)", (1, "X", 1))
        self.cur.execute("ROLLBACK")
        self.cur.execute("SELECT * FROM t")
        self.assertEqual(self.cur.fetchall(), [])

        self.cur.execute("INSERT INTO t VALUES (?, ?, ?)", (2, "Y", 2))
        self.cur.execute("COMMIT")
        self.cur.execute("SELECT name FROM t")
        self.assertEqual(self.cur.fetchall(), [("Y",)])


class CrashRecoveryTest(unittest.TestCase):
    """Simulate crashes mid-transaction and verify recovery."""

    def setUp(self):
        self.db = _fresh("test_recover_db")

    def tearDown(self):
        shutil.rmtree(self.db, ignore_errors=True)

    def _setup_table(self):
        with minidb.connect(self.db) as c:
            cur = c.cursor()
            cur.execute(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"
            )
            cur.execute("INSERT INTO t VALUES (?, ?)", (1, "Ada"))

    def test_recovery_replays_committed_wal(self):
        """Simulate: commit (WAL durable) but data files NOT flushed.

        We do this by manually appending to the WAL after a connection
        is closed, mimicking what would be on disk if the process died
        between WAL fsync and data flush.
        """
        self._setup_table()
        # Manually craft a WAL entry that wasn't applied to data files.
        wal_path = os.path.join(self.db, "wal.log")
        with open(wal_path, "a") as f:
            import json
            f.write(json.dumps({
                "txid": 99,
                "op": "INSERT",
                "table": "t",
                "row": {"id": 5, "name": "Recovered"},
            }) + "\n")
            f.write(json.dumps({"txid": 99, "op": "COMMIT"}) + "\n")

        # Reopen — recovery should pick up the row.
        with minidb.connect(self.db) as c:
            cur = c.cursor()
            cur.execute("SELECT name FROM t WHERE id = ?", (5,))
            self.assertEqual(cur.fetchone(), ("Recovered",))

        # And now the WAL should be empty (truncated post-recovery).
        with open(wal_path) as f:
            self.assertEqual(f.read().strip(), "")

    def test_recovery_discards_uncommitted_wal(self):
        """An incomplete txn (no COMMIT marker) is discarded on restart."""
        self._setup_table()
        wal_path = os.path.join(self.db, "wal.log")
        with open(wal_path, "a") as f:
            import json
            f.write(json.dumps({
                "txid": 99,
                "op": "INSERT",
                "table": "t",
                "row": {"id": 99, "name": "Lost"},
            }) + "\n")
            # No COMMIT marker — this txn never finished.

        with minidb.connect(self.db) as c:
            cur = c.cursor()
            cur.execute("SELECT name FROM t WHERE id = ?", (99,))
            self.assertIsNone(cur.fetchone())

    def test_recovery_is_idempotent(self):
        """If recovery runs twice, no row should appear twice."""
        self._setup_table()
        wal_path = os.path.join(self.db, "wal.log")
        import json
        for _ in range(2):
            with open(wal_path, "a") as f:
                f.write(json.dumps({
                    "txid": 50,
                    "op": "INSERT",
                    "table": "t",
                    "row": {"id": 7, "name": "Dup"},
                }) + "\n")
                f.write(json.dumps({"txid": 50, "op": "COMMIT"}) + "\n")
            with minidb.connect(self.db):
                pass  # triggers recovery + truncate

        with minidb.connect(self.db) as c:
            cur = c.cursor()
            cur.execute("SELECT COUNT(*) FROM t WHERE id = ?", (7,))
            self.assertEqual(cur.fetchone(), (1,))


class PEP249Test(unittest.TestCase):
    """Coverage of the DB-API 2.0 surface."""

    def setUp(self):
        self.db = _fresh("test_pep249_db")
        self.conn = minidb.connect(self.db)
        self.cur = self.conn.cursor()

    def tearDown(self):
        try:
            self.conn.close()
        except Exception:
            pass
        shutil.rmtree(self.db, ignore_errors=True)

    def test_module_constants(self):
        self.assertEqual(minidb.apilevel, "2.0")
        self.assertEqual(minidb.paramstyle, "qmark")
        self.assertIn(minidb.threadsafety, (0, 1, 2, 3))

    def test_description(self):
        self.cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        self.cur.execute("INSERT INTO t VALUES (?, ?)", (1, "x"))
        self.cur.execute("SELECT id, name FROM t")
        self.assertEqual(len(self.cur.description), 2)
        self.assertEqual(self.cur.description[0][0], "id")
        self.assertEqual(self.cur.description[0][1], int)
        self.assertEqual(self.cur.description[1][0], "name")
        self.assertEqual(self.cur.description[1][1], str)

    def test_fetchone_fetchmany_fetchall(self):
        self.cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        self.cur.executemany(
            "INSERT INTO t VALUES (?)", [(i,) for i in range(5)]
        )
        self.cur.execute("SELECT id FROM t ORDER BY id")
        self.assertEqual(self.cur.fetchone(), (0,))
        self.assertEqual(self.cur.fetchmany(2), [(1,), (2,)])
        self.assertEqual(self.cur.fetchall(), [(3,), (4,)])
        self.assertIsNone(self.cur.fetchone())

    def test_arraysize_defaults_fetchmany(self):
        self.cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        self.cur.executemany(
            "INSERT INTO t VALUES (?)", [(i,) for i in range(3)]
        )
        self.cur.arraysize = 2
        self.cur.execute("SELECT id FROM t ORDER BY id")
        self.assertEqual(self.cur.fetchmany(), [(0,), (1,)])

    def test_executemany_only_for_dml(self):
        self.cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        with self.assertRaises(NotSupportedError):
            self.cur.executemany("SELECT * FROM t", [(), ()])

    def test_cursor_iteration(self):
        self.cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        self.cur.executemany(
            "INSERT INTO t VALUES (?)", [(i,) for i in range(3)]
        )
        self.cur.execute("SELECT id FROM t ORDER BY id")
        self.assertEqual([row[0] for row in self.cur], [0, 1, 2])

    def test_closed_connection_errors(self):
        self.conn.close()
        with self.assertRaises(InterfaceError):
            self.conn.cursor()

    def test_closed_cursor_errors(self):
        self.cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        self.cur.close()
        with self.assertRaises(InterfaceError):
            self.cur.execute("SELECT * FROM t")

    def test_unknown_column_errors(self):
        self.cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        with self.assertRaises(ProgrammingError):
            self.cur.execute("SELECT nope FROM t")


class PersistenceTest(unittest.TestCase):
    """Reopening the database returns the committed state."""

    def setUp(self):
        self.db = _fresh("test_persist_db")

    def tearDown(self):
        shutil.rmtree(self.db, ignore_errors=True)

    def test_reopen_sees_committed_data(self):
        with minidb.connect(self.db) as c:
            cur = c.cursor()
            cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            cur.executemany(
                "INSERT INTO t VALUES (?, ?)",
                [(1, "A"), (2, "B"), (3, "C")],
            )
        with minidb.connect(self.db) as c:
            cur = c.cursor()
            cur.execute("SELECT id, name FROM t ORDER BY id")
            self.assertEqual(
                cur.fetchall(),
                [(1, "A"), (2, "B"), (3, "C")],
            )

    def test_reopen_index_works(self):
        with minidb.connect(self.db) as c:
            cur = c.cursor()
            cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            cur.executemany(
                "INSERT INTO t VALUES (?, ?)",
                [(i, f"n{i}") for i in range(50)],
            )
        with minidb.connect(self.db) as c:
            cur = c.cursor()
            # PK index should be rebuilt on open
            cur.execute("SELECT name FROM t WHERE id = ?", (37,))
            self.assertEqual(cur.fetchone(), ("n37",))


if __name__ == "__main__":
    unittest.main(verbosity=2)
