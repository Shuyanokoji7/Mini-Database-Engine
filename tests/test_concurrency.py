"""Concurrency tests for minidb's RCU storage.

These tests exercise the Read-Copy-Update locking and dirty-flag
machinery in ``storage.py`` directly. They confirm that:

  * Readers see consistent snapshots even while writers are mutating.
  * A writer never deletes a snapshot that's still being read.
  * ``flush()`` is race-free with concurrent inserts (the dirty flag
    is only cleared while the write lock is held).
  * Many concurrent readers + writers don't deadlock or crash.

Run with:  python tests/test_concurrency.py
"""

from __future__ import annotations

import os
import random
import shutil
import sys
import tempfile
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from minidb.exceptions import OperationalError
from minidb.storage import Table


def _fresh_dir(name: str) -> str:
    path = os.path.join(tempfile.gettempdir(), name)
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)
    return path


class DirtyFlagTest(unittest.TestCase):
    """The single-threaded rules for the dirty flag."""

    def setUp(self):
        self.db = _fresh_dir("test_dirty_db")
        self.table = Table(self.db, "t")

    def tearDown(self):
        try:
            self.table.remove_file()
        except Exception:
            pass
        shutil.rmtree(self.db, ignore_errors=True)

    def test_new_table_is_dirty(self):
        # A table whose data file doesn't exist yet should flush the
        # first time we ask, even if no rows were inserted.
        self.assertTrue(self.table.dirty)

    def test_flush_resets_dirty(self):
        self.table.flush()
        self.assertFalse(self.table.dirty)
        self.assertTrue(os.path.exists(self.table.path))

    def test_insert_sets_dirty(self):
        self.table.flush()
        self.assertFalse(self.table.dirty)
        self.table.insert({"id": 1, "v": "x"})
        self.assertTrue(self.table.dirty)

    def test_noop_update_does_not_set_dirty(self):
        rid = self.table.insert({"id": 1, "v": "x"})
        self.table.flush()
        self.assertFalse(self.table.dirty)
        self.table.update(rid, {"id": 1, "v": "x"})  # identical
        self.assertFalse(self.table.dirty)

    def test_real_update_sets_dirty(self):
        rid = self.table.insert({"id": 1, "v": "x"})
        self.table.flush()
        self.table.update(rid, {"id": 1, "v": "y"})
        self.assertTrue(self.table.dirty)

    def test_noop_delete_does_not_set_dirty(self):
        rid = self.table.insert({"id": 1, "v": "x"})
        self.table.delete(rid)  # tombstone it
        self.table.flush()
        self.assertFalse(self.table.dirty)
        self.table.delete(rid)  # already a tombstone
        self.assertFalse(self.table.dirty)

    def test_clean_flush_does_not_touch_disk(self):
        self.table.insert({"id": 1, "v": "x"})
        self.table.flush()
        mtime_before = os.stat(self.table.path).st_mtime_ns
        time.sleep(0.01)
        self.table.flush()  # no changes => no write
        mtime_after = os.stat(self.table.path).st_mtime_ns
        self.assertEqual(mtime_before, mtime_after)


class RCUSnapshotTest(unittest.TestCase):
    """Readers always see a consistent snapshot."""

    def setUp(self):
        self.db = _fresh_dir("test_rcu_snapshot_db")
        self.table = Table(self.db, "t")
        self.table.insert({"id": 1, "name": "Alice"})
        self.table.insert({"id": 2, "name": "Bob"})

    def tearDown(self):
        try:
            self.table.remove_file()
        except Exception:
            pass
        shutil.rmtree(self.db, ignore_errors=True)

    def test_writer_waits_for_active_reader(self):
        """A writer must NOT delete the old snapshot while a reader is on it.

        We pin a reader in the middle of its scan via a sleep. The
        writer kicks off an update; it should swap rows immediately
        but block on synchronize_rcu until the reader is done.
        """
        reader_finished_at = []
        writer_finished_at = []

        def slow_reader():
            for _, row in self.table.scan():
                time.sleep(0.5)  # 2 rows -> ~1.0s total
                self.assertIn(row["name"], {"Alice", "Bob"})
            reader_finished_at.append(time.time())

        def writer():
            time.sleep(0.05)  # let the reader start first
            self.table.update(1, {"id": 2, "name": "Charlie"})
            writer_finished_at.append(time.time())

        rt = threading.Thread(target=slow_reader)
        wt = threading.Thread(target=writer)
        rt.start()
        wt.start()
        rt.join()
        wt.join()

        # Writer should finish AFTER the reader (i.e. it waited).
        self.assertGreaterEqual(
            writer_finished_at[0] - reader_finished_at[0],
            -0.05,  # tiny scheduling slack
            "writer finished before reader released old snapshot",
        )

    def test_fast_reader_sees_new_state_during_slow_read(self):
        """A reader that arrives AFTER the writer's swap sees new data."""
        slow_seen = []
        fast_seen = []
        writer_done = threading.Event()

        def slow_reader():
            for _, row in self.table.scan():
                slow_seen.append(row["name"])
                time.sleep(0.4)

        def writer():
            time.sleep(0.05)  # let slow_reader start
            self.table.update(1, {"id": 2, "name": "Charlie"})
            writer_done.set()

        def fast_reader():
            writer_done.wait()  # arrive AFTER swap
            for _, row in self.table.scan():
                fast_seen.append(row["name"])

        ts = [
            threading.Thread(target=slow_reader),
            threading.Thread(target=writer),
            threading.Thread(target=fast_reader),
        ]
        for t in ts:
            t.start()
        for t in ts:
            t.join()

        # Slow reader should have seen "Bob" (the OLD value) at least
        # once because it had pinned the pre-swap snapshot.
        self.assertIn("Bob", slow_seen)
        # Fast reader should see only the new state.
        self.assertEqual(sorted(fast_seen), ["Alice", "Charlie"])


class ConcurrentStressTest(unittest.TestCase):
    """Many threads, no crashes, no torn snapshots."""

    def setUp(self):
        self.db = _fresh_dir("test_stress_db")
        self.table = Table(self.db, "stress")
        for i in range(10):
            self.table.insert({"id": i, "version": 0, "active": True})

    def tearDown(self):
        try:
            self.table.remove_file()
        except Exception:
            pass
        shutil.rmtree(self.db, ignore_errors=True)

    def test_many_readers_writers(self):
        running = [True]
        errors = []

        def reader_task(rid):
            iters = 0
            while running[0]:
                try:
                    for _, row in self.table.scan():
                        # Snapshot guarantees: never None, schema intact.
                        self.assertIsNotNone(row)
                        self.assertIn("id", row)
                        self.assertIn("version", row)
                    iters += 1
                except Exception as e:
                    errors.append(f"R{rid}: {e!r}")
                    return
                time.sleep(0.001)

        def writer_task(wid):
            while running[0]:
                try:
                    action = random.choice(["insert", "update", "delete"])
                    if action == "insert":
                        self.table.insert(
                            {"id": random.randint(1000, 100000),
                             "version": 0, "active": True}
                        )
                    elif action == "update":
                        rid = random.randint(0, max(0, len(self.table.rows) - 1))
                        try:
                            cur = self.table.get(rid)
                            if cur is not None:
                                new = dict(cur)
                                new["version"] = new["version"] + 1
                                self.table.update(rid, new)
                        except OperationalError:
                            pass
                    else:  # delete
                        rid = random.randint(0, max(0, len(self.table.rows) - 1))
                        try:
                            self.table.delete(rid)
                        except OperationalError:
                            pass
                except Exception as e:
                    errors.append(f"W{wid}: {e!r}")
                    return
                time.sleep(0.001)

        readers = [threading.Thread(target=reader_task, args=(i,)) for i in range(8)]
        writers = [threading.Thread(target=writer_task, args=(i,)) for i in range(4)]
        for t in readers + writers:
            t.start()

        time.sleep(2.0)  # 2s of contention
        running[0] = False
        for t in readers + writers:
            t.join(timeout=5)

        self.assertEqual(errors, [], f"errors during stress: {errors[:3]}")

    def test_concurrent_flusher_does_not_lose_writes(self):
        """The classic dirty-flag race: flush() must clear dirty under
        the write lock, not after, or a concurrent insert can be
        silently dropped."""
        running = [True]
        flush_count = [0]

        def flusher():
            while running[0]:
                if self.table.dirty:
                    self.table.flush()
                    flush_count[0] += 1
                time.sleep(0.001)

        def writer():
            for i in range(200):
                self.table.insert({"id": 100 + i, "version": 0, "active": True})
                time.sleep(0.002)

        ft = threading.Thread(target=flusher)
        wt = threading.Thread(target=writer)
        ft.start()
        wt.start()
        wt.join()
        time.sleep(0.5)  # let flusher catch the final dirty
        running[0] = False
        ft.join()

        # Final state on disk should equal final state in memory.
        with open(self.table.path) as f:
            disk_count = sum(1 for line in f if line.strip())
        mem_count = len(self.table.live_rows())
        self.assertEqual(
            mem_count,
            disk_count,
            f"flusher lost a write: memory={mem_count}, disk={disk_count}",
        )
        self.assertGreater(flush_count[0], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
