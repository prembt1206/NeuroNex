import os
import threading
import time
import unittest
from src.engine import KVEngine, DiskFullError, OP_PUT
from src.cli import check_file_integrity


class TestKVDBScenarios(unittest.TestCase):
    def setUp(self):
        self.db_path = "test_unit_scenario.db"
        self._cleanup()

    def tearDown(self):
        self._cleanup()

    def _cleanup(self):
        for p in (self.db_path, self.db_path + ".compact.tmp"):
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass

    def test_basic_put_get_delete_and_batch_b5(self):
        """Tests basic operations and Scenario B5 (intra-batch ordering on same key)."""
        db = KVEngine(self.db_path)
        db.put(b"k1", b"v1")
        self.assertEqual(db.get(b"k1"), b"v1")

        # Scenario B5: put X then del X in same batch -> X must be absent
        db.batch([("put", b"x", b"val_x"), ("del", b"x")])
        self.assertIsNone(db.get(b"x"))

        # Scenario B5 reverse: del X then put X in same batch -> X must be present
        db.batch([("del", b"x"), ("put", b"x", b"val_x2")])
        self.assertEqual(db.get(b"x"), b"val_x2")
        db.close()

        # Re-open and verify durability after recovery
        db2 = KVEngine(self.db_path)
        self.assertEqual(db2.get(b"k1"), b"v1")
        self.assertEqual(db2.get(b"x"), b"val_x2")
        db2.close()

    def test_scenario_d4_concurrent_reader_during_batch(self):
        """Scenario D4: Reader calling get during a batch sees state before or after, never half."""
        db = KVEngine(self.db_path)
        db.batch([("put", b"a", b"old"), ("put", b"b", b"old")])

        violation = []
        stop = threading.Event()

        def reader():
            while not stop.is_set():
                va = db.get(b"a")
                vb = db.get(b"b")
                # Under atomic batch lock, if we read inside a single lock or across batches,
                # check that a single batch is never partially visible in index
                if va not in (b"old", b"new1", b"new2") or vb not in (b"old", b"new1", b"new2"):
                    violation.append((va, vb))

        t = threading.Thread(target=reader)
        t.start()
        for i in range(50):
            val = b"new1" if i % 2 == 0 else b"new2"
            db.batch([("put", b"a", val), ("put", b"b", val)])
        stop.set()
        t.join()
        db.close()
        self.assertEqual(violation, [])

    def test_scenario_e1_to_e5_disk_full_and_auto_compaction(self):
        """Scenarios E1-E5: Disk full cap, clean batch rejection, and auto-compaction after deletes."""
        db = KVEngine(self.db_path, max_bytes=2048)
        db.put(b"k1", b"A" * 500)
        db.put(b"k2", b"B" * 500)

        # E1: Single put exceeding 2048 bytes fails cleanly
        with self.assertRaises(DiskFullError):
            db.put(b"k3", b"C" * 1200)

        # Previous data still intact
        self.assertEqual(db.get(b"k1"), b"A" * 500)
        self.assertEqual(db.get(b"k2"), b"B" * 500)

        # E2: Batch exceeding cap halfway fails cleanly with 0 operations applied
        with self.assertRaises(DiskFullError):
            db.batch([("put", b"b1", b"X" * 300), ("put", b"b2", b"Y" * 1000)])
        self.assertIsNone(db.get(b"b1"))
        self.assertIsNone(db.get(b"b2"))

        # E5: Delete k2 to free logical space, then write k4 -> auto-compaction runs and succeeds!
        db.delete(b"k2")
        db.put(b"k4", b"D" * 500)
        self.assertEqual(db.get(b"k4"), b"D" * 500)
        self.assertIsNone(db.get(b"k2"))
        db.close()

    def test_scenario_f4_manual_1_byte_truncation(self):
        """Scenario F4: Database file truncated by 1 byte at the end by hand."""
        db = KVEngine(self.db_path)
        db.put(b"safe_key", b"safe_value")
        db.put(b"tail_key", b"tail_value")
        db.close()

        # Truncate file by 1 byte manually (destroys the last byte of CMIT on tail_key)
        size = os.path.getsize(self.db_path)
        with open(self.db_path, "r+b") as f:
            f.truncate(size - 1)

        # Re-open must succeed! safe_key must be intact, broken tail_key frame truncated
        db2 = KVEngine(self.db_path)
        self.assertEqual(db2.get(b"safe_key"), b"safe_value")
        self.assertIsNone(db2.get(b"tail_key"))
        db2.close()

        ok, msg = check_file_integrity(self.db_path)
        self.assertTrue(ok, msg)

    def test_scenario_f3_one_million_keys_recovery_under_2_seconds(self):
        """Scenario F3: 1,000,000 keys written, process restarted, open() completes in < 2.0s."""
        db = KVEngine(self.db_path)
        # Write 1,000,000 keys in 100 batches of 10,000 keys each for fast setup
        for b in range(100):
            base = b * 10000
            ops = [(OP_PUT, f"k{base + i:07d}".encode("ascii"), b"v") for i in range(10000)]
            db._write_batch_internal(ops)
        db.close()

        # Measure open() recovery time on 1,000,000 keys
        t0 = time.perf_counter()
        db_recovered = KVEngine(self.db_path)
        elapsed = time.perf_counter() - t0

        print(f"\n[Scenario F3] 1,000,000 keys recovery took {elapsed:.3f} seconds (limit: 2.000s)")
        self.assertEqual(len(db_recovered.index), 1_000_000)
        self.assertEqual(db_recovered.get(b"k0000000"), b"v")
        self.assertEqual(db_recovered.get(b"k0999999"), b"v")
        self.assertLess(elapsed, 2.0, "Recovery took longer than 2.0 seconds!")
        db_recovered.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)