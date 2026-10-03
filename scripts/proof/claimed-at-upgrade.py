#!/usr/bin/env python3
"""Check the claimed_at upgrade contract against two real extension builds.

No optional dependencies and no skips: missing/unloadable binaries fail the proof.
The legacy build must be c18c263 (the last main before claimed_at).
"""
import argparse
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest


class ClaimedAtUpgrade(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="honker-claimed-upgrade-")
        self.addCleanup(tmp.cleanup)
        self.path = str(Path(tmp.name) / "jobs.db")
        self.clock = [1000]

    def connect(self, extension):
        c = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(c.close)
        c.enable_load_extension(True)
        c.load_extension(str(extension))
        # A controlled clock avoids tests depending on crossing a wall-clock
        # second. Both real extensions still execute their actual queue SQL.
        c.create_function("unixepoch", 0, lambda: self.clock[0])
        self.one(c, "SELECT honker_bootstrap()")
        return c

    @staticmethod
    def one(c, sql, args=()):
        return c.execute(sql, args).fetchone()[0]

    def enqueue(self, c):
        return self.one(c, "SELECT honker_enqueue('q','{\"v\":1}',NULL,NULL,0,5,NULL)")

    def claim(self, c, worker):
        return json.loads(self.one(c, "SELECT honker_claim_batch('q',?,1,5)", (worker,)))[0]

    def snapshot(self, c, job):
        return json.loads(self.one(c, "SELECT honker_get_job(?)", (job,)))

    def legacy(self):
        old = self.connect(LEGACY)
        self.assertNotIn("claimed_at", [r[1] for r in old.execute("PRAGMA table_info(_honker_live)")],
                         "the control must really predate the column")
        return old

    def test_stop_upgrade_resume_preserves_jobs_and_uses_new_attempt_times(self):
        old = self.legacy()
        job = self.enqueue(old)
        self.claim(old, "legacy")
        before = self.snapshot(old, job)
        old.close()  # All legacy processes stop before the new code opens the DB.
        new = self.connect(CURRENT)
        migrated = self.snapshot(new, job)
        self.assertIsNone(migrated.pop("claimed_at"))
        self.assertEqual(migrated, before, "migration must not change the existing claim")
        self.clock[0] = 1006  # Existing lease expired; this is a new attempt.
        claim = self.claim(new, "new")
        self.assertEqual(claim["claimed_at"], 1006)
        self.assertEqual(claim["attempts"], 2)
        self.clock[0] = 1007
        self.assertEqual(self.one(new, "SELECT honker_heartbeat(?,'new',10)", (job,)), 1)
        self.assertEqual(self.snapshot(new, job)["claimed_at"], 1006)
        self.assertEqual(self.one(new, "SELECT honker_retry(?,'new',0,'retry')", (job,)), 1)
        self.assertIsNone(self.snapshot(new, job)["claimed_at"])
        self.clock[0] = 1008
        self.assertEqual(self.claim(new, "new")["claimed_at"], 1008)
        self.assertEqual(self.one(new, "SELECT honker_ack(?,'new')", (job,)), 1)
        self.assertEqual(self.one(new, "SELECT count(*) FROM _honker_live"), 0)

    def test_mixed_workers_are_inaccurate_and_recovery_only_clears_timestamps(self):
        old = self.legacy()
        job = self.enqueue(old)
        new = self.connect(CURRENT)
        self.assertEqual(self.claim(new, "new")["claimed_at"], 1000)
        self.clock[0] = 1001
        self.assertEqual(self.one(old, "SELECT honker_retry(?,'new',0,'retry')", (job,)), 1)
        pending = self.snapshot(new, job)
        self.assertEqual(pending["state"], "pending")
        self.assertEqual(pending["claimed_at"], 1000, "old retry leaves a stale timestamp")
        self.clock[0] = 1002
        self.claim(old, "old-again")
        stale = self.snapshot(new, job)
        self.assertEqual(stale["state"], "processing")
        self.assertGreaterEqual(stale["claim_expires_at"], self.clock[0])
        self.assertLess(stale["claimed_at"], self.clock[0], "valid lease does not make the timestamp correct")
        old.close()  # Maintenance starts only after old/new workers have stopped.
        new.execute("BEGIN IMMEDIATE")
        new.execute("UPDATE _honker_live SET claimed_at = NULL")
        new.execute("COMMIT")
        recovered = self.snapshot(new, job)
        self.assertIsNone(recovered.pop("claimed_at"))
        stale.pop("claimed_at")
        self.assertEqual(recovered, stale, "do not alter ownership, attempts, payload, state or lease")
        self.assertEqual(self.one(new, "SELECT count(*) FROM _honker_dead"), 0)
        self.clock[0] = 1008
        fresh = self.claim(new, "upgraded")
        self.assertEqual(fresh["claimed_at"], 1008)
        self.assertEqual(fresh["attempts"], 3)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy-extension", type=Path, required=True)
    parser.add_argument("--extension", type=Path, required=True)
    args = parser.parse_args()
    LEGACY, CURRENT = args.legacy_extension.resolve(strict=True), args.extension.resolve(strict=True)
    if LEGACY == CURRENT:
        parser.error("provide distinct legacy and current extension builds")
    unittest.main(argv=[__file__], verbosity=2)
