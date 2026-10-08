#!/usr/bin/env python3
"""Check the claim v2 upgrade against two real extension builds.

The legacy build is the last main before claim v2 (28587ee). It writes the
rows; the current build bootstraps the same file and must migrate them:
future `pending` rows become `scheduled`, `pending` rows with no attempts
left go to `_honker_dead`, in-flight rows stay as they are, and the old
claim indexes are gone. Then eight processes bootstrap one legacy file at
the same moment: none may fail, and the migration's effects happen once.

No optional dependencies and no skips: missing or unloadable binaries fail
the proof.
"""
import argparse
import json
import multiprocessing
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest

LEGACY = CURRENT = None
PROCS = 8


def ext_connect(path, extension, clock=None):
    c = sqlite3.connect(path, isolation_level=None, timeout=30)
    c.enable_load_extension(True)
    c.load_extension(str(extension))
    if clock is not None:
        # A controlled clock: both builds run their real queue SQL, and
        # leases lapse without sleeping.
        c.create_function("unixepoch", 0, lambda: clock[0])
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("SELECT honker_bootstrap()")
    return c


def one(c, sql, args=()):
    return c.execute(sql, args).fetchone()[0]


def indexes(c):
    return sorted(
        r[0]
        for r in c.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND tbl_name='_honker_live' AND sql IS NOT NULL"
        )
    )


NEW_INDEXES = [
    "_honker_live_expiry",
    "_honker_live_processing_deadline",
    "_honker_live_ready",
    "_honker_live_scheduled",
]


def bootstrap_child(path, extension, barrier, results):
    try:
        c = sqlite3.connect(path, isolation_level=None, timeout=30)
        c.enable_load_extension(True)
        c.load_extension(str(extension))
        barrier.wait(timeout=60)
        c.execute("SELECT honker_bootstrap()").fetchone()
        c.close()
        results.put("ok")
    except Exception as e:  # reported to the parent, which fails the proof
        results.put(f"{type(e).__name__}: {e}")


class ClaimV2Upgrade(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="honker-claim-v2-upgrade-")
        self.addCleanup(tmp.cleanup)
        self.path = str(Path(tmp.name) / "jobs.db")
        self.clock = [1000]

    def connect(self, extension, clock=True):
        c = ext_connect(self.path, extension, self.clock if clock else None)
        self.addCleanup(c.close)
        return c

    def enqueue(self, c, priority=0, delay=None, max_attempts=3):
        return one(
            c,
            "SELECT honker_enqueue('q','{}',NULL,?,?,?,NULL)",
            (delay, priority, max_attempts),
        )

    def claim(self, c, worker, n, lease):
        return json.loads(one(c, "SELECT honker_claim_batch('q',?,?,?)", (worker, n, lease)))

    def row(self, c, job):
        return c.execute(
            "SELECT state, worker_id, attempts, claim_expires_at FROM _honker_live WHERE id=?",
            (job,),
        ).fetchone()

    def dead(self, c, job):
        r = c.execute("SELECT last_error FROM _honker_dead WHERE id=?", (job,)).fetchone()
        return r and r[0]

    def legacy(self, clock=True):
        old = self.connect(LEGACY, clock)
        self.assertIn("_honker_live_claim", indexes(old), "the control must predate claim v2")
        self.assertNotIn("_honker_live_ready", indexes(old))
        return old

    def test_stop_upgrade_resume_migrates_every_row_kind(self):
        old = self.legacy()
        held = self.enqueue(old, priority=9)  # in flight, lease still valid
        spent = self.enqueue(old, priority=8, max_attempts=1)  # in flight, last attempt
        lapsed = self.enqueue(old, priority=7)  # in flight, lease will lapse
        self.assertEqual([j["id"] for j in self.claim(old, "old", 1, 300)], [held])
        self.assertEqual([j["id"] for j in self.claim(old, "old", 2, 5)], [spent, lapsed])
        # Enqueued after the legacy claims: a legacy claim dead-letters
        # exhausted due rows itself.
        due = self.enqueue(old)
        future = self.enqueue(old, delay=600)
        exhausted = self.enqueue(old, max_attempts=0)
        exhausted_future = self.enqueue(old, delay=600, max_attempts=0)
        self.assertEqual(self.row(old, future)[0], "pending", "old builds write future rows as pending")
        self.assertEqual(self.row(old, exhausted)[0], "pending")
        self.clock[0] = 1010  # `spent` and `lapsed` leases have lapsed
        old.close()  # All legacy processes stop before the new build opens the DB.

        new = self.connect(CURRENT)
        self.assertEqual(indexes(new), NEW_INDEXES)
        self.assertEqual(self.row(new, due), ("pending", None, 0, None))
        self.assertEqual(self.row(new, future)[0], "scheduled")
        self.assertEqual(self.row(new, held), ("processing", "old", 1, 1300))
        self.assertEqual(self.row(new, spent), ("processing", "old", 1, 1005))
        self.assertEqual(self.row(new, lapsed), ("processing", "old", 1, 1005))
        self.assertEqual(self.dead(new, exhausted), "max attempts exceeded")
        self.assertEqual(self.dead(new, exhausted_future), "max attempts exceeded")
        before = list(new.execute("SELECT * FROM _honker_live ORDER BY id"))
        dead_before = list(new.execute("SELECT * FROM _honker_dead ORDER BY id"))

        one(new, "SELECT honker_bootstrap()")
        self.assertEqual(list(new.execute("SELECT * FROM _honker_live ORDER BY id")), before)
        self.assertEqual(list(new.execute("SELECT * FROM _honker_dead ORDER BY id")), dead_before)

        got = self.claim(new, "new", 10, 3600)
        self.assertEqual([(j["id"], j["attempts"]) for j in got], [(lapsed, 2), (due, 1)])
        self.assertEqual(self.dead(new, spent), "max attempts exceeded")
        self.assertEqual(self.row(new, held), ("processing", "old", 1, 1300))
        self.assertEqual(json.loads(one(new, "SELECT honker_get_job(?)", (future,)))["state"], "scheduled")

        self.clock[0] = 1700  # `future` is due and `held` has lapsed
        got = self.claim(new, "new", 10, 60)
        self.assertEqual([(j["id"], j["attempts"]) for j in got], [(held, 2), (future, 1)])
        self.assertEqual(one(new, "SELECT count(*) FROM _honker_live WHERE state <> 'processing'"), 0)

    def test_eight_processes_bootstrap_a_legacy_database_at_once(self):
        self.concurrent_bootstrap(hold_lock=False)

    def test_eight_processes_queued_on_the_write_lock_migrate_once(self):
        """All eight see the old index before any of them can migrate.

        The new indexes already exist, as after a bootstrap that stopped
        before its migration, so no bootstrap statement before the
        migration needs the write lock. The parent holds the write lock
        until every child has started its bootstrap, so all eight read the
        marker and then queue on the lock. Only the re-check under the
        lock stops the other seven from migrating again (dropping the old
        index a second time is an error).
        """
        self.concurrent_bootstrap(hold_lock=True)

    def concurrent_bootstrap(self, hold_lock):
        old = self.legacy(clock=False)
        old.execute("BEGIN IMMEDIATE")
        for i in range(200):
            self.enqueue(old, delay=3600)
        for i in range(50):
            self.enqueue(old, max_attempts=0)
        for i in range(25):
            self.enqueue(old, delay=3600, max_attempts=0)
        due = [self.enqueue(old) for _ in range(10)]
        old.execute("COMMIT")
        # Record every row the migration touches.
        old.executescript(
            """
            CREATE TABLE proof_log (kind TEXT NOT NULL, id INTEGER NOT NULL);
            CREATE TRIGGER proof_scheduled AFTER UPDATE OF state ON _honker_live
              WHEN old.state = 'pending' AND new.state = 'scheduled'
              BEGIN INSERT INTO proof_log VALUES ('scheduled', new.id); END;
            CREATE TRIGGER proof_dead AFTER INSERT ON _honker_dead
              BEGIN INSERT INTO proof_log VALUES ('dead', new.id); END;
            CREATE TRIGGER proof_deleted AFTER DELETE ON _honker_live
              BEGIN INSERT INTO proof_log VALUES ('deleted', old.id); END;
            """
        )
        if hold_lock:
            fresh = sqlite3.connect(str(Path(self.path).with_name("fresh.db")))
            fresh.enable_load_extension(True)
            fresh.load_extension(str(CURRENT))
            fresh.execute("SELECT honker_bootstrap()")
            for name in NEW_INDEXES:
                sql = one(fresh, "SELECT sql FROM sqlite_master WHERE name = ?", (name,))
                old.execute(sql.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS", 1))
            fresh.close()
            old.execute("BEGIN IMMEDIATE")
        else:
            old.close()

        ctx = multiprocessing.get_context("spawn")
        barrier = ctx.Barrier(PROCS + 1)
        results = ctx.Queue()
        procs = [
            ctx.Process(target=bootstrap_child, args=(self.path, CURRENT, barrier, results))
            for _ in range(PROCS)
        ]
        for p in procs:
            p.start()
        barrier.wait(timeout=60)
        if hold_lock:
            time.sleep(1.0)  # let every child reach the lock
            self.assertTrue(results.empty(), "a bootstrap finished while the lock was held")
            old.execute("COMMIT")
            old.close()
        outcomes = [results.get(timeout=120) for _ in procs]
        for p in procs:
            p.join(timeout=60)
        self.assertEqual(outcomes, ["ok"] * PROCS)
        self.assertEqual([p.exitcode for p in procs], [0] * PROCS)

        c = self.connect(CURRENT, clock=False)
        self.assertEqual(indexes(c), NEW_INDEXES)
        log = dict(c.execute("SELECT kind, count(*) FROM proof_log GROUP BY kind"))
        distinct = dict(c.execute("SELECT kind, count(DISTINCT id) FROM proof_log GROUP BY kind"))
        # 25 future exhausted rows are scheduled and then dead-lettered
        # in the same migration.
        self.assertEqual(log, {"scheduled": 225, "dead": 75, "deleted": 75})
        self.assertEqual(distinct, log, "a row was migrated twice")
        self.assertEqual(
            dict(c.execute("SELECT state, count(*) FROM _honker_live GROUP BY state")),
            {"pending": 10, "scheduled": 200},
        )
        self.assertEqual(one(c, "SELECT count(*) FROM _honker_dead WHERE last_error = 'max attempts exceeded'"), 75)
        got = json.loads(one(c, "SELECT honker_claim_batch('q','w',50,60)"))
        self.assertEqual(sorted(j["id"] for j in got), due)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy-extension", type=Path, required=True)
    parser.add_argument("--extension", type=Path, required=True)
    args = parser.parse_args()
    LEGACY, CURRENT = args.legacy_extension.resolve(strict=True), args.extension.resolve(strict=True)
    if LEGACY == CURRENT:
        parser.error("provide distinct legacy and current extension builds")
    unittest.main(argv=[__file__], verbosity=2)
