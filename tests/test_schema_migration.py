"""Schema migration tests.

Users upgrading from a pre-refactor `honker` release have a `.db`
file with old schemas on disk. Opening it with current code must
not crash, silently corrupt data, or leave the user stuck. These
tests build legacy schemas directly via sqlite3, then open the
file with honker and assert the upgrade path.

What we're defending against:
  * old indexes / tables lingering and confusing the query planner,
  * a fresh `enqueue → claim → ack` failing on an upgraded DB,
  * a new-schema column reference hitting an old-schema table.
"""

import sqlite3

import honker


def test_legacy_pending_processing_tables_dropped_on_open(db_path):
    """Pre-v0.1 layout had separate `_honker_pending` and
    `_honker_processing` tables plus their claim/reclaim indexes.
    Current code consolidates into `_honker_live` and
    `Queue._init_schema` DROPs the old objects. Verify the drop
    runs cleanly on an existing legacy DB and that the full
    enqueue/claim/ack path works afterwards.
    """
    # Build a legacy DB by hand.
    raw = sqlite3.connect(db_path)
    raw.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE _honker_pending (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          queue TEXT NOT NULL,
          payload TEXT NOT NULL,
          priority INTEGER NOT NULL DEFAULT 0,
          run_at INTEGER NOT NULL DEFAULT (unixepoch()),
          max_attempts INTEGER NOT NULL DEFAULT 3,
          attempts INTEGER NOT NULL DEFAULT 0,
          created_at INTEGER NOT NULL DEFAULT (unixepoch())
        );
        CREATE INDEX _honker_pending_claim
          ON _honker_pending(queue, priority DESC, run_at, id);
        CREATE TABLE _honker_processing (
          id INTEGER PRIMARY KEY,
          queue TEXT NOT NULL,
          payload TEXT NOT NULL,
          worker_id TEXT NOT NULL,
          claim_expires_at INTEGER NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0,
          max_attempts INTEGER NOT NULL DEFAULT 3,
          priority INTEGER NOT NULL DEFAULT 0,
          run_at INTEGER NOT NULL DEFAULT 0,
          created_at INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX _honker_processing_reclaim
          ON _honker_processing(claim_expires_at);
        -- Seed one row into the old pending table; the migration
        -- drops the table, so its contents are lost — users running
        -- the migration are expected to drain the queue first. This
        -- test just verifies the drop happens, not that rows move.
        INSERT INTO _honker_pending (queue, payload)
          VALUES ('old-queue', '{"stale": true}');
        """
    )
    raw.commit()
    raw.close()

    # Open with current honker — triggers Queue._init_schema.
    db = honker.open(db_path)
    db.queue("new-queue")

    # Legacy objects dropped.
    check = sqlite3.connect(db_path)
    leftover = check.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type IN ('table', 'index') "
        "  AND name IN ('_honker_pending', '_honker_processing', "
        "              '_honker_pending_claim', "
        "              '_honker_processing_reclaim')"
    ).fetchall()
    assert leftover == [], f"legacy objects still present: {leftover}"

    # Current schema present.
    live_cols = [
        r[1]
        for r in check.execute("PRAGMA table_info(_honker_live)").fetchall()
    ]
    assert "queue" in live_cols
    assert "state" in live_cols
    assert "expires_at" in live_cols
    check.close()

    # Full round-trip on the upgraded DB.
    q = db.queue("new-queue")
    q.enqueue({"ok": True})
    job = q.claim_one("w1")
    assert job is not None
    assert job.payload == {"ok": True}
    assert job.ack() is True


def test_legacy_scheduler_state_table_leftover_is_harmless(db_path):
    """Commit 5/6 replaced `_honker_scheduler_state(name, last_fire_at)`
    with `_honker_scheduler_tasks(name, queue, cron_expr, payload,
    priority, expires_s, next_fire_at)`. The old table isn't
    automatically dropped — it's harmless dead weight. Verify that
    its presence doesn't break the new scheduler path.
    """
    raw = sqlite3.connect(db_path)
    raw.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE _honker_scheduler_state (
          name TEXT PRIMARY KEY,
          last_fire_at INTEGER NOT NULL
        );
        INSERT INTO _honker_scheduler_state VALUES ('nightly', 1700000000);
        """
    )
    raw.commit()
    raw.close()

    # Opening + registering a scheduler task must succeed.
    db = honker.open(db_path)
    from honker import Scheduler, crontab

    sched = Scheduler(db)
    sched.add(name="nightly", queue="backups", schedule=crontab("0 3 * * *"))

    rows = db.query(
        "SELECT queue FROM _honker_scheduler_tasks WHERE name='nightly'"
    )
    assert rows[0]["queue"] == "backups"

    # Old table still there but untouched — that's fine.
    check = sqlite3.connect(db_path)
    old = check.execute(
        "SELECT COUNT(*) FROM _honker_scheduler_state"
    ).fetchone()
    assert old[0] == 1  # our seeded row is preserved
    check.close()


def test_fresh_db_has_all_current_tables(db_path):
    """Sanity: a fresh (non-legacy) DB boots with every table the
    current schema expects. Catches regressions where a table is
    added to BOOTSTRAP_JOBLITE_SQL but not to the bootstrap call
    path."""
    db = honker.open(db_path)
    db.queue("q")  # triggers bootstrap if not already done

    check = sqlite3.connect(db_path)
    names = {
        r[0]
        for r in check.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    check.close()

    expected = {
        "_honker_notifications",
        "_honker_live",
        "_honker_dead",
        "_honker_locks",
        "_honker_rate_limits",
        "_honker_scheduler_tasks",
        "_honker_results",
        "_honker_stream",
        "_honker_stream_consumers",
    }
    missing = expected - names
    assert not missing, f"missing tables on fresh DB: {missing}"


# ---------------------------------------------------------------------
# Upgrade from the latest published release.
#
# The tests above build legacy schemas by hand. These let the real
# previous release write the file: the newest honker from PyPI (in a
# scratch venv, via uv) or the newest honker-node from npm (in a
# scratch directory) populates a database with due, delayed, in-flight,
# exhausted and expiring jobs plus a schedule, and exits. The binding
# under test then opens the same file and has to run everything
# correctly, which covers the #180 bootstrap migration ('scheduled'
# state, new indexes) on data a user would actually have. The
# migration lives in honker-core, so the current Python binding reads
# both files.
#
# Needs network. Skips when uv/npm or the registry is unavailable,
# unless HONKER_REQUIRE_UPGRADE=1 (set in CI), where that is a failure.
# HONKER_UPGRADE_FROM pins the PyPI version; the default is the newest.
# ---------------------------------------------------------------------

import asyncio  # noqa: E402
import json  # noqa: E402
import os  # noqa: E402
import shutil  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402
import time  # noqa: E402

import pytest  # noqa: E402

_OLD_PYPI_WRITER = r"""
import json, sys, time
import importlib.metadata as md
import honker

db = honker.open(sys.argv[1])
ids = {"version": md.version("honker"), "file": honker.__file__}
up = db.queue("up")
ids["due"] = up.enqueue({"k": "due"})
ids["delayed"] = up.enqueue({"k": "delayed"}, delay=4)
ids["expiring"] = up.enqueue({"k": "expiring"}, delay=1, expires=1)

inflight = db.queue("up-inflight", visibility_timeout_s=1)
ids["inflight"] = inflight.enqueue({"k": "inflight"})
assert inflight.claim_one("old-worker").id == ids["inflight"]

exhausted = db.queue("up-exhausted", visibility_timeout_s=1, max_attempts=1)
ids["exhausted"] = exhausted.enqueue({"k": "exhausted"})
assert exhausted.claim_one("old-worker").id == ids["exhausted"]

honker.Scheduler(db).add(
    name="up-beat", queue="up-beats", schedule=honker.every_s(1), payload={"k": "beat"}
)
ids["now"] = time.time()
print(json.dumps(ids), flush=True)
"""

_OLD_NPM_WRITER = r"""
const h = require('@russellthehippo/honker-node');
const db = h.open(process.argv[1]);
const ids = {
  version: require('@russellthehippo/honker-node/package.json').version,
  file: require.resolve('@russellthehippo/honker-node'),
};
const up = db.queue('up');
ids.due = up.enqueue({ k: 'due' });
ids.delayed = up.enqueue({ k: 'delayed' }, { delay: 4 });
ids.expiring = up.enqueue({ k: 'expiring' }, { delay: 1, expires: 1 });
const inflight = db.queue('up-inflight', { visibilityTimeoutS: 1 });
ids.inflight = inflight.enqueue({ k: 'inflight' });
if (inflight.claimOne('old-worker').id !== ids.inflight) throw new Error('claim');
const exhausted = db.queue('up-exhausted', { visibilityTimeoutS: 1, maxAttempts: 1 });
ids.exhausted = exhausted.enqueue({ k: 'exhausted' });
if (exhausted.claimOne('old-worker').id !== ids.exhausted) throw new Error('claim');
db.scheduler().add({ name: 'up-beat', queue: 'up-beats', cron: '@every 1s', payload: { k: 'beat' } });
ids.now = Date.now() / 1000;
console.log(JSON.stringify(ids));
db.close();
"""


def _skip_or_fail(msg):
    if os.environ.get("HONKER_REQUIRE_UPGRADE") == "1":
        pytest.fail(msg)
    pytest.skip(msg)


def _write_with_npm_release(tmp_path, db_file) -> dict:
    node, npm = shutil.which("node"), shutil.which("npm")
    if node is None or npm is None:
        _skip_or_fail("node/npm not on PATH")
    root = tmp_path / "old-npm"
    root.mkdir()
    (root / "package.json").write_text('{"private": true}')
    res = subprocess.run(
        [npm, "install", "--no-audit", "--no-fund", "--silent",
         "@russellthehippo/honker-node"],
        capture_output=True, text=True, timeout=300, cwd=root,
    )
    if res.returncode != 0:
        _skip_or_fail(f"could not install honker-node from npm: {res.stderr[-800:]}")
    res = subprocess.run(
        [node, "-e", _OLD_NPM_WRITER, db_file],
        capture_output=True, text=True, timeout=60, cwd=root,
    )
    assert res.returncode == 0, res.stderr
    return json.loads(res.stdout)


def _write_with_pypi_release(tmp_path, db_file) -> dict:
    old_py = _old_release_python(tmp_path)
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    res = subprocess.run(
        [old_py, "-c", _OLD_PYPI_WRITER, db_file],
        capture_output=True, text=True, timeout=60, cwd=tmp_path, env=env,
    )
    assert res.returncode == 0, res.stderr
    return json.loads(res.stdout)


def _old_release_python(tmp_path) -> str:
    uv = shutil.which("uv")
    if uv is None:
        _skip_or_fail("uv not on PATH")
    venv = tmp_path / "old-release"
    spec = "honker"
    if os.environ.get("HONKER_UPGRADE_FROM"):
        spec = f"honker=={os.environ['HONKER_UPGRADE_FROM']}"
    py = venv / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    steps = [
        [uv, "venv", "-q", "--python", sys.executable, str(venv)],
        [uv, "pip", "install", "-q", "--python", str(py), "--no-sources", spec],
    ]
    for cmd in steps:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=300, cwd=tmp_path)
        if res.returncode != 0:
            _skip_or_fail(f"could not install {spec} from PyPI: {res.stderr[-800:]}")
    return str(py)


@pytest.mark.parametrize("release", ["pypi", "npm"])
def test_db_written_by_latest_release_runs_correctly_after_upgrade(tmp_path, release):
    db_file = str(tmp_path / "upgrade.db")
    if release == "pypi":
        ids = _write_with_pypi_release(tmp_path, db_file)
        assert "old-release" in ids["file"], f"old writer imported {ids['file']}"
    else:
        ids = _write_with_npm_release(tmp_path, db_file)
        assert "old-npm" in ids["file"], f"old writer loaded {ids['file']}"
    print(f"upgrading a database written by {release} release {ids['version']}")

    db = honker.open(db_file)
    up = db.queue("up")

    # #180 bootstrap migration: the future job is 'scheduled' now and
    # the claim-path indexes exist.
    delayed_row = up.get_job(ids["delayed"])
    assert delayed_row["state"] == "scheduled"
    assert up.get_job(ids["due"])["state"] == "pending"
    indexes = {
        r["name"]
        for r in db.query(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='_honker_live'"
        )
    }
    assert {
        "_honker_live_ready",
        "_honker_live_scheduled",
        "_honker_live_expiry",
        "_honker_live_processing_deadline",
    } <= indexes

    async def run_up_queue():
        # A normal claim loop on the upgraded file. It must get the due
        # job at once and the delayed job when it falls due, nothing else.
        seen = []
        async for job in up.claim("new-worker"):
            seen.append((job.payload["k"], time.time(), job.attempts))
            assert job.ack()
            if job.payload["k"] == "delayed":
                return seen

    seen = asyncio.run(asyncio.wait_for(run_up_queue(), timeout=20))
    assert [s[0] for s in seen] == ["due", "delayed"]
    run_at = delayed_row["run_at"]  # whole seconds
    assert seen[1][1] >= run_at - 0.05, "delayed job ran early"
    assert seen[1][1] <= run_at + 1.5, "delayed job ran late"

    # The in-flight job's lease lapsed: reclaimed as attempt 2.
    inflight = db.queue("up-inflight").claim_one("new-worker")
    assert (inflight.id, inflight.attempts) == (ids["inflight"], 2)
    assert inflight.ack()
    # The exhausted job is dead-lettered, never handed out again.
    assert db.queue("up-exhausted").claim_one("new-worker") is None
    dead = {
        r["id"]: r["last_error"]
        for r in db.query("SELECT id, last_error FROM _honker_dead")
    }
    assert dead == {
        ids["exhausted"]: "max attempts exceeded",
        ids["expiring"]: "expired",
    }

    # The schedule the old release registered keeps firing.
    async def run_scheduler(seconds):
        stop = asyncio.Event()
        asyncio.get_running_loop().call_later(seconds, stop.set)
        await honker.Scheduler(db).run(stop)

    asyncio.run(run_scheduler(2.5))
    beats = db.query("SELECT COUNT(*) AS c FROM _honker_live WHERE queue='up-beats'")[0]["c"]
    assert beats >= 1, "the old release's schedule did not fire after the upgrade"
    assert db.query("PRAGMA integrity_check")[0]["integrity_check"] == "ok"
