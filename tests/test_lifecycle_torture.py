"""Multi-process, model-checked lifecycle torture test for the core queue.

Separate OS processes drive the raw loadable extension through stdlib
``sqlite3`` on one file-backed WAL database with the real clock. They
enqueue, claim, ack, retry, fail, heartbeat, stall past the lease and
cancel at random. Some share a worker id; a coordinator SIGKILLs some
mid-run and restarts them with the same id. Every process writes a
ledger. After a quiesce and drain, ``lifecycle_torture.check`` checks
the invariants in ``INVARIANTS`` against the ledgers and the final
database. See the module docstring of ``tests/lifecycle_torture.py``.

Run it: ``python -m pytest -o addopts="" -n 0 tests/test_lifecycle_torture.py -rxX``
(it is marked ``slow``, so the default run deselects it).

Knobs (environment):

* ``HONKER_TORTURE_SECONDS``: run length per seed. Default 25 (PR CI).
  The nightly run sets a few minutes.
* ``HONKER_TORTURE_SEEDS``: comma-separated seeds. Default ``1``.
* ``HONKER_TORTURE_PROCS``: worker processes. Default 6.
* ``HONKER_TORTURE_SHARED_IDS``: ``0`` gives each live process its own
  worker id. Default ``1``: two pairs of live processes share an id,
  which models a restarted worker whose old incarnation is still
  finishing a stalled handler.
* ``HONKER_TORTURE_FENCED``: ``0`` makes handlers call the legacy
  unfenced ``honker_ack(id, worker)`` etc. instead of the fenced
  ``honker_ack(id, worker, attempt)`` forms. With shared worker ids the
  fencing invariant then fails (#176) and is marked xfail.
* ``HONKER_EXTENSION_PATH``: the extension to load. Default
  ``target/release/libhonker_ext.{dylib,so}``.

This test fails, rather than skips, when the extension is missing or
the interpreter's sqlite3 cannot load extensions. It only skips on
Windows, which has no SIGKILL.

Every invariant is expected to pass. Expiry (5) guards #177: a job
that expires in flight (abandoned, stalled or SIGKILLed handlers with
short ``expires``) must end in ``_honker_dead``, not stay live. Fencing
(2) passes with the fenced forms (#176) and is only xfail when
``HONKER_TORTURE_FENCED=0`` drives the legacy unfenced forms.
"""

import itertools
import json
import os
import sqlite3
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lifecycle_torture as lt  # noqa: E402

SECONDS = float(os.environ.get("HONKER_TORTURE_SECONDS", "25"))
SEEDS = [int(s) for s in os.environ.get("HONKER_TORTURE_SEEDS", "1").split(",") if s.strip()]
PROCS = int(os.environ.get("HONKER_TORTURE_PROCS", "6"))
# 0 gives every live process its own worker id (restarts still reuse
# the killed process's id). Used to isolate mutations from the known
# same-id fencing bug.
SHARED_IDS = os.environ.get("HONKER_TORTURE_SHARED_IDS", "1") != "0"
# 0 drives the legacy unfenced ack/retry/fail/heartbeat arities.
FENCED = os.environ.get("HONKER_TORTURE_FENCED", "1") != "0"

FENCING_ISSUE = "https://github.com/russellromney/honker/issues/176"

KNOWN_BUGS = {}
if not FENCED:
    KNOWN_BUGS["2_fencing"] = (
        "legacy unfenced ack/retry/fail/heartbeat check worker_id + lease, not the attempt, "
        "so a stale handler with the same worker id acts on the new attempt. " + FENCING_ISSUE
    )

pytestmark = [
    # ~30 s per seed, so it is kept out of the default `-n auto` run and
    # run on its own (CI: a dedicated step with `-n 0`). Under xdist,
    # each worker would start its own torture run.
    pytest.mark.slow,
    pytest.mark.skipif(
        sys.platform == "win32", reason="the torture coordinator needs SIGKILL (POSIX only)"
    ),
]

_RUNS = {}


def _require_extension():
    ext = lt.find_extension()
    if ext is None:
        pytest.fail(
            "honker extension not found (set HONKER_EXTENSION_PATH or run "
            "`cargo build -p honker-extension --release`). The torture test does not skip.",
            pytrace=False,
        )
    if not hasattr(sqlite3.connect(":memory:"), "enable_load_extension"):
        pytest.fail(
            f"{sys.executable}'s sqlite3 cannot load extensions; use an interpreter built "
            "with SQLITE_ENABLE_LOAD_EXTENSION (e.g. uv-managed Python). The torture test does not skip.",
            pytrace=False,
        )
    return ext


@pytest.fixture(scope="module", params=SEEDS, ids=lambda s: f"seed{s}")
def report(request, tmp_path_factory):
    seed = request.param
    if seed not in _RUNS:
        ext = _require_extension()
        workdir = str(tmp_path_factory.mktemp(f"torture-seed{seed}"))
        res = lt.run_torture(workdir, ext, seed, SECONDS, PROCS, SHARED_IDS, FENCED)
        rep = lt.check(res)
        print(f"\n[torture seed={seed} seconds={SECONDS} fenced={FENCED}] workdir={workdir}\n{rep.stats}")
        _RUNS[seed] = rep
    return _RUNS[seed]


def _invariant_params():
    for inv in lt.INVARIANTS:
        marks = []
        if inv in KNOWN_BUGS:
            marks.append(pytest.mark.xfail(reason=KNOWN_BUGS[inv], raises=AssertionError, strict=False))
        yield pytest.param(inv, marks=marks, id=inv)


def test_run_exercised_the_lifecycle(report):
    """Guard against a run that silently did nothing interesting."""
    s = report.stats
    ops = s["lifecycle_by_op"]
    problems = []
    if s["claims"] < 50:
        problems.append(f"only {s['claims']} claims")
    for op in ("ack", "retry", "fail", "heartbeat", "cancel"):
        if ops[op]["ok"] == 0:
            problems.append(f"no successful {op}")
        if op != "cancel" and ops[op]["miss"] == 0 and SECONDS >= 20:
            problems.append(f"no stale-owner {op} miss")
    if s["kills"] == 0 and SECONDS >= 10:
        problems.append("no SIGKILL happened")
    # #177: the run must actually put jobs through in-flight expiry, or
    # invariant 5 proves nothing about it.
    if s["expired_in_flight"] == 0 and SECONDS >= 20:
        problems.append("no job expired in flight")
    assert not problems, f"seed={report.seed}: weak run: {problems}; stats={s}"


@pytest.mark.parametrize("invariant", list(_invariant_params()))
def test_invariant(report, invariant):
    assert not report.by_invariant(invariant), report.describe(invariant)



# ---------------------------------------------------------------------
# Scheduler tick across processes (#173)
#
# `honker_scheduler_tick` takes the write lock with its first statement
# and does all its work in one savepoint. These tests drive it from
# separate OS processes through the raw extension, with no transaction
# around the tick (autocommit) and inside a deferred BEGIN.
# ---------------------------------------------------------------------

# One process. argv: tests dir, extension, db, mode, seconds, ledger.
#   tick      SELECT honker_scheduler_tick(unixepoch()) in autocommit
#   deferred  BEGIN; tick at a synthetic clock one second further on
#             every round; COMMIT. Every round has a due boundary.
#   noise     commit an enqueue into another queue, over and over
# Every mode pauses 1 ms per round. A process that retakes the
# lock at once starves the other's busy_timeout retries.
# The ledger is JSON: {"fires": [...], "errors": [...], "rounds": n}.
_TICK_PROC = r"""
import json, sqlite3, sys, time
sys.path.insert(0, sys.argv[1])
import lifecycle_torture as lt
ext, db, mode, seconds, ledger = sys.argv[2:7]
conn = lt.connect(db, ext)
fires, errors, rounds = [], [], 0
start = conn.execute("SELECT unixepoch()").fetchone()[0]
deadline = time.monotonic() + float(seconds)
while time.monotonic() < deadline:
    rounds += 1
    try:
        if mode == "tick":
            out = conn.execute("SELECT honker_scheduler_tick(unixepoch())").fetchone()[0]
            fires.extend(json.loads(out))
            time.sleep(0.001)
        elif mode == "deferred":
            conn.execute("BEGIN")
            try:
                out = conn.execute(
                    "SELECT honker_scheduler_tick(?)", (start + rounds,)
                ).fetchone()[0]
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            fires.extend(json.loads(out))
            time.sleep(0.001)
        else:
            conn.execute("SELECT honker_enqueue('noise', '{}', NULL, NULL, 0, 3, NULL)")
            time.sleep(0.001)
    except sqlite3.Error as e:
        errors.append(str(e))
with open(ledger, "w") as f:
    json.dump({"fires": fires, "errors": errors, "rounds": rounds}, f)
"""

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))


def _sched_db(tmp_path, ext, every="@every 1s"):
    db = str(tmp_path / "sched.db")
    conn = lt.connect(db, ext)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("SELECT honker_bootstrap()")
    conn.execute(
        "SELECT honker_scheduler_register('every', 'sched', ?, '{}', 0, NULL, 3)", (every,)
    )
    return db, conn


def _run_procs(tmp_path, ext, db, modes, seconds):
    procs = []
    for i, mode in enumerate(modes):
        ledger = str(tmp_path / f"ledger-{i}-{mode}.json")
        p = subprocess.Popen(
            [sys.executable, "-c", _TICK_PROC, _TESTS_DIR, ext, db, mode, str(seconds), ledger]
        )
        procs.append((p, ledger))
    out = []
    for p, ledger in procs:
        assert p.wait(timeout=seconds + 60) == 0, f"tick process exited {p.returncode}"
        with open(ledger) as f:
            out.append(json.load(f))
    return out


def _assert_one_job_per_boundary(conn, fires):
    fire_ats = sorted(f["fire_at"] for f in fires)
    dupes = sorted({t for t in fire_ats if fire_ats.count(t) > 1})
    assert not dupes, f"boundaries enqueued more than once: {dupes}"
    # The tickers run without a break, so no boundary is skipped and
    # the catch-up cap never applies.
    gaps = [(a, b) for a, b in itertools.pairwise(fire_ats) if b != a + 1]
    assert not gaps, f"missed boundaries between: {gaps}"
    jobs = conn.execute("SELECT count(*) FROM _honker_live WHERE queue = 'sched'").fetchone()[0]
    assert jobs == len(fires), f"{jobs} jobs for {len(fires)} reported fires"
    nfa = conn.execute(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name = 'every'"
    ).fetchone()[0]
    assert nfa == fire_ats[-1] + 1, "next_fire_at is the boundary after the last fire"


def test_scheduler_two_autocommit_tickers_enqueue_each_boundary_once(tmp_path):
    """Two processes tick `@every 1s` in autocommit for 10 s: one job per
    boundary, no duplicates, no gaps, no errors."""
    ext = _require_extension()
    db, conn = _sched_db(tmp_path, ext)
    ledgers = _run_procs(tmp_path, ext, db, ["tick", "tick"], 10)
    errors = [e for led in ledgers for e in led["errors"]]
    assert not errors, f"tick errors: {errors[:5]} ({len(errors)} total)"
    fires = [f for led in ledgers for f in led["fires"]]
    assert len(fires) >= 8, f"only {len(fires)} fires in 10 s"
    _assert_one_job_per_boundary(conn, fires)


def test_scheduler_tick_in_deferred_transaction_survives_concurrent_commits(tmp_path):
    """A tick inside a deferred BEGIN, with another process committing
    all the time, never fails with "database is locked"."""
    ext = _require_extension()
    db, conn = _sched_db(tmp_path, ext)
    ledgers = _run_procs(tmp_path, ext, db, ["deferred", "noise"], 5)
    ticker, noise = ledgers
    assert noise["rounds"] > 100 and not noise["errors"], (noise["rounds"], noise["errors"][:5])
    print(f"deferred ticker: {ticker['rounds']} rounds; noise: {noise['rounds']} commits")
    errors = ticker["errors"]
    assert not errors, (
        f"{len(errors)} of {ticker['rounds']} ticks failed, e.g. {errors[:3]}"
    )
    assert len(ticker["fires"]) >= 50, f"only {len(ticker['fires'])} fires"
    _assert_one_job_per_boundary(conn, ticker["fires"])


def test_scheduler_failed_tick_leaves_no_orphan_and_no_advance(tmp_path):
    """A trigger fails the third enqueue of one tick (run in another
    process, autocommit). Nothing is enqueued and next_fire_at stays.
    Once the trigger is gone, the next tick fires each boundary once."""
    ext = _require_extension()
    db, conn = _sched_db(tmp_path, ext)
    first = conn.execute(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name = 'every'"
    ).fetchone()[0]
    conn.execute(
        "CREATE TRIGGER boom BEFORE INSERT ON _honker_live "
        "WHEN NEW.queue = 'sched' "
        "AND (SELECT count(*) FROM _honker_live WHERE queue = 'sched') >= 2 "
        "BEGIN SELECT RAISE(ABORT, 'boom'); END"
    )
    at = first + 4
    peer = (
        "import sqlite3, sys; sys.path.insert(0, sys.argv[1]); import lifecycle_torture as lt\n"
        "c = lt.connect(sys.argv[3], sys.argv[2])\n"
        "try:\n"
        "    print(c.execute('SELECT honker_scheduler_tick(?)', (int(sys.argv[4]),)).fetchone()[0])\n"
        "except sqlite3.Error as e:\n"
        "    print('ERROR', e)\n"
    )
    def tick_elsewhere():
        return subprocess.run(
            [sys.executable, "-c", peer, _TESTS_DIR, ext, db, str(at)],
            check=True, capture_output=True, text=True, timeout=60,
        ).stdout.strip()

    out = tick_elsewhere()
    assert out.startswith("ERROR") and "boom" in out, out
    jobs = conn.execute("SELECT count(*) FROM _honker_live WHERE queue = 'sched'").fetchone()[0]
    assert jobs == 0, f"{jobs} orphan jobs after a failed tick"
    nfa = conn.execute(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name = 'every'"
    ).fetchone()[0]
    assert nfa == first, "a failed tick must not advance next_fire_at"

    conn.execute("DROP TRIGGER boom")
    fires = json.loads(tick_elsewhere())
    assert [f["fire_at"] for f in fires] == list(range(first, at + 1))
    _assert_one_job_per_boundary(conn, fires)
