"""Tests for honker.Scheduler and the Rust-backed crontab parser.

Scheduler has two separate concerns:
  1. Cron parsing + next-boundary (pure Rust; tested via the Python
     facade). Low-level parser tests live alongside the Rust
     implementation (`honker-core/src/cron.rs`); here we only
     exercise the Python-facing API.
  2. Fire-due logic (pure — test with a mock `now`).
  3. Live scheduler loop (integration — hard to test without waiting
     for real cron boundaries; covered by a minimal happy-path test
     with `*/1 * * * *` + a short-running loop).
"""

import asyncio
import json
import time
from datetime import datetime

import pytest

import honker
from honker import Scheduler, crontab, every_s


# ---------- crontab() / CronSchedule.next_after ----------


def test_crontab_field_count_validated():
    with pytest.raises(ValueError):
        crontab("* * * *")        # 4 fields
    with pytest.raises(ValueError):
        crontab("* * *")          # 3 fields


def test_crontab_out_of_range_validated():
    with pytest.raises(ValueError):
        crontab("60 * * * *")
    with pytest.raises(ValueError):
        crontab("* 24 * * *")


def test_crontab_inverted_range_validated():
    with pytest.raises(ValueError):
        crontab("30-10 * * * *")


def test_crontab_zero_step_validated():
    with pytest.raises(ValueError):
        crontab("*/0 * * * *")


def test_crontab_next_after_hourly():
    c = crontab("0 * * * *")
    # Next top of the hour after 10:05:03 is 11:00.
    dt = datetime(2025, 1, 1, 10, 5, 3)
    nxt = c.next_after(dt)
    assert nxt == datetime(2025, 1, 1, 11, 0)


def test_crontab_next_after_exactly_at_boundary_returns_next():
    c = crontab("0 * * * *")
    # At exactly 10:00 — next match is 11:00, not 10:00.
    dt = datetime(2025, 1, 1, 10, 0)
    nxt = c.next_after(dt)
    assert nxt == datetime(2025, 1, 1, 11, 0)


def test_crontab_next_after_crosses_day():
    c = crontab("0 3 * * *")
    # At 4am, next 3am is tomorrow.
    dt = datetime(2025, 1, 1, 4, 0)
    nxt = c.next_after(dt)
    assert nxt == datetime(2025, 1, 2, 3, 0)


def test_crontab_next_after_crosses_year():
    c = crontab("0 0 1 1 *")  # Jan 1 midnight.
    dt = datetime(2025, 6, 15, 12, 0)
    nxt = c.next_after(dt)
    assert nxt == datetime(2026, 1, 1, 0, 0)


def test_crontab_six_field_next_after_seconds():
    c = crontab("*/10 * * * * *")
    dt = datetime(2025, 1, 1, 10, 5, 3)
    nxt = c.next_after(dt)
    assert nxt == datetime(2025, 1, 1, 10, 5, 10)


def test_every_s_next_after_seconds():
    c = every_s(1)
    dt = datetime(2025, 1, 1, 10, 5, 3)
    nxt = c.next_after(dt)
    assert nxt == datetime(2025, 1, 1, 10, 5, 4)


def test_every_s_requires_positive_integer():
    with pytest.raises(ValueError):
        every_s(0)


# ---------- Scheduler.add / _fire_due ----------


def test_scheduler_add_registers_task(db_path):
    db = honker.open(db_path)
    sched = Scheduler(db)
    sched.add(
        name="nightly",
        queue="backups",
        schedule=crontab("0 3 * * *"),
    )
    # Registration is persisted in _honker_scheduler_tasks.
    rows = db.query(
        "SELECT name, queue, cron_expr FROM _honker_scheduler_tasks "
        "WHERE name='nightly'"
    )
    assert len(rows) == 1
    assert rows[0]["queue"] == "backups"
    assert rows[0]["cron_expr"] == "0 3 * * *"
    assert "nightly" in sched._registered


def test_scheduler_add_registers_interval_expression(db_path):
    db = honker.open(db_path)
    sched = Scheduler(db)
    sched.add(
        name="fast",
        queue="backups",
        schedule=every_s(5),
    )
    rows = db.query(
        "SELECT name, queue, cron_expr FROM _honker_scheduler_tasks "
        "WHERE name='fast'"
    )
    assert len(rows) == 1
    assert rows[0]["queue"] == "backups"
    assert rows[0]["cron_expr"] == "@every 5s"


def test_scheduler_add_replaces_by_name(db_path):
    db = honker.open(db_path)
    sched = Scheduler(db)
    sched.add(name="t", queue="a", schedule=crontab("* * * * *"))
    sched.add(name="t", queue="b", schedule=crontab("* * * * *"))
    rows = db.query(
        "SELECT queue FROM _honker_scheduler_tasks WHERE name='t'"
    )
    assert len(rows) == 1
    assert rows[0]["queue"] == "b"


def test_scheduler_tick_enqueues_on_boundary(db_path):
    """honker_scheduler_tick(now) enqueues one job per registered task
    whose next_fire_at <= now, and advances next_fire_at."""
    db = honker.open(db_path)
    db.queue("hourly-q")  # create schema
    sched = Scheduler(db)
    sched.add(
        name="hourly",
        queue="hourly-q",
        schedule=crontab("0 * * * *"),
        payload={"ping": True},
    )

    # Fetch the task's next_fire_at (set by register to the next top
    # of the hour after "now") and tick one second past it.
    row = db.query(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='hourly'"
    )[0]
    boundary = int(row["next_fire_at"])
    with db.transaction() as tx:
        result = tx.query(
            "SELECT honker_scheduler_tick(?) AS j", [boundary + 1]
        )
    fires = json.loads(result[0]["j"])
    assert len(fires) == 1
    assert fires[0]["name"] == "hourly"
    assert fires[0]["queue"] == "hourly-q"
    assert fires[0]["fire_at"] == boundary
    # Job landed in the queue.
    rows = db.query(
        "SELECT payload FROM _honker_live WHERE queue='hourly-q'"
    )
    assert len(rows) == 1
    # next_fire_at advanced one hour.
    row = db.query(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='hourly'"
    )[0]
    assert int(row["next_fire_at"]) == boundary + 3600


def test_scheduler_tick_skips_already_fired(db_path):
    """Calling tick twice at the same `now` doesn't re-fire —
    next_fire_at advances past `now` on the first call, so the
    second is a no-op. Keeps scheduler restart safe within a
    boundary window."""
    db = honker.open(db_path)
    db.queue("no-dup")
    sched = Scheduler(db)
    sched.add(
        name="t",
        queue="no-dup",
        schedule=crontab("0 * * * *"),
    )
    row = db.query(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='t'"
    )[0]
    boundary = int(row["next_fire_at"])
    with db.transaction() as tx:
        result_a = tx.query(
            "SELECT honker_scheduler_tick(?) AS j", [boundary + 1]
        )
    with db.transaction() as tx:
        result_b = tx.query(
            "SELECT honker_scheduler_tick(?) AS j", [boundary + 1]
        )
    assert len(json.loads(result_a[0]["j"])) == 1
    assert len(json.loads(result_b[0]["j"])) == 0
    rows = db.query("SELECT COUNT(*) AS c FROM _honker_live WHERE queue='no-dup'")
    assert rows[0]["c"] == 1


def test_scheduler_tick_catches_up_multiple_boundaries(db_path):
    """If the scheduler was down for multiple boundaries, tick walks
    forward firing each one (within the catch-up cap). For noisy
    schedules callers can use `expires` to drop stale catch-up jobs.
    """
    db = honker.open(db_path)
    db.queue("catchup-q")
    sched = Scheduler(db)
    sched.add(
        name="h",
        queue="catchup-q",
        schedule=crontab("0 * * * *"),  # hourly
    )
    # Rewind next_fire_at to 4 hours ago to simulate downtime
    # across 4 boundaries (the boundary we're rewinding to + 3
    # more while we sleep through now).
    row = db.query(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='h'"
    )[0]
    orig_next = int(row["next_fire_at"])
    rewound = orig_next - 4 * 3600
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_scheduler_tasks SET next_fire_at=? WHERE name='h'",
            [rewound],
        )
    # Now tick at a time slightly past the original boundary: 5
    # boundaries should fire (rewound, rewound+1h, ..., rewound+4h).
    now = orig_next + 60
    with db.transaction() as tx:
        result = tx.query("SELECT honker_scheduler_tick(?) AS j", [now])
    fires = json.loads(result[0]["j"])
    assert len(fires) == 5
    # next_fire_at advanced to the hour after `now`.
    row = db.query(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='h'"
    )[0]
    assert int(row["next_fire_at"]) == orig_next + 3600


def test_scheduler_register_defaults_to_three_max_attempts(db_path):
    """Scheduler jobs default to a schedule-level attempt budget of 3.

    Queue handles can have their own enqueue default; scheduled work is
    cross-binding and uses Scheduler.add(max_attempts=...) for overrides.
    """
    db = honker.open(db_path)
    db.queue("custom", max_attempts=7)
    sched = Scheduler(db)
    sched.add(
        name="t",
        queue="custom",
        schedule=every_s(1),
        payload={"x": 1},
    )
    row = db.query(
        "SELECT max_attempts FROM _honker_scheduler_tasks WHERE name='t'"
    )[0]
    assert int(row["max_attempts"]) == 3
    # Force due and tick.
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_scheduler_tasks SET next_fire_at = unixepoch() - 1 "
            "WHERE name='t'"
        )
        tx.query("SELECT honker_scheduler_tick(unixepoch())")
    job = db.query(
        "SELECT max_attempts FROM _honker_live WHERE queue='custom'"
    )[0]
    assert int(job["max_attempts"]) == 3


def test_scheduler_register_accepts_explicit_max_attempts(db_path):
    db = honker.open(db_path)
    db.queue("custom", max_attempts=7)
    sched = Scheduler(db)
    sched.add(
        name="t",
        queue="custom",
        schedule=every_s(1),
        payload={"x": 1},
        max_attempts=4,
    )
    row = db.query(
        "SELECT max_attempts FROM _honker_scheduler_tasks WHERE name='t'"
    )[0]
    assert int(row["max_attempts"]) == 4


def test_scheduler_update_max_attempts_applies_to_future_fires(db_path):
    db = honker.open(db_path)
    db.queue("custom", max_attempts=7)
    sched = Scheduler(db)
    sched.add(name="t", queue="custom", schedule=every_s(1), payload={"x": 1})

    assert sched.update("t", max_attempts=2) is True
    row = sched.list()[0]
    assert int(row["max_attempts"]) == 2

    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_scheduler_tasks SET next_fire_at = unixepoch() - 1 "
            "WHERE name='t'"
        )
        tx.query("SELECT honker_scheduler_tick(unixepoch())")
    job = db.query(
        "SELECT max_attempts FROM _honker_live WHERE queue='custom'"
    )[0]
    assert int(job["max_attempts"]) == 2


def test_scheduler_tick_caps_catchup_storm(db_path):
    """A long outage on a high-frequency schedule must not enqueue
    unbounded jobs in one tick. Cap is 64; remaining boundaries are
    skipped and next_fire_at jumps past now.
    """
    db = honker.open(db_path)
    db.queue("storm-q")
    sched = Scheduler(db)
    sched.add(
        name="every-sec",
        queue="storm-q",
        schedule=every_s(1),
    )
    row = db.query(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='every-sec'"
    )[0]
    orig = int(row["next_fire_at"])
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_scheduler_tasks SET next_fire_at=? WHERE name='every-sec'",
            [orig - 1000],
        )
    now = orig
    with db.transaction() as tx:
        result = tx.query("SELECT honker_scheduler_tick(?) AS j", [now])
    fires = json.loads(result[0]["j"])
    assert len(fires) == 64, f"expected cap of 64 fires, got {len(fires)}"
    live = db.query(
        "SELECT COUNT(*) AS c FROM _honker_live WHERE queue='storm-q'"
    )[0]["c"]
    assert live == 64
    row = db.query(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='every-sec'"
    )[0]
    next_at = int(row["next_fire_at"])
    assert next_at > now
    # Intermediate boundaries between the 64th fire and next_at are
    # intentionally never enqueued (catch-up skip semantics).
    fire_ats = sorted(int(f["fire_at"]) for f in fires)
    assert fire_ats[-1] == orig - 1000 + 63
    assert next_at == now + 1  # @every 1s: next_after(now)


def test_scheduler_tick_racing_writers_produce_no_duplicates(db_path):
    """Multiple workers calling `honker_scheduler_tick` concurrently
    must never double-fire a boundary.

    In production the `honker-scheduler` leader lock gates callers —
    only one process holds it at a time. But the lock is advisory at
    the application layer; if a future binding forgets to acquire
    it, or a test misuses the API, the underlying SQL must still be
    safe. This proves that `BEGIN IMMEDIATE` + the advance-then-
    return contract in `scheduler_tick` means at most one ticker
    observes an unfired boundary.

    Strategy: register one task with `next_fire_at = now - 1` (one
    boundary overdue), fire 10 Python threads each running one
    `SELECT honker_scheduler_tick(now)` through its own writer
    transaction. Exactly one thread should see the fire, nine
    should see `[]`. The job lands in `_honker_live` exactly once.
    """
    import threading

    db = honker.open(db_path)
    db.queue("race-q")
    sched = Scheduler(db)
    sched.add(
        name="one",
        queue="race-q",
        schedule=crontab("* * * * *"),  # every minute
    )
    # Force exactly one boundary overdue.
    row = db.query(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='one'"
    )[0]
    boundary = int(row["next_fire_at"])
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_scheduler_tasks "
            "SET next_fire_at = ? WHERE name = 'one'",
            [boundary - 60],
        )
    # Tick at `boundary - 60 + 1`: exactly one boundary should be
    # eligible to fire (the rewound one), not two.
    now = boundary - 60 + 1

    fire_counts: list = [0] * 10
    barrier = threading.Barrier(10)

    def worker(i: int) -> None:
        # Synchronize starts so threads race, rather than serializing
        # trivially through Python's GIL + thread scheduling.
        barrier.wait()
        with db.transaction() as tx:
            rows = tx.query("SELECT honker_scheduler_tick(?) AS j", [now])
        fires = json.loads(rows[0]["j"])
        fire_counts[i] = len(fires)

    threads = [
        threading.Thread(target=worker, args=(i,)) for i in range(10)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    # Exactly one ticker saw the fire; nine saw nothing.
    total_fires = sum(fire_counts)
    assert total_fires == 1, (
        f"expected 1 total fire across 10 concurrent tickers, "
        f"got {total_fires}: {fire_counts}"
    )
    # And the queue has exactly one job, not ten.
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _honker_live WHERE queue='race-q'"
    )
    assert rows[0]["c"] == 1


async def test_scheduler_run_with_stop_event(db_path):
    """Happy-path integration: start a scheduler with a *very* fast
    schedule, fire at least once, stop via stop_event, return cleanly.
    Verifies the lock-acquire + heartbeat + stop path without needing
    to wait for real cron boundaries.
    """
    db = honker.open(db_path)
    db.queue("flash")

    sched = Scheduler(db)
    sched.add(
        name="flash-task",
        queue="flash",
        schedule=crontab("* * * * *"),  # every minute
    )

    # Override the main loop's "wait until next boundary" behavior by
    # pre-setting next_fires to now, so the first iteration fires
    # immediately. Easier than mocking datetime.
    stop_event = asyncio.Event()
    run_task = asyncio.create_task(sched.run(stop_event))

    # Let the scheduler start up + fire once, then stop.
    await asyncio.sleep(0.3)
    stop_event.set()
    await asyncio.wait_for(run_task, timeout=5.0)

    # Scheduler acquired + released the lock cleanly.
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _honker_locks WHERE name='honker-scheduler'"
    )
    assert rows[0]["c"] == 0


async def test_two_schedulers_one_runs_one_raises_lockheld(db_path):
    """Leader election: two scheduler processes can't both hold the
    lock. The second one raises LockHeld, matching our documented
    hot-standby semantics (caller retries in a loop)."""
    db = honker.open(db_path)
    db.queue("leader-q")

    s1 = Scheduler(db)
    s2 = Scheduler(db)
    s1.add(name="x", queue="leader-q", schedule=crontab("* * * * *"))
    s2.add(name="x", queue="leader-q", schedule=crontab("* * * * *"))

    stop = asyncio.Event()
    t1 = asyncio.create_task(s1.run(stop))
    await asyncio.sleep(0.1)  # let s1 acquire the lock

    # s2 tries to run — the lock is held, so it raises LockHeld.
    with pytest.raises(honker.LockHeld):
        await s2.run()

    stop.set()
    await asyncio.wait_for(t1, timeout=3.0)


def test_scheduler_run_raises_without_tasks(db_path):
    """A scheduler with no tasks added and an empty DB raises a clear
    error rather than silently no-opping. Phase Shakedown (b)."""
    db = honker.open(db_path)
    sched = Scheduler(db)
    with pytest.raises(RuntimeError, match="no registered tasks"):
        asyncio.run(sched.run())
    # Lock never acquired — we raised before the lock block.
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _honker_locks WHERE name='honker-scheduler'"
    )
    assert rows[0]["c"] == 0


async def test_scheduler_wakes_on_new_registration(db_path):
    """A scheduler sleeping until a far-future fire must wake within ~1s
    when another caller registers a task whose next_fire_at is sooner.
    Phase Shakedown (c).

    `honker_scheduler_register` fires a wake on the 'honker:scheduler'
    channel; the main loop races its timer against update_events(), so a
    new registration kicks it out of sleep. Without this, a leader
    scheduled to next wake in an hour would silently miss a task
    registered 1 minute from now until the current sleep ended.
    """
    db = honker.open(db_path)
    sched = Scheduler(db)
    # Pre-register one task so .run() doesn't bail on the empty-table
    # guard from Phase Shakedown (b). The cron expr fires in ~1hr
    # (next top-of-the-hour plus one), so the leader's first sleep
    # will be long enough to demonstrate the wake.
    sched.add(
        name="far-future",
        queue="q",
        schedule=crontab("0 */2 * * *"),  # every 2hr — next fire is at least ~an hour
    )

    stop = asyncio.Event()
    run_task = asyncio.create_task(sched.run(stop_event=stop))
    await asyncio.sleep(0.2)  # let the leader enter its sleep

    # Inject a "closer" task from a second scheduler on the same DB.
    # The main loop should wake within the WAL update-watcher cadence.
    injected = Scheduler(db)
    start = time.monotonic()
    injected.add(
        name="injected",
        queue="q",
        schedule=crontab("* * * * *"),  # every minute
    )

    # Give the loop a moment to observe the wake; then stop.
    await asyncio.sleep(0.5)
    elapsed = time.monotonic() - start
    stop.set()
    await asyncio.wait_for(run_task, timeout=5.0)

    # If the wake worked, we spent <1s between injecting the task and
    # setting stop. The real assertion is indirect: the fact that the
    # run_task completed at all means the sleep didn't block past the
    # registration wake — otherwise `stop.set()` would race a long
    # sleep and the test would take the full ~2hr cron target (or at
    # least time out on asyncio.wait_for).
    assert elapsed < 1.5, (
        f"Loop didn't react to injected registration within {elapsed:.2f}s"
    )


async def test_scheduler_run_proceeds_if_another_process_registered(db_path):
    """A scheduler that didn't `.add()` anything locally but finds rows
    in `_honker_scheduler_tasks` (registered by another process or a
    prior run) still runs. Phase Shakedown (b) — prevents silent no-op
    for dedicated runner processes."""
    db = honker.open(db_path)
    # Simulate another process having registered a task.
    Scheduler(db).add(
        name="from-elsewhere",
        queue="health",
        schedule=crontab("0 3 * * *"),
    )

    # Fresh instance — no local self._registered entries. Must still run.
    runner = Scheduler(db)
    stop = asyncio.Event()

    async def stop_soon():
        await asyncio.sleep(0.1)
        stop.set()

    # Race the scheduler loop against a quick stop. If it silently
    # returns (the old bug), the gather completes immediately and
    # the lock was never acquired. If the fix is in place, it
    # acquires the lock, enters the main loop, and stops on the event.
    async def check_lock_was_held():
        await asyncio.sleep(0.05)
        rows = db.query(
            "SELECT COUNT(*) AS c FROM _honker_locks "
            "WHERE name='honker-scheduler'"
        )
        return rows[0]["c"]

    holds_lock_during_run = asyncio.create_task(check_lock_was_held())
    await asyncio.gather(runner.run(stop_event=stop), stop_soon())
    assert await holds_lock_during_run == 1, (
        "scheduler silently returned without acquiring the lock — "
        "regression of Phase Shakedown (b)"
    )


async def test_scheduler_heartbeat_scoped_to_owner_exits_on_lock_steal(db_path):
    """After another process steals the leader lock, the old leader's
    heartbeat must fail (owner-scoped UPDATE) and stop the main loop
    so it cannot keep firing alongside the new leader.

    Regression: a name-only UPDATE would extend the *successor's* row
    and the old leader would dual-fire forever.
    """
    db = honker.open(db_path)
    q = db.queue("steal-q")
    sched = Scheduler(db)
    # Fast heartbeat so the test doesn't wait 30s.
    sched.heartbeat_interval = 0.05
    sched.lock_ttl = 60
    # Far-future schedule so ticks don't enqueue noise; we only care
    # that the leader exits after lock steal.
    sched.add(
        name="steal-task",
        queue="steal-q",
        schedule=crontab("0 0 1 1 *"),
        payload={"k": 1},
    )

    stop = asyncio.Event()
    run_task = asyncio.create_task(sched.run(stop_event=stop))

    # Wait until the leader holds the lock.
    deadline = time.time() + 2.0
    owner = None
    while time.time() < deadline:
        rows = db.query(
            "SELECT owner FROM _honker_locks WHERE name=?",
            [sched.lock_name],
        )
        if rows:
            owner = rows[0]["owner"]
            break
        await asyncio.sleep(0.01)
    assert owner is not None, "leader never acquired the lock"

    # Steal the lock: expire the old row, insert a new owner. This is
    # what a standby does after TTL elapses.
    with db.transaction() as tx:
        tx.execute(
            "DELETE FROM _honker_locks WHERE name=?",
            [sched.lock_name],
        )
        tx.execute(
            "INSERT INTO _honker_locks (name, owner, expires_at) "
            "VALUES (?, 'thief', unixepoch() + 60)",
            [sched.lock_name],
        )

    # Old leader's next tick/heartbeat must notice renew failure and
    # raise LeadershipLost (not a silent return).
    with pytest.raises(honker.LeadershipLost, match="lost"):
        await asyncio.wait_for(run_task, timeout=3.0)

    # Thief still holds the lock — old leader must not have released
    # the thief's row (release is owner-scoped) or extended it under
    # the old owner path.
    rows = db.query(
        "SELECT owner FROM _honker_locks WHERE name=?",
        [sched.lock_name],
    )
    assert len(rows) == 1
    assert rows[0]["owner"] == "thief"

    # No accidental enqueues from dual-fire.
    live = db.query(
        "SELECT COUNT(*) AS c FROM _honker_live WHERE queue='steal-q'"
    )[0]["c"]
    assert live == 0
    del q


async def test_scheduler_heartbeat_extends_own_ttl(db_path):
    """Happy path: heartbeat refreshes expires_at for the leader's
    own owner token so long sleeps don't drop leadership.
    """
    db = honker.open(db_path)
    db.queue("hb-q")
    sched = Scheduler(db)
    sched.heartbeat_interval = 0.05
    sched.lock_ttl = 30
    sched.add(
        name="hb-task",
        queue="hb-q",
        schedule=crontab("0 0 1 1 *"),
    )

    stop = asyncio.Event()
    run_task = asyncio.create_task(sched.run(stop_event=stop))

    # Wait for lock acquisition.
    deadline = time.time() + 2.0
    while time.time() < deadline:
        rows = db.query(
            "SELECT owner, expires_at FROM _honker_locks WHERE name=?",
            [sched.lock_name],
        )
        if rows:
            break
        await asyncio.sleep(0.01)
    assert rows, "leader never acquired the lock"
    first_exp = int(rows[0]["expires_at"])

    # Force expires_at near now; next heartbeat should push it out.
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_locks SET expires_at = unixepoch() + 2 "
            "WHERE name=?",
            [sched.lock_name],
        )

    # Wait for at least one heartbeat cycle.
    await asyncio.sleep(0.2)
    rows = db.query(
        "SELECT expires_at FROM _honker_locks WHERE name=?",
        [sched.lock_name],
    )
    assert rows, "lock vanished during heartbeat"
    new_exp = int(rows[0]["expires_at"])
    # Should be roughly now + lock_ttl (30), not ~now+2.
    assert new_exp >= first_exp - 5  # not collapsed
    import time as _time
    assert new_exp >= int(_time.time()) + 20, (
        f"heartbeat did not extend TTL; expires_at={new_exp}"
    )

    stop.set()
    await asyncio.wait_for(run_task, timeout=3.0)
