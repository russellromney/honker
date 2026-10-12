"""Deployment-shaped end-to-end tests for time-triggered work.

These tests exercise the realistic user shape:
  * one process registers schedules / seeds delayed jobs,
  * one long-running scheduler process enqueues due scheduled jobs,
  * one long-running worker process drains the queue with a large
    `idle_poll_s`, and
  * the parent inspects wall-clock timing of what actually happened.

This is intentionally broader than the focused tests in
`test_scheduler_boundaries.py` and `test_joblite.py`. The goal here is
to prove the whole user story works when the pieces are split across
processes, not just that each component behaves in isolation.
"""

from __future__ import annotations

import json
import os
import queue as queue_mod
import subprocess
import sys
import textwrap
import threading
import time
from collections import defaultdict

import pytest

import honker
from honker import Scheduler, crontab, every_s


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PACKAGES_ROOT = os.path.join(REPO_ROOT, "packages")
HONKER_PYTHON_ROOT = os.path.join(PACKAGES_ROOT, "honker", "python")


_SCHEDULER_PROCESS = r"""
import asyncio
import signal
import sys

sys.path.insert(0, {honker_python!r})
sys.path.insert(0, {packages!r})
import honker

db = honker.open({db_path!r})
scheduler = honker.Scheduler(db)


async def main():
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    print("READY", flush=True)
    await scheduler.run(stop)


asyncio.run(main())
"""


# The hot-standby shape the Scheduler.run docstring tells users to
# build: retry on LockHeld, and drop back to standby on LeadershipLost.
_STANDBY_SCHEDULER_PROCESS = r"""
import asyncio
import signal
import sys

sys.path.insert(0, {honker_python!r})
sys.path.insert(0, {packages!r})
import honker

db = honker.open({db_path!r})


async def main():
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    print("READY", flush=True)
    while not stop.is_set():
        scheduler = honker.Scheduler(db)
        scheduler.lock_ttl = {lock_ttl!r}
        scheduler.heartbeat_interval = 1
        try:
            await scheduler.run(stop)
        except (honker.LockHeld, honker.LeadershipLost):
            try:
                await asyncio.wait_for(stop.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass


asyncio.run(main())
"""


_WORKER_PROCESS = r"""
import asyncio
import json
import signal
import sys
import time

sys.path.insert(0, {honker_python!r})
sys.path.insert(0, {packages!r})
import honker

db = honker.open({db_path!r})
q = db.queue({queue_name!r})


async def main():
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass

    log = open({log_path!r}, "a", buffering=1)
    it = q.claim({worker_id!r}, idle_poll_s=30.0)
    print("READY", flush=True)

    while not stop.is_set():
        next_job = asyncio.create_task(it.__anext__())
        stop_task = asyncio.create_task(stop.wait())
        done, pending = await asyncio.wait(
            {{next_job, stop_task}},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if stop_task in done:
            next_job.cancel()
            try:
                await next_job
            except BaseException:
                pass
            break

        stop_task.cancel()
        try:
            await stop_task
        except BaseException:
            pass

        job = await next_job
        payload = job.payload
        row = {{
            "claimed_at": time.time(),
            "payload": payload,
            "attempts": job.attempts,
            "job_id": job.id,
        }}
        log.write(json.dumps(row) + "\n")
        log.flush()
        job.ack()

    log.close()


asyncio.run(main())
"""


def _spawn(script: str) -> subprocess.Popen:
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    return subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env=env,
    )


def _wait_for_line(proc: subprocess.Popen, expected: str, timeout: float = 5.0) -> None:
    lines: queue_mod.Queue[str] = queue_mod.Queue()

    def reader():
        if proc.stdout is None:
            return
        line = proc.stdout.readline()
        if line:
            lines.put(line.strip())

    threading.Thread(target=reader, daemon=True).start()
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            out = proc.stdout.read() if proc.stdout else ""
            err = proc.stderr.read() if proc.stderr else ""
            raise AssertionError(
                f"subprocess exited before {expected!r}: "
                f"rc={proc.returncode}, stdout={out!r}, stderr={err!r}"
            )
        try:
            line = lines.get(timeout=0.1)
            if line == expected:
                return
        except queue_mod.Empty:
            pass
    raise AssertionError(f"timed out waiting for {expected!r}")


def _terminate(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2.0)


def _load_jsonl(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def test_time_triggers_e2e_scheduler_and_delayed_job(tmp_path):
    """Second-level schedules and delayed jobs should work together in
    the real deployment shape: registrar/parent, scheduler process, worker
    process.

    Covers the new feature surface end to end:
      * interval schedules via `@every` / `every_s`
      * six-field cron schedules
      * delayed `run_at` jobs waking a sleeping worker
    """
    db_path = str(tmp_path / "timekeeper.db")
    worker_log = str(tmp_path / "worker-log.jsonl")

    db = honker.open(db_path)
    q = db.queue("timed")
    sched = Scheduler(db)
    sched.add(
        name="interval-fast",
        queue="timed",
        schedule=every_s(1),
        payload={"kind": "every"},
    )
    sched.add(
        name="cron-fast",
        queue="timed",
        schedule=crontab("*/2 * * * * *"),
        payload={"kind": "cron"},
    )

    delayed_run_at = int(time.time()) + 4
    q.enqueue({"kind": "delayed"}, run_at=delayed_run_at)

    due_rows = db.query(
        "SELECT name, next_fire_at FROM _honker_scheduler_tasks ORDER BY name"
    )
    first_due = {row["name"]: int(row["next_fire_at"]) for row in due_rows}

    worker_proc = _spawn(
        _WORKER_PROCESS.format(
            honker_python=HONKER_PYTHON_ROOT,
            packages=PACKAGES_ROOT,
            db_path=db_path,
            queue_name="timed",
            worker_id="e2e-worker",
            log_path=worker_log,
        )
    )
    scheduler_proc = _spawn(
        _SCHEDULER_PROCESS.format(
            honker_python=HONKER_PYTHON_ROOT,
            packages=PACKAGES_ROOT,
            db_path=db_path,
        )
    )

    try:
        _wait_for_line(worker_proc, "READY")
        _wait_for_line(scheduler_proc, "READY")
        time.sleep(6.5)
    finally:
        _terminate(scheduler_proc)
        _terminate(worker_proc)

    rows = _load_jsonl(worker_log)
    assert rows, "worker log was empty; no timed work fired"
    last_due = {
        row["name"]: int(row["next_fire_at"])
        for row in db.query("SELECT name, next_fire_at FROM _honker_scheduler_tasks")
    }

    by_kind: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        kind = row["payload"].get("kind")
        if kind is not None:
            by_kind[kind].append(row)

    assert len(by_kind["delayed"]) == 1, (
        f"expected exactly one delayed job, saw {len(by_kind['delayed'])}: {rows}"
    )
    delayed = by_kind["delayed"][0]
    assert delayed["claimed_at"] >= delayed_run_at - 0.05, (
        f"delayed job claimed before run_at: claimed_at={delayed['claimed_at']}, "
        f"run_at={delayed_run_at}"
    )
    assert delayed["claimed_at"] <= delayed_run_at + 3.0, (
        f"delayed job claimed too late: claimed_at={delayed['claimed_at']}, "
        f"run_at={delayed_run_at}"
    )

    every_rows = sorted(by_kind["every"], key=lambda row: row["claimed_at"])
    cron_rows = sorted(by_kind["cron"], key=lambda row: row["claimed_at"])

    # Upper bounds: each boundary the scheduler advanced past fires at
    # most one job. Lower bounds alone let a double fire pass.
    every_boundaries = last_due["interval-fast"] - first_due["interval-fast"]
    cron_boundaries = (last_due["cron-fast"] - first_due["cron-fast"]) // 2
    assert len(every_rows) <= every_boundaries, (
        f"{len(every_rows)} @every jobs for {every_boundaries} boundaries: {rows}"
    )
    assert len(cron_rows) <= cron_boundaries, (
        f"{len(cron_rows)} cron jobs for {cron_boundaries} boundaries: {rows}"
    )
    assert len(every_rows) >= 4, (
        f"expected at least 4 @every fires in ~6.5s, saw {len(every_rows)}: {rows}"
    )
    assert len(cron_rows) >= 2, (
        f"expected at least 2 six-field cron fires in ~6.5s, saw {len(cron_rows)}: {rows}"
    )

    assert every_rows[0]["claimed_at"] >= first_due["interval-fast"] - 0.05
    assert every_rows[0]["claimed_at"] <= first_due["interval-fast"] + 2.0
    assert cron_rows[0]["claimed_at"] >= first_due["cron-fast"] - 0.05
    assert cron_rows[0]["claimed_at"] <= first_due["cron-fast"] + 2.0

    every_deltas = [
        b["claimed_at"] - a["claimed_at"]
        for a, b in zip(every_rows, every_rows[1:])
    ]
    cron_deltas = [
        b["claimed_at"] - a["claimed_at"]
        for a, b in zip(cron_rows, cron_rows[1:])
    ]
    assert all(delta < 2.5 for delta in every_deltas[:3]), (
        f"@every cadence drifted too far: deltas={every_deltas}, rows={every_rows}"
    )
    assert all(delta < 3.5 for delta in cron_deltas[:2]), (
        f"six-field cron cadence drifted too far: deltas={cron_deltas}, rows={cron_rows}"
    )


def test_time_triggers_e2e_reclaim_deadline_wake(tmp_path):
    """A sleeping worker in another process should reclaim a job when the
    original claim expires, without any commit landing at reclaim time.
    """
    db_path = str(tmp_path / "reclaim.db")
    worker_log = str(tmp_path / "reclaim-worker-log.jsonl")

    db = honker.open(db_path)
    q = db.queue("reclaim-q")
    q.enqueue({"kind": "reclaim"})
    first = q.claim_one("owner")
    assert first is not None

    reclaim_at = int(time.time()) + 4
    expected_claimable_at = reclaim_at + 1
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_live SET claim_expires_at = ? WHERE id = ?",
            [reclaim_at, first.id],
        )

    worker_proc = _spawn(
        _WORKER_PROCESS.format(
            honker_python=HONKER_PYTHON_ROOT,
            packages=PACKAGES_ROOT,
            db_path=db_path,
            queue_name="reclaim-q",
            worker_id="rescuer",
            log_path=worker_log,
        )
    )

    try:
        _wait_for_line(worker_proc, "READY")
        time.sleep(6.5)
    finally:
        _terminate(worker_proc)

    rows = _load_jsonl(worker_log)
    assert len(rows) == 1, f"expected exactly one reclaimed job, saw: {rows}"
    row = rows[0]
    assert row["payload"] == {"kind": "reclaim"}
    assert row["job_id"] == first.id
    assert row["attempts"] == 2
    assert row["claimed_at"] >= expected_claimable_at - 0.05, (
        f"reclaimed before expiry window opened: claimed_at={row['claimed_at']}, "
        f"expected_claimable_at={expected_claimable_at}"
    )
    assert row["claimed_at"] <= expected_claimable_at + 3.0, (
        f"reclaim lagged too far past expiry: claimed_at={row['claimed_at']}, "
        f"expected_claimable_at={expected_claimable_at}"
    )


def _beats_fired_and_enqueued(db) -> tuple[int, int]:
    nxt = int(
        db.query(
            "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='beat'"
        )[0]["next_fire_at"]
    )
    jobs = db.query("SELECT COUNT(*) AS c FROM _honker_live WHERE queue='beats'")[0]["c"]
    return nxt, int(jobs)


def _start_standby(db_path: str, lock_ttl: int) -> subprocess.Popen:
    proc = _spawn(
        _STANDBY_SCHEDULER_PROCESS.format(
            honker_python=HONKER_PYTHON_ROOT,
            packages=PACKAGES_ROOT,
            db_path=db_path,
            lock_ttl=lock_ttl,
        )
    )
    _wait_for_line(proc, "READY")
    return proc


def test_two_scheduler_processes_fire_each_boundary_once(tmp_path):
    """C7: two scheduler processes in hot standby on `@every 1s` for 10 s.
    Every boundary the schedule advanced past has exactly one job."""
    db_path = str(tmp_path / "two-schedulers.db")
    db = honker.open(db_path)
    Scheduler(db).add(name="beat", queue="beats", schedule=every_s(1))
    first_due, _ = _beats_fired_and_enqueued(db)

    procs = [_start_standby(db_path, lock_ttl=3) for _ in range(2)]
    try:
        time.sleep(10.0)
    finally:
        for proc in procs:
            _terminate(proc)
    if sys.platform != "win32":
        # terminate() is TerminateProcess on Windows (exit 1), so only
        # POSIX can tell a clean SIGTERM stop from a crash.
        for proc in procs:
            assert proc.returncode == 0, proc.stderr.read() if proc.stderr else ""

    next_due, jobs = _beats_fired_and_enqueued(db)
    fired = next_due - first_due
    assert fired >= 8, f"only {fired} boundaries fired in 10 s"
    assert jobs == fired, f"{jobs} jobs for {fired} boundaries"


@pytest.mark.skipif(sys.platform == "win32", reason="SIGSTOP/SIGCONT are Unix-only")
def test_frozen_scheduler_leader_is_replaced_without_double_fires(tmp_path):
    """C7 with a failover: the leader freezes (SIGSTOP) past its lock
    TTL, the standby takes over and catches up the missed boundaries,
    then the old leader thaws (SIGCONT) and must step down. Each
    boundary still fires exactly once (#181 makes the tick itself
    atomic, so even an overlapping tick cannot double-fire)."""
    import signal

    db_path = str(tmp_path / "failover.db")
    db = honker.open(db_path)
    Scheduler(db).add(name="beat", queue="beats", schedule=every_s(1))
    first_due, _ = _beats_fired_and_enqueued(db)

    leader = _start_standby(db_path, lock_ttl=3)
    standby = None
    try:
        deadline = time.time() + 5
        while _beats_fired_and_enqueued(db)[1] == 0 and time.time() < deadline:
            time.sleep(0.1)
        assert _beats_fired_and_enqueued(db)[1] > 0, "first scheduler never fired"
        standby = _start_standby(db_path, lock_ttl=3)
        time.sleep(2.0)

        os.kill(leader.pid, signal.SIGSTOP)
        _, at_freeze = _beats_fired_and_enqueued(db)
        time.sleep(7.0)
        _, at_thaw = _beats_fired_and_enqueued(db)
        os.kill(leader.pid, signal.SIGCONT)
        time.sleep(3.0)
    finally:
        try:
            os.kill(leader.pid, signal.SIGCONT)
        except OSError:
            pass
        _terminate(leader)
        if standby is not None:
            _terminate(standby)

    assert at_thaw >= at_freeze + 4, (
        f"standby did not take over: {at_freeze} jobs at freeze, {at_thaw} at thaw"
    )
    next_due, jobs = _beats_fired_and_enqueued(db)
    fired = next_due - first_due
    assert jobs == fired, f"{jobs} jobs for {fired} boundaries"
