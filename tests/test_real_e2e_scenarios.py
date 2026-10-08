"""Real user-shaped end-to-end scenarios.

These tests use separate Python processes that share one SQLite file.
That is the deployment shape Honker is trying to make boring: an app
process commits data, sleeping workers/listeners wake, and all state is
in the same database file.
"""

from __future__ import annotations

import json
import os
import queue
import sqlite3
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

import honker


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PACKAGES_ROOT = os.path.join(REPO_ROOT, "packages")
HONKER_PYTHON_ROOT = os.path.join(PACKAGES_ROOT, "honker", "python")
IDLE_POLL_S = 30.0

EXT_CANDIDATES = [
    os.path.join(REPO_ROOT, "target", "release", "libhonker_ext.dylib"),
    os.path.join(REPO_ROOT, "target", "release", "libhonker_ext.so"),
    os.path.join(REPO_ROOT, "target", "release", "honker_ext.dll"),
]
EXT_PATH = next((p for p in EXT_CANDIDATES if os.path.exists(p)), None)
HAS_LOAD_EXTENSION = hasattr(sqlite3.connect(":memory:"), "enable_load_extension")


CHILD = r"""
import asyncio
import json
import os
import signal
import sqlite3
import sys
import time

sys.path.insert(0, {honker_python!r})
sys.path.insert(0, {packages!r})
import honker

IDLE_POLL_S = {idle_poll_s!r}
LEDGER = None


def close_db(db):
    close = getattr(getattr(db, "_inner", None), "close", None)
    if callable(close):
        close()


async def claim_and_ack(db_path, queue, worker):
    db = honker.open(db_path)
    try:
        q = db.queue(queue)
        it = q.claim(worker, idle_poll_s=IDLE_POLL_S)
        print("READY", flush=True)
        async for job in it:
            payload = job.payload
            claimed_at = time.time()
            assert job.ack()
            print("RESULT " + json.dumps({{
                "payload": payload,
                "claimed_at": claimed_at,
                "job_id": job.id,
            }}, sort_keys=True), flush=True)
            return
    finally:
        close_db(db)


def crash_after_claim(db_path, queue, worker):
    db = honker.open(db_path)
    q = db.queue(queue, visibility_timeout_s=2)
    job = q.claim_one(worker)
    assert job is not None
    print("CLAIMED " + json.dumps(job.payload, sort_keys=True), flush=True)
    os._exit(0)


async def retry_claim(db_path, queue, worker, delay_s):
    db = honker.open(db_path)
    try:
        q = db.queue(queue, max_attempts=2)
        it = q.claim(worker, idle_poll_s=IDLE_POLL_S)
        print("READY", flush=True)
        async for job in it:
            assert job.retry(delay_s=int(delay_s), error=f"retry by {{worker}}")
            print("RETRIED " + json.dumps({{
                "attempts": job.attempts,
                "payload": job.payload,
                "at": time.time(),
            }}, sort_keys=True), flush=True)
            return
    finally:
        close_db(db)


async def save_result_worker(db_path, queue):
    db = honker.open(db_path)
    try:
        q = db.queue(queue)
        it = q.claim("result-worker", idle_poll_s=IDLE_POLL_S)
        print("READY", flush=True)
        async for job in it:
            value = {{"sum": job.payload["x"] + job.payload["y"]}}
            q.save_result(job.id, value, ttl=60)
            assert job.ack()
            print("SAVED " + json.dumps(value, sort_keys=True), flush=True)
            return
    finally:
        close_db(db)


async def wait_result(db_path, queue, job_id):
    db = honker.open(db_path)
    try:
        q = db.queue(queue)
        waiter = asyncio.create_task(q.wait_result(int(job_id), timeout=IDLE_POLL_S))
        await asyncio.sleep(0.05)
        print("READY", flush=True)
        value = await waiter
        print("RESULT " + json.dumps(value, sort_keys=True), flush=True)
    finally:
        close_db(db)


def rate_limit_once(db_path, name, limit, per):
    db = honker.open(db_path)
    try:
        ok = db.try_rate_limit(name, limit=int(limit), per=int(per))
        print("RESULT " + json.dumps({{"ok": ok}}), flush=True)
    finally:
        close_db(db)


def lock_holder_crash(db_path, name, ttl):
    db = honker.open(db_path)
    lock = db.lock(name, ttl=int(ttl), owner="holder")
    lock.__enter__()
    print("HELD", flush=True)
    os._exit(0)


async def lock_waiter(db_path, name, ttl):
    db = honker.open(db_path)
    try:
        try:
            with db.lock(name, ttl=int(ttl), owner="waiter"):
                print("UNEXPECTED", flush=True)
                return
        except honker.LockHeld:
            print("BLOCKED", flush=True)
        await asyncio.sleep(int(ttl) + 1.2)
        with db.lock(name, ttl=int(ttl), owner="waiter"):
            print("ACQUIRED", flush=True)
    finally:
        close_db(db)


async def stream_read(db_path, stream_name, consumer, count):
    db = honker.open(db_path)
    try:
        stream = db.stream(stream_name)
        it = stream.subscribe(
            consumer=consumer,
            save_every_n=0,
            save_every_s=0,
        )
        out = []
        for i in range(int(count)):
            next_event = asyncio.create_task(it.__anext__())
            if i == 0:
                await asyncio.sleep(0.1)
                print("READY", flush=True)
            event = await asyncio.wait_for(next_event, timeout=IDLE_POLL_S)
            out.append({{"offset": event.offset, "payload": event.payload}})
            stream.save_offset(consumer, event.offset)
        print("RESULT " + json.dumps(out, sort_keys=True), flush=True)
    finally:
        close_db(db)


async def listen_once(db_path, channel):
    db = honker.open(db_path)
    try:
        it = db.listen(channel)
        next_note = asyncio.create_task(it.__anext__())
        await asyncio.sleep(0.1)
        print("READY", flush=True)
        note = await asyncio.wait_for(next_note, timeout=IDLE_POLL_S)
        print("RESULT " + json.dumps({{
            "payload": note.payload,
            "seen_at": time.time(),
        }}, sort_keys=True), flush=True)
    finally:
        close_db(db)


def raw_sql_enqueue(db_path, ext_path, commit):
    conn = sqlite3.connect(db_path)
    conn.enable_load_extension(True)
    conn.load_extension(ext_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("SELECT honker_bootstrap()")
    conn.execute("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, email TEXT)")
    order_id = 2 if commit == "commit" else 1
    email = "alice@example.com" if commit == "commit" else "rollback@example.com"
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("INSERT INTO orders (id, email) VALUES (?, ?)", (order_id, email))
    conn.execute(
        "SELECT honker_enqueue(?, ?, NULL, NULL, 0, 3, NULL)",
        ("emails", json.dumps({{"to": email, "order_id": order_id}})),
    )
    if commit == "commit":
        conn.commit()
        print("COMMIT", flush=True)
    else:
        conn.rollback()
        print("ROLLBACK", flush=True)
    conn.close()


def ledger_write(path, **row):
    row.update(pid=os.getpid(), t=time.time())
    with open(path, "a") as f:
        f.write(json.dumps(row, sort_keys=True) + "\n")


def e2e_work(key, wait_file=None, sleep_s=0, fail=False, fail_once=False):
    # Body of the decorated task the worker processes run. The parent
    # enqueues through a task registered under the same name.
    ledger_write(LEDGER, event="start", key=key)
    marker = f"{{LEDGER}}.{{key}}.failed"
    if fail_once and not os.path.exists(marker):
        open(marker, "w").close()
        ledger_write(LEDGER, event="end", key=key)
        raise RuntimeError("first attempt fails on purpose")
    deadline = time.time() + 30
    while wait_file and not os.path.exists(wait_file) and time.time() < deadline:
        time.sleep(0.02)
    if sleep_s:
        time.sleep(sleep_s)
    ledger_write(LEDGER, event="end", key=key)
    if fail:
        raise RuntimeError("handler failed on purpose")
    return key


async def task_worker(db_path, cfg_json):
    # The public worker loop: decorated task + db.run_workers().
    global LEDGER
    cfg = json.loads(cfg_json)
    LEDGER = cfg["ledger"]
    db = honker.open(db_path)
    retries = cfg.get("retries", 3)
    q = db.queue(
        cfg["queue"],
        visibility_timeout_s=cfg.get("visibility", 300),
        max_attempts=retries,
    )
    q.task(
        name="e2e.work",
        retries=retries,
        retry_delay_s=cfg.get("retry_delay_s", 0),
        store_result=False,
    )(e2e_work)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass

    async def report_cpu(idle_s):
        await asyncio.sleep(1.0)
        start = time.process_time()
        await asyncio.sleep(idle_s)
        print("CPU " + json.dumps(time.process_time() - start), flush=True)

    if cfg.get("report_cpu_after"):
        cpu_task = asyncio.create_task(report_cpu(cfg["report_cpu_after"]))
    print("READY", flush=True)
    await db.run_workers(
        queue=cfg["queue"],
        concurrency=cfg.get("concurrency", 1),
        stop_event=stop,
    )
    close_db(db)


def lifecycle_call(job, op):
    if op == "ack":
        return job.ack()
    if op == "retry":
        return job.retry(delay_s=0, error="stale retry")
    if op == "fail":
        return job.fail(error="stale fail")
    if op == "heartbeat":
        return job.heartbeat(60)
    raise SystemExit(f"unknown op: {{op}}")


async def stale_owner(db_path, queue, worker, op):
    # A user-written loop over q.claim(). It claims one job with a 1 s
    # lease, stalls until the parent says go (by then the lease lapsed
    # and another process owns the job), then makes one lifecycle call.
    # The loop must then keep working.
    db = honker.open(db_path)
    try:
        q = db.queue(queue, visibility_timeout_s=1)
        it = q.claim(worker, idle_poll_s=IDLE_POLL_S)
        print("READY", flush=True)
        job = await it.__anext__()
        print("CLAIMED " + json.dumps(dict(id=job.id, attempts=job.attempts)), flush=True)
        sys.stdin.readline()
        print("OP " + json.dumps(lifecycle_call(job, op)), flush=True)
        job = await it.__anext__()
        print("NEXT " + json.dumps(dict(id=job.id, ack=job.ack())), flush=True)
    finally:
        close_db(db)


def hold_claim(db_path, queue, worker):
    # Claims one job and holds it until the parent says go, then acks.
    db = honker.open(db_path)
    try:
        q = db.queue(queue, visibility_timeout_s=60)
        job = None
        deadline = time.time() + 10
        while job is None and time.time() < deadline:
            job = q.claim_one(worker)
            if job is None:
                time.sleep(0.05)
        assert job is not None, "nothing to claim"
        print("CLAIMED " + json.dumps(dict(id=job.id, attempts=job.attempts)), flush=True)
        sys.stdin.readline()
        print("ACK " + json.dumps(job.ack()), flush=True)
    finally:
        close_db(db)


async def heartbeat_claim(db_path, queue, worker, run_s, heartbeat):
    # A long handler on a 2 s lease, with or without manual heartbeats
    # every 0.5 s through the public Job.heartbeat().
    db = honker.open(db_path)
    try:
        q = db.queue(queue, visibility_timeout_s=2)
        it = q.claim(worker, idle_poll_s=IDLE_POLL_S)
        job = await it.__anext__()
        print("CLAIMED " + json.dumps(dict(id=job.id, attempts=job.attempts)), flush=True)
        beats = []
        end = time.time() + float(run_s)
        while time.time() < end:
            await asyncio.sleep(0.5)
            if heartbeat == "on":
                beats.append(job.heartbeat(2))
        print("DONE " + json.dumps(dict(beats=beats, ack=job.ack())), flush=True)
    finally:
        close_db(db)


def poll_claim(db_path, queue, worker, seconds):
    # An intruder that keeps trying to claim for `seconds`.
    db = honker.open(db_path)
    try:
        q = db.queue(queue, visibility_timeout_s=2)
        stolen = []
        end = time.time() + float(seconds)
        while time.time() < end:
            job = q.claim_one(worker)
            if job is not None:
                stolen.append(dict(id=job.id, attempts=job.attempts))
                job.ack()
            time.sleep(0.1)
        print("RESULT " + json.dumps(stolen), flush=True)
    finally:
        close_db(db)


async def main():
    role = sys.argv[1]
    if role == "task-worker":
        await task_worker(sys.argv[2], sys.argv[3])
    elif role == "stale-owner":
        await stale_owner(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    elif role == "hold-claim":
        hold_claim(sys.argv[2], sys.argv[3], sys.argv[4])
    elif role == "heartbeat-claim":
        await heartbeat_claim(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
    elif role == "poll-claim":
        poll_claim(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    elif role == "claim":
        await claim_and_ack(sys.argv[2], sys.argv[3], sys.argv[4])
    elif role == "crash-after-claim":
        crash_after_claim(sys.argv[2], sys.argv[3], sys.argv[4])
    elif role == "retry-claim":
        await retry_claim(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    elif role == "save-result-worker":
        await save_result_worker(sys.argv[2], sys.argv[3])
    elif role == "wait-result":
        await wait_result(sys.argv[2], sys.argv[3], sys.argv[4])
    elif role == "rate-limit":
        rate_limit_once(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    elif role == "lock-holder-crash":
        lock_holder_crash(sys.argv[2], sys.argv[3], sys.argv[4])
    elif role == "lock-waiter":
        await lock_waiter(sys.argv[2], sys.argv[3], sys.argv[4])
    elif role == "stream-read":
        await stream_read(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    elif role == "listen-once":
        await listen_once(sys.argv[2], sys.argv[3])
    elif role == "raw-sql-enqueue":
        raw_sql_enqueue(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        raise SystemExit(f"unknown role: {{role}}")


asyncio.run(main())
""".format(
    honker_python=HONKER_PYTHON_ROOT,
    packages=PACKAGES_ROOT,
    idle_poll_s=IDLE_POLL_S,
)


def _spawn(*args: str, stdin=None) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-c", CHILD, *args],
        stdin=stdin,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _run(*args: str, timeout: float = 20.0) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", CHILD, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _line(proc: subprocess.Popen, timeout: float = 12.0) -> str:
    assert proc.stdout is not None
    lines: queue.Queue[str | None] = queue.Queue(maxsize=1)

    def reader() -> None:
        line = proc.stdout.readline()
        lines.put(line.strip() if line else None)

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    try:
        line = lines.get(timeout=timeout)
        if line is not None:
            return line
    except queue.Empty:
        pass

    stderr = "<still running>"
    if proc.poll() is not None and proc.stderr is not None:
        stderr = proc.stderr.read()
    raise AssertionError(f"child produced no line; rc={proc.poll()} stderr={stderr}")


def _json_line(line: str, prefix: str = "RESULT ") -> dict | list:
    assert line.startswith(prefix), line
    return json.loads(line.removeprefix(prefix))


def _finish(proc: subprocess.Popen, timeout: float = 5.0) -> None:
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5.0)
    assert proc.returncode == 0, (
        f"child exited {proc.returncode}; stderr="
        f"{proc.stderr.read() if proc.stderr else ''}"
    )


def _kill(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    proc.kill()
    try:
        proc.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        proc.wait(timeout=1.0)


def test_delayed_job_wakes_sleeping_worker_process(db_path):
    db = honker.open(db_path)
    q = db.queue("delayed")
    worker = _spawn("claim", db_path, "delayed", "delayed-worker")
    try:
        assert _line(worker) == "READY"
        run_at = int(time.time()) + 3
        q.enqueue({"kind": "delayed"}, run_at=run_at)

        got = _json_line(_line(worker, timeout=8.0))
        assert got["payload"] == {"kind": "delayed"}
        assert got["claimed_at"] >= run_at - 0.05
        assert got["claimed_at"] <= run_at + 3.0
        _finish(worker)
    finally:
        _kill(worker)


def test_crashed_worker_claim_is_reclaimed_by_sleeping_worker(db_path):
    db = honker.open(db_path)
    q = db.queue("reclaim", visibility_timeout_s=2)
    q.enqueue({"kind": "recover"})

    crashed = _spawn("crash-after-claim", db_path, "reclaim", "crashy")
    assert _json_line(_line(crashed), prefix="CLAIMED ") == {"kind": "recover"}
    crashed.wait(timeout=5.0)
    assert crashed.returncode == 0
    rows = db.query(
        "SELECT claim_expires_at FROM _honker_jobs WHERE queue='reclaim'"
    )
    claim_expires_at = int(rows[0]["claim_expires_at"])

    rescuer = _spawn("claim", db_path, "reclaim", "rescuer")
    try:
        assert _line(rescuer) == "READY"
        got = _json_line(_line(rescuer, timeout=8.0))
        assert got["payload"] == {"kind": "recover"}
        assert got["claimed_at"] >= claim_expires_at - 0.05
        assert got["claimed_at"] <= claim_expires_at + 3.0
        _finish(rescuer)
    finally:
        _kill(rescuer)


def test_retry_backoff_wakes_then_exhausts_to_dead(db_path):
    db = honker.open(db_path)
    q = db.queue("retry", max_attempts=2)
    q.enqueue({"kind": "retry"})

    first = _spawn("retry-claim", db_path, "retry", "retry-a", "2")
    try:
        assert _line(first) == "READY"
        first_retry = _json_line(_line(first), prefix="RETRIED ")
        assert first_retry["payload"] == {"kind": "retry"}
        _finish(first)
    finally:
        _kill(first)
    retry_due_at = int(
        db.query("SELECT run_at FROM _honker_jobs WHERE queue='retry'")[0]["run_at"]
    )

    second = _spawn("retry-claim", db_path, "retry", "retry-b", "0")
    try:
        assert _line(second) == "READY"
        second_retry = _json_line(_line(second, timeout=8.0), prefix="RETRIED ")
        assert second_retry["payload"] == {"kind": "retry"}
        assert second_retry["at"] >= retry_due_at - 0.05
        assert second_retry["at"] <= retry_due_at + 3.0
        _finish(second)
    finally:
        _kill(second)

    rows = db.query(
        "SELECT state, attempts, last_error FROM _honker_jobs WHERE queue='retry'"
    )
    assert rows == [
        {
            "state": "dead",
            "attempts": 2,
            "last_error": "retry by retry-b",
        }
    ]


async def test_wait_result_wakes_when_worker_process_saves_result(db_path):
    db = honker.open(db_path)
    q = db.queue("results")
    job_id = q.enqueue({"x": 2, "y": 5})

    waiter = _spawn("wait-result", db_path, "results", str(job_id))
    try:
        assert _line(waiter) == "READY"
        worker = _spawn("save-result-worker", db_path, "results")
        try:
            assert _line(worker) == "READY"
            assert _json_line(_line(worker), prefix="SAVED ") == {"sum": 7}
            _finish(worker)
        finally:
            _kill(worker)

        assert _json_line(_line(waiter)) == {"sum": 7}
        _finish(waiter)
    finally:
        _kill(waiter)


def test_rate_limit_is_shared_across_processes(db_path):
    honker.open(db_path).try_rate_limit("warmup", limit=1, per=60)

    def run_one(_i: int) -> bool:
        res = _run("rate-limit", db_path, "api", "3", "60")
        assert res.returncode == 0, res.stderr
        return bool(_json_line(res.stdout.strip())["ok"])

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(run_one, range(8)))

    assert results.count(True) == 3
    assert results.count(False) == 5


def test_named_lock_blocks_cross_process_until_crashed_holder_ttl(db_path):
    holder = _spawn("lock-holder-crash", db_path, "singleton", "2")
    assert _line(holder) == "HELD"
    holder.wait(timeout=5.0)
    assert holder.returncode == 0

    waiter = _spawn("lock-waiter", db_path, "singleton", "2")
    try:
        assert _line(waiter) == "BLOCKED"
        assert _line(waiter, timeout=6.0) == "ACQUIRED"
        _finish(waiter)
    finally:
        _kill(waiter)


def test_stream_consumer_replays_then_resumes_after_saved_offset(db_path):
    db = honker.open(db_path)
    stream = db.stream("orders")
    stream.publish({"n": 1})
    stream.publish({"n": 2})

    first = _spawn("stream-read", db_path, "orders", "dashboard", "1")
    try:
        assert _line(first) == "READY"
        got_first = _json_line(_line(first))
        assert [row["payload"] for row in got_first] == [{"n": 1}]
        _finish(first)
    finally:
        _kill(first)

    second = _spawn("stream-read", db_path, "orders", "dashboard", "1")
    try:
        assert _line(second) == "READY"
        got_second = _json_line(_line(second))
        assert [row["payload"] for row in got_second] == [{"n": 2}]
        _finish(second)
    finally:
        _kill(second)


def test_live_stream_subscriber_wakes_on_new_event(db_path):
    db = honker.open(db_path)
    stream = db.stream("live-orders")

    reader = _spawn("stream-read", db_path, "live-orders", "dashboard-live", "1")
    try:
        assert _line(reader) == "READY"
        published_at = time.time()
        stream.publish({"n": 1, "kind": "created"})

        got = _json_line(_line(reader, timeout=8.0))
        assert got == [
            {"offset": 1, "payload": {"n": 1, "kind": "created"}},
        ]
        assert time.time() - published_at < 5.0
        _finish(reader)
    finally:
        _kill(reader)


def test_live_notification_listener_wakes_on_new_notify(db_path):
    db = honker.open(db_path)

    listener = _spawn("listen-once", db_path, "orders")
    try:
        assert _line(listener) == "READY"
        with db.transaction() as tx:
            tx.notify("orders", {"order_id": 1, "kind": "created"})

        got = _json_line(_line(listener, timeout=8.0))
        assert got["payload"] == {"order_id": 1, "kind": "created"}
        _finish(listener)
    finally:
        _kill(listener)


@pytest.mark.skipif(
    EXT_PATH is None or not HAS_LOAD_EXTENSION,
    reason="loadable extension is unavailable in this Python/sqlite build",
)
def test_raw_sql_transaction_enqueue_wakes_python_worker(db_path):
    worker = _spawn("claim", db_path, "emails", "python-worker")
    try:
        assert _line(worker) == "READY"

        rolled_back = _run(
            "raw-sql-enqueue",
            db_path,
            EXT_PATH,
            "rollback",
        )
        assert rolled_back.returncode == 0, rolled_back.stderr
        assert rolled_back.stdout.strip() == "ROLLBACK"

        committed = _run(
            "raw-sql-enqueue",
            db_path,
            EXT_PATH,
            "commit",
        )
        assert committed.returncode == 0, committed.stderr
        assert committed.stdout.strip() == "COMMIT"

        got = _json_line(_line(worker, timeout=8.0))
        assert got["payload"] == {"order_id": 2, "to": "alice@example.com"}
        _finish(worker)

        db = honker.open(db_path)
        rows = db.query("SELECT COUNT(*) AS c FROM orders")
        assert rows[0]["c"] == 1
    finally:
        _kill(worker)


# ---------------------------------------------------------------------
# Worker-loop contract through the public worker API (C1-C9).
#
# Each scenario runs the worker in its own OS process on a WAL file
# with the real clock. The worker is either `db.run_workers()` with a
# decorated task (the built-in loop) or a user loop over `q.claim()`.
# Where the binding breaks the contract today the test is a strict
# xfail that names the binding PR or issue expected to flip it.
# ---------------------------------------------------------------------

PY_BINDING_PR = "#185 (Python worker-loop contract, planned for PR #143)"


def _e2e_work(key, wait_file=None, sleep_s=0, fail=False, fail_once=False):
    raise AssertionError("e2e.work runs in the worker process")


def _work_queue(db, name, *, visibility=300, retries=3, retry_delay_s=0):
    """Queue plus the producer side of the `e2e.work` task the
    `task-worker` child registers under the same name."""
    q = db.queue(name, visibility_timeout_s=visibility, max_attempts=retries)
    task = q.task(
        name="e2e.work",
        retries=retries,
        retry_delay_s=retry_delay_s,
        store_result=False,
    )(_e2e_work)
    return q, task


def _start_task_worker(db_path: str, **cfg) -> tuple[subprocess.Popen, str]:
    ledger = os.path.join(os.path.dirname(db_path), f"ledger-{cfg['queue']}.jsonl")
    cfg.setdefault("ledger", ledger)
    proc = _spawn("task-worker", db_path, json.dumps(cfg))
    assert _line(proc) == "READY"
    return proc, cfg["ledger"]


def _ledger_rows(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def _events(path: str, event: str, key: str) -> list[dict]:
    return [r for r in _ledger_rows(path) if r["event"] == event and r["key"] == key]


def _wait_until(predicate, timeout: float, interval: float = 0.05) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return bool(predicate())


def _job_row(db, job_id: int) -> dict | None:
    rows = db.query(
        "SELECT state, worker_id, attempts, last_error FROM _honker_jobs WHERE id=?",
        [job_id],
    )
    return rows[0] if rows else None


def _stop_worker(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.terminate()
    try:
        proc.wait(timeout=10.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5.0)


class _WriteLock:
    """Another OS connection holds BEGIN EXCLUSIVE for `seconds`, the
    way a long migration or a stuck admin session would."""

    def __init__(self, db_path: str, seconds: float):
        self.db_path = db_path
        self.seconds = seconds
        self.held = threading.Event()
        self.released = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        conn = sqlite3.connect(self.db_path, timeout=10.0, isolation_level=None)
        try:
            conn.execute("BEGIN EXCLUSIVE")
            self.held.set()
            time.sleep(self.seconds)
            conn.execute("ROLLBACK")
        finally:
            conn.close()
            self.released.set()

    def start(self) -> "_WriteLock":
        self.thread.start()
        assert self.held.wait(10.0), "could not take the write lock"
        return self

    def join(self) -> None:
        self.thread.join(timeout=self.seconds + 10.0)


def _stale_owner_scenario(db_path: str, op: str, new_worker: str) -> None:
    db = honker.open(db_path)
    q = db.queue("c1", visibility_timeout_s=1)
    job_id = q.enqueue({"n": 1})

    stale = _spawn("stale-owner", db_path, "c1", "worker-a", op, stdin=subprocess.PIPE)
    new_owner = None
    try:
        assert _line(stale) == "READY"
        claimed = _json_line(_line(stale), prefix="CLAIMED ")
        assert claimed == {"id": job_id, "attempts": 1}
        expires = int(_job_row_with_lease(db, job_id)["claim_expires_at"])
        # The lease is lapsed once unixepoch() > claim_expires_at.
        time.sleep(max(0.0, expires + 1.05 - time.time()))

        new_owner = _spawn("hold-claim", db_path, "c1", new_worker, stdin=subprocess.PIPE)
        assert _json_line(_line(new_owner), prefix="CLAIMED ") == {
            "id": job_id,
            "attempts": 2,
        }

        stale.stdin.write("go\n")
        stale.stdin.flush()
        result = _json_line(_line(stale), prefix="OP ")
        row = _job_row(db, job_id)
        dead = db.query("SELECT COUNT(*) AS c FROM _honker_dead WHERE queue='c1'")[0]["c"]
        assert result is False, f"stale {op} acted on attempt 2: row now {row}"
        assert row == {
            "state": "processing",
            "worker_id": new_worker,
            "attempts": 2,
            "last_error": None,
        }
        assert dead == 0

        new_owner.stdin.write("go\n")
        new_owner.stdin.flush()
        assert _json_line(_line(new_owner), prefix="ACK ") is True
        _finish(new_owner)

        # The stale owner's loop keeps working.
        next_id = q.enqueue({"n": 2})
        assert _json_line(_line(stale), prefix="NEXT ") == {"id": next_id, "ack": True}
        _finish(stale)
    finally:
        _kill(stale)
        if new_owner is not None:
            _kill(new_owner)


def _job_row_with_lease(db, job_id: int) -> dict:
    return db.query(
        "SELECT claim_expires_at FROM _honker_live WHERE id=?", [job_id]
    )[0]


@pytest.mark.parametrize("op", ["ack", "retry", "fail", "heartbeat"])
def test_c1_stale_owner_cannot_touch_job_reclaimed_by_another_worker(db_path, op):
    """C1, different worker id. #165 keeps retry's ownership check and
    its write in one statement; ack/fail/heartbeat check the lease."""
    _stale_owner_scenario(db_path, op, new_worker="worker-b")


@pytest.mark.xfail(
    strict=True,
    reason=(
        "#176: the binding still calls the unfenced honker_ack/retry/fail/"
        "heartbeat, so a stale handler with the same worker id acts on the "
        f"new attempt. Fenced arities exist since #179; {PY_BINDING_PR}."
    ),
)
@pytest.mark.parametrize("op", ["ack", "retry", "fail", "heartbeat"])
def test_c1_stale_owner_with_same_worker_id_cannot_touch_new_attempt(db_path, op):
    """C1, same worker id: a restarted process or a second replica that
    reuses a fixed id such as the README's "worker-1"."""
    _stale_owner_scenario(db_path, op, new_worker="worker-a")


@pytest.mark.parametrize("handler", ["returns", "raises"])
def test_c2_cancel_in_flight_is_dropped_and_worker_continues(db_path, handler):
    """C2: the operator cancels a job while run_workers is running it.
    The late ack (or retry, if the handler raises) returns False, the
    job is not retried or dead-lettered, and the loop keeps going."""
    db = honker.open(db_path)
    q, work = _work_queue(db, "c2")
    go = os.path.join(os.path.dirname(db_path), "go")
    worker, ledger = _start_task_worker(db_path, queue="c2")
    try:
        cancelled = work("cancelled", wait_file=go, fail=handler == "raises")
        assert _wait_until(lambda: _events(ledger, "start", "cancelled"), 10.0)
        assert q.cancel(cancelled.id) is True
        open(go, "w").close()
        assert _wait_until(lambda: _events(ledger, "end", "cancelled"), 10.0)

        after = work("after")
        assert _wait_until(lambda: _job_row(db, after.id) is None, 10.0), _ledger_rows(ledger)
        time.sleep(1.0)
        assert len(_events(ledger, "start", "cancelled")) == 1
        assert _job_row(db, cancelled.id) is None
        assert db.query("SELECT COUNT(*) AS c FROM _honker_dead")[0]["c"] == 0
        assert worker.poll() is None
    finally:
        _stop_worker(worker)


def _locked_mid_run(db_path: str, lock_s: float) -> None:
    """The worker's first attempt fails and is retried with a 2 s
    delay. While that retry is scheduled another connection takes the
    write lock, so the sleeping worker wakes and claims into the lock.

    run_at has one-second resolution, so a 2 s delay leaves the job
    'scheduled' for between 1 and 2 s; a 1 s delay can leave almost
    no window. The lock (3 s or more) always covers the due time."""
    db = honker.open(db_path)
    _q, work = _work_queue(db, "c3")
    worker, ledger = _start_task_worker(db_path, queue="c3", retry_delay_s=2)
    try:
        flaky = work("flaky", fail_once=True)
        assert _wait_until(
            lambda: (_job_row(db, flaky.id) or {}).get("state") == "scheduled", 10.0
        ), _ledger_rows(ledger)
        lock = _WriteLock(db_path, lock_s).start()
        lock.join()
        after = work("after")
        assert _wait_until(
            lambda: _job_row(db, flaky.id) is None and _job_row(db, after.id) is None,
            10.0,
        ), f"worker did not drain after the lock: {_ledger_rows(ledger)}"
        assert len(_events(ledger, "start", "flaky")) == 2
        assert len(_events(ledger, "start", "after")) == 1
        assert worker.poll() is None
    finally:
        _stop_worker(worker)


def test_c3_worker_waits_out_a_3s_write_lock_then_drains(db_path):
    """C3: a 3 s lock is shorter than the binding's 5 s busy_timeout,
    so the claim waits instead of failing."""
    _locked_mid_run(db_path, 3.0)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Worker-loop contract 3: a claim that fails with 'database is "
        "locked' ends the run_workers drain loop for good (the error is "
        f"discarded at shutdown). {PY_BINDING_PR}."
    ),
)
def test_c3_worker_survives_a_write_lock_longer_than_busy_timeout(db_path):
    """C3 variant: the lock outlasts busy_timeout (5 s), so the claim
    raises. The loop must back off and keep going."""
    _locked_mid_run(db_path, 7.0)


@pytest.mark.skip(
    reason=(
        "C4 with auto-heartbeat: run_workers has no auto-heartbeat option "
        "and a decorated task cannot reach its Job to heartbeat. Contract "
        f"item 5, {PY_BINDING_PR}. The manual heartbeat path is covered by "
        "test_c4_manual_heartbeat_keeps_a_long_handler_single_owner."
    )
)
def test_c4_auto_heartbeat_runs_a_long_handler_once():
    pass


def test_c4_without_heartbeat_a_handler_twice_the_lease_runs_twice(db_path):
    """C4, heartbeat off: the documented double run. With a 2 s lease
    and a 5 s handler the second drain loop reclaims the job while the
    first run is still going. Neither late ack lands, and the job is
    dead-lettered when the second lease lapses on its last attempt,
    although both runs succeeded."""
    db = honker.open(db_path)
    _q, work = _work_queue(db, "c4", visibility=2, retries=2)
    worker, ledger = _start_task_worker(
        db_path, queue="c4", visibility=2, retries=2, concurrency=2
    )
    try:
        long = work("long", sleep_s=5)
        assert _wait_until(
            lambda: (_job_row(db, long.id) or {}).get("state") == "dead", 20.0
        ), _ledger_rows(ledger)
        assert _wait_until(lambda: len(_events(ledger, "end", "long")) == 2, 10.0)
        assert len(_events(ledger, "start", "long")) == 2
        assert _job_row(db, long.id) == {
            "state": "dead",
            "worker_id": None,
            "attempts": 2,
            "last_error": "max attempts exceeded",
        }
        assert worker.poll() is None
    finally:
        _stop_worker(worker)


@pytest.mark.parametrize("heartbeat", ["on", "off"])
def test_c4_manual_heartbeat_keeps_a_long_handler_single_owner(db_path, heartbeat):
    """A 5 s handler on a 2 s lease while an intruder process polls the
    queue. With Job.heartbeat() every 0.5 s nobody steals it and the ack
    lands; without heartbeats the intruder reclaims it (the control that
    makes the positive case meaningful)."""
    db = honker.open(db_path)
    q = db.queue("c4m", visibility_timeout_s=2)
    job_id = q.enqueue({"n": 1})
    owner = _spawn("heartbeat-claim", db_path, "c4m", "owner", "5", heartbeat)
    intruder = None
    try:
        assert _json_line(_line(owner), prefix="CLAIMED ") == {"id": job_id, "attempts": 1}
        intruder = _spawn("poll-claim", db_path, "c4m", "intruder", "6")
        done = _json_line(_line(owner, timeout=15.0), prefix="DONE ")
        stolen = _json_line(_line(intruder, timeout=15.0))
        if heartbeat == "on":
            assert done["beats"] and all(done["beats"]), done
            assert done["ack"] is True
            assert stolen == []
        else:
            assert done["ack"] is False
            assert stolen == [{"id": job_id, "attempts": 2}]
        _finish(owner)
        _finish(intruder)
    finally:
        _kill(owner)
        if intruder is not None:
            _kill(intruder)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Worker-loop contract 2: _worker.run_task acks inside the handler's "
        "try, so an ack that fails with 'database is locked' after the "
        f"handler succeeded retries the job and runs it again. {PY_BINDING_PR}."
    ),
)
def test_c5_ack_failure_after_success_does_not_rerun_the_handler(db_path):
    """C5: the handler finishes while another connection holds the
    write lock for longer than busy_timeout, so the ack raises. The
    handler already succeeded and must not run again."""
    db = honker.open(db_path)
    _q, work = _work_queue(db, "c5")
    go = os.path.join(os.path.dirname(db_path), "go")
    worker, ledger = _start_task_worker(db_path, queue="c5")
    try:
        job = work("once", wait_file=go)
        assert _wait_until(lambda: _events(ledger, "start", "once"), 10.0)
        lock = _WriteLock(db_path, 6.5).start()
        open(go, "w").close()
        lock.join()
        time.sleep(3.0)
        assert len(_events(ledger, "start", "once")) == 1, _ledger_rows(ledger)
        assert worker.poll() is None
        del job
    finally:
        _stop_worker(worker)


def test_c6_idle_workers_use_almost_no_cpu(db_path):
    """C6: four idle drain loops for 5 s. They block on the update
    watcher and the next deadline, so CPU stays near zero."""
    db = honker.open(db_path)
    _work_queue(db, "c6")
    worker, _ledger = _start_task_worker(
        db_path, queue="c6", concurrency=4, report_cpu_after=5
    )
    try:
        cpu_s = _json_line(_line(worker, timeout=15.0), prefix="CPU ")
        assert cpu_s < 0.5, f"idle workers used {cpu_s:.3f} s CPU in 5 s"
    finally:
        _stop_worker(worker)


def test_c8_rolled_back_enqueue_never_reaches_a_worker(db_path):
    """C8 through the Python API: an app transaction that rolls back
    leaves no job, and a payload json.dumps rejects raises without
    breaking the surrounding transaction. #153 (core JSON validation)
    is not merged; Python cannot send non-JSON through enqueue anyway."""
    db = honker.open(db_path)
    q = db.queue("emails")
    with db.transaction() as tx:
        tx.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, email TEXT)")

    worker = _spawn("claim", db_path, "emails", "python-worker")
    try:
        assert _line(worker) == "READY"
        with pytest.raises(RuntimeError, match="app error"):
            with db.transaction() as tx:
                tx.execute("INSERT INTO orders VALUES (1, 'rollback@example.com')")
                q.enqueue({"order_id": 1}, tx=tx)
                raise RuntimeError("app error")

        with db.transaction() as tx:
            tx.execute("INSERT INTO orders VALUES (2, 'alice@example.com')")
            with pytest.raises(TypeError):
                q.enqueue({"order_id": 2, "bad": object()}, tx=tx)
            q.enqueue({"order_id": 2}, tx=tx)

        got = _json_line(_line(worker, timeout=8.0))
        assert got["payload"] == {"order_id": 2}
        _finish(worker)
        assert db.query("SELECT id FROM orders") == [{"id": 2}]
        assert db.query("SELECT COUNT(*) AS c FROM _honker_jobs")[0]["c"] == 0
    finally:
        _kill(worker)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Worker-loop contract 7: _drain_loop decodes job.payload outside "
        "any try, so one non-JSON row (legacy, or raw SQL before #153) "
        "raises JSONDecodeError and ends the drain loop; the good jobs "
        f"behind it never run. {PY_BINDING_PR}; read side of #152."
    ),
)
def test_c9_one_bad_payload_fails_alone(db_path):
    """C9: a row whose payload is not JSON sits at the head of the queue.
    Only that job may fail; the jobs behind it still run.

    The row is written straight into the table, as a legacy row from
    before #153 would be, so the scenario still applies once core
    rejects non-JSON at enqueue (#152)."""
    db = honker.open(db_path)
    _q, work = _work_queue(db, "c9")
    with db.transaction() as tx:
        tx.execute(
            "INSERT INTO _honker_live (queue, payload, priority) "
            "VALUES ('c9', 'not json', 10)"
        )
    good = [work(f"good-{i}") for i in range(2)]
    worker, ledger = _start_task_worker(db_path, queue="c9")
    try:
        assert _wait_until(
            lambda: all(_job_row(db, g.id) is None for g in good), 10.0
        ), _ledger_rows(ledger)
        dead = db.query("SELECT payload FROM _honker_dead WHERE queue='c9'")
        assert dead == [{"payload": "not json"}]
        assert worker.poll() is None
    finally:
        _stop_worker(worker)


def test_expired_job_held_by_a_killed_worker_never_runs_again(db_path):
    """#177 / #180: a worker claims a job with `expires=3` and dies. When
    its lease lapses after the job's expiry, the next claim moves the
    job to dead ('expired') instead of handing it to another worker."""
    db = honker.open(db_path)
    q = db.queue("expiring", visibility_timeout_s=1)
    job_id = q.enqueue({"kind": "expiring"}, expires=3)
    keeper = q.enqueue({"kind": "keeper"}, delay=5)

    crashed = _spawn("crash-after-claim", db_path, "expiring", "crashy")
    assert _json_line(_line(crashed), prefix="CLAIMED ") == {"kind": "expiring"}
    crashed.wait(timeout=5.0)

    rescuer = _spawn("claim", db_path, "expiring", "rescuer")
    try:
        assert _line(rescuer) == "READY"
        # The only job this worker may ever see is the delayed keeper.
        got = _json_line(_line(rescuer, timeout=12.0))
        assert got["payload"] == {"kind": "keeper"}
        assert got["job_id"] == keeper
        _finish(rescuer)
    finally:
        _kill(rescuer)
    rows = db.query(
        "SELECT state, attempts, last_error FROM _honker_jobs WHERE id=?", [job_id]
    )
    assert rows == [{"state": "dead", "attempts": 1, "last_error": "expired"}]
