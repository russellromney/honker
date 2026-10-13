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
import sqlite3
import sys
import time

sys.path.insert(0, {honker_python!r})
sys.path.insert(0, {packages!r})
import honker

IDLE_POLL_S = {idle_poll_s!r}


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


async def scheduler_run(db_path, seconds):
    # Hot standby the way the Scheduler docstring says: run() raises
    # LockHeld while another process leads, so retry until stop.
    db = honker.open(db_path)
    try:
        scheduler = honker.Scheduler(db)
        stop = asyncio.Event()
        asyncio.get_running_loop().call_later(float(seconds), stop.set)
        print("READY", flush=True)
        while not stop.is_set():
            try:
                await scheduler.run(stop)
            except honker.LockHeld:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=0.2)
                except asyncio.TimeoutError:
                    pass
        print("STOPPED", flush=True)
    finally:
        close_db(db)


async def main():
    role = sys.argv[1]
    if role == "claim":
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
    elif role == "scheduler-run":
        await scheduler_run(sys.argv[2], sys.argv[3])
    else:
        raise SystemExit(f"unknown role: {{role}}")


asyncio.run(main())
""".format(
    honker_python=HONKER_PYTHON_ROOT,
    packages=PACKAGES_ROOT,
    idle_poll_s=IDLE_POLL_S,
)


def _spawn(*args: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-c", CHILD, *args],
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
# JSON payload contract (#152), from the user's side: what an app sees
# when it hands honker text that is not JSON. Python's typed APIs run
# json.dumps, which still emits two things honker rejects: NaN/Infinity
# (not JSON) and lone surrogates (JSON, but undecodable).
# ---------------------------------------------------------------------

NOT_JSON = "payload must be valid JSON"
UNDECODABLE = "payload is valid JSON but cannot be decoded"
RAW_ENQUEUE = "SELECT honker_enqueue(?, ?, NULL, NULL, 0, 3, NULL)"


class _HonkerTxApp:
    """The app writes through honker's own transaction, enqueueing with
    raw SQL inside it (the same function call an ORM user makes)."""

    def __init__(self, db_path):
        self.db = honker.open(db_path)
        self.queue = self.db.queue("emails")
        with self.db.transaction() as tx:
            tx.execute(
                "CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, email TEXT)"
            )

    def run(self, body, commit):
        class Rollback(Exception):
            pass

        try:
            with self.db.transaction() as tx:
                body(
                    lambda sql, args: tx.query(sql, list(args)),
                    lambda payload: self.queue.enqueue(payload, tx=tx),
                )
                if not commit:
                    raise Rollback()
        except Rollback:
            pass

    def query(self, sql):
        return [tuple(r.values()) for r in self.db.query(sql)]


class _OrmConnectionApp:
    """The app owns a plain sqlite3 connection with the extension loaded,
    the way the ORM guide wires SQLAlchemy/Django, and runs its own
    BEGIN/COMMIT/ROLLBACK."""

    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path, isolation_level=None)
        honker.load_extension(self.conn)
        self.conn.execute("SELECT honker_bootstrap()")
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, email TEXT)"
        )

    def run(self, body, commit):
        self.conn.execute("BEGIN IMMEDIATE")
        body(
            lambda sql, args: self.conn.execute(sql, args).fetchall(),
            lambda payload: self.conn.execute(
                RAW_ENQUEUE, ("emails", json.dumps(payload))
            ).fetchall(),
        )
        assert self.conn.in_transaction, "a rejected enqueue ended the app transaction"
        self.conn.execute("COMMIT" if commit else "ROLLBACK")

    def query(self, sql):
        return self.conn.execute(sql).fetchall()


def _orm_app_unavailable() -> str | None:
    if not HAS_LOAD_EXTENSION:
        return "this Python's sqlite3 cannot load extensions"
    try:
        honker.extension_info()
    except FileNotFoundError as e:
        return f"loadable extension not built: {e}"
    return None


def _c8_bad_json_scenario(db_path, app):
    worker = _spawn("claim", db_path, "emails", "python-worker")
    try:
        assert _line(worker) == "READY"

        def write_then_bad_enqueue(order_id):
            def body(execute, _enqueue):
                execute(
                    "INSERT INTO orders (id, email) VALUES (?, ?)",
                    (order_id, f"o{order_id}@example.com"),
                )
                with pytest.raises(Exception, match=NOT_JSON) as err:
                    execute(RAW_ENQUEUE, ("emails", "not json"))
                assert "honker:" in str(err.value)

            return body

        # The app catches the error and commits: its own write survives,
        # no job exists.
        app.run(write_then_bad_enqueue(1), commit=True)
        assert app.query("SELECT id FROM orders ORDER BY id") == [(1,)]
        assert app.query("SELECT COUNT(*) FROM _honker_live") == [(0,)]

        # The app rolls back instead: neither exists.
        app.run(write_then_bad_enqueue(2), commit=False)
        assert app.query("SELECT id FROM orders ORDER BY id") == [(1,)]
        assert app.query("SELECT COUNT(*) FROM _honker_live") == [(0,)]

        # A valid enqueue in the same transaction as a rejected one.
        def bad_then_good(execute, enqueue):
            write_then_bad_enqueue(3)(execute, enqueue)
            enqueue({"order_id": 3})

        app.run(bad_then_good, commit=True)
        got = _json_line(_line(worker, timeout=8.0))
        assert got["payload"] == {"order_id": 3}
        _finish(worker)
        assert app.query("SELECT id FROM orders ORDER BY id") == [(1,), (3,)]
        assert app.query("SELECT COUNT(*) FROM _honker_live") == [(0,)]
        assert app.query("SELECT COUNT(*) FROM _honker_dead") == [(0,)]
    finally:
        _kill(worker)


def test_c8_non_json_enqueue_in_honker_transaction_raises_and_keeps_app_write(db_path):
    _c8_bad_json_scenario(db_path, _HonkerTxApp(db_path))


def test_c8_non_json_enqueue_in_orm_connection_raises_and_keeps_app_write(db_path):
    reason = _orm_app_unavailable()
    if reason:
        pytest.skip(reason)
    _c8_bad_json_scenario(db_path, _OrmConnectionApp(db_path))


@pytest.mark.parametrize(
    "value, message",
    [
        pytest.param(float("nan"), NOT_JSON, id="nan"),
        pytest.param(float("inf"), NOT_JSON, id="infinity"),
        pytest.param("\ud800", UNDECODABLE, id="lone-surrogate"),
    ],
)
def test_typed_python_apis_reject_what_json_dumps_emits_but_json_forbids(
    db_path, value, message
):
    db = honker.open(db_path)
    q = db.queue("typed")
    with pytest.raises(RuntimeError, match=message):
        q.enqueue({"v": value})
    with pytest.raises(RuntimeError, match=message):
        q.save_result(1, {"v": value})
    with pytest.raises(RuntimeError, match=message):
        db.stream("typed").publish({"v": value})
    with pytest.raises(RuntimeError, match=message):
        with db.transaction() as tx:
            tx.notify("typed", {"v": value})
    assert q.get_result(1) == (False, None)
    counts = db.query(
        "SELECT (SELECT COUNT(*) FROM _honker_live) AS live,"
        " (SELECT COUNT(*) FROM _honker_stream) AS events,"
        " (SELECT COUNT(*) FROM _honker_notifications) AS notes"
    )[0]
    assert counts == {"live": 0, "events": 0, "notes": 0}


def test_python_save_result_still_round_trips_normal_values(db_path):
    db = honker.open(db_path)
    q = db.queue("results")
    for job_id, value in enumerate(
        ["done", 42, 1.5, True, None, [1, "two"], {"sum": 3, "nested": {"ok": True}}],
        start=1,
    ):
        q.save_result(job_id, value, ttl=60)
        assert q.get_result(job_id) == (True, value)


def test_non_json_notify_and_publish_raise_and_reach_no_listener(db_path):
    db = honker.open(db_path)
    listener = _spawn("listen-once", db_path, "orders")
    reader = _spawn("stream-read", db_path, "orders-stream", "dashboard", "1")
    try:
        assert _line(listener) == "READY"
        assert _line(reader) == "READY"

        with pytest.raises(RuntimeError, match=NOT_JSON):
            with db.transaction() as tx:
                tx.notify("orders", {"total": float("nan")})
        with pytest.raises(RuntimeError, match=UNDECODABLE):
            with db.transaction() as tx:
                tx.notify("orders", {"note": "\ud800"})
        with pytest.raises(RuntimeError, match=NOT_JSON):
            with db.transaction() as tx:
                tx.query("SELECT notify(?, ?)", ["orders", "not json"])
        stream = db.stream("orders-stream")
        with pytest.raises(RuntimeError, match=NOT_JSON):
            stream.publish({"total": float("nan")})
        with pytest.raises(RuntimeError, match=UNDECODABLE):
            stream.publish({"note": "\ud800"})
        with pytest.raises(RuntimeError, match=NOT_JSON):
            with db.transaction() as tx:
                tx.query(
                    "SELECT honker_stream_publish(?, NULL, ?)",
                    ["orders-stream", "not json"],
                )

        # Then a good one each: it is the first thing either side sees,
        # and the stream offset was not consumed by the rejected ones.
        with db.transaction() as tx:
            tx.notify("orders", {"order_id": 1})
        stream.publish({"order_id": 1})

        assert _json_line(_line(listener, timeout=8.0))["payload"] == {"order_id": 1}
        assert _json_line(_line(reader, timeout=8.0)) == [
            {"offset": 1, "payload": {"order_id": 1}}
        ]
        _finish(listener)
        _finish(reader)
    finally:
        _kill(listener)
        _kill(reader)


_SCHED_SNAPSHOT = """
SELECT
  (SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name = 'good') AS good_next,
  (SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name = 'bad-payload') AS bad_next,
  (SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name = 'zero-attempts') AS zero_next,
  (SELECT COUNT(*) FROM _honker_live WHERE queue = 'sched-good') AS good_jobs,
  (SELECT COUNT(*) FROM _honker_live WHERE queue = 'sched-bad') AS bad_jobs,
  (SELECT COUNT(*) FROM _honker_live WHERE queue = 'sched-zero') AS zero_jobs,
  (SELECT json_group_array(payload) FROM (
     SELECT payload FROM _honker_live WHERE queue = 'sched-bad' ORDER BY id)) AS bad_payloads,
  (SELECT json_group_array(json_object('run_at', run_at, 'error', last_error)) FROM (
     SELECT run_at, last_error FROM _honker_dead WHERE queue = 'sched-bad' ORDER BY run_at, id)) AS bad_dead,
  (SELECT json_group_array(json_object('run_at', run_at, 'error', last_error)) FROM (
     SELECT run_at, last_error FROM _honker_dead WHERE queue = 'sched-zero' ORDER BY run_at, id)) AS zero_dead,
  (SELECT COUNT(*) FROM _honker_dead WHERE queue = 'sched-good') AS good_dead
"""


def _sched_snapshot(db) -> dict:
    # One SELECT, so the counts and next_fire_at come from one snapshot.
    row = dict(db.query(_SCHED_SNAPSHOT)[0])
    for k in ("bad_payloads", "bad_dead", "zero_dead"):
        row[k] = json.loads(row[k])
    return row


def test_legacy_bad_schedules_are_dead_lettered_while_two_scheduler_processes_keep_firing(
    db_path,
):
    """Two schedule rows an older build or raw SQL could leave behind (a
    non-JSON payload, max_attempts = 0) next to a good one, run by two
    Scheduler.run processes. The good one fires once per boundary, each
    bad boundary lands in _honker_dead naming its schedule, the
    schedulers stay up, and fixing a row with Scheduler.update makes it
    fire."""
    db = honker.open(db_path)
    scheduler = honker.Scheduler(db)
    scheduler.add(
        name="good", queue="sched-good", schedule=honker.every_s(1), payload={"ok": True}
    )
    first = db.query(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name = 'good'"
    )[0]["next_fire_at"]
    with db.transaction() as tx:
        for name, queue, payload, max_attempts in [
            ("bad-payload", "sched-bad", "not json", 3),
            ("zero-attempts", "sched-zero", '{"ok": true}', 0),
        ]:
            tx.execute(
                "INSERT INTO _honker_scheduler_tasks"
                " (name, queue, cron_expr, payload, priority, next_fire_at, enabled, max_attempts)"
                " VALUES (?, ?, '@every 1s', ?, 0, ?, 1, ?)",
                [name, queue, payload, first, max_attempts],
            )

    procs = [_spawn("scheduler-run", db_path, "12") for _ in range(2)]
    try:
        for p in procs:
            assert _line(p) == "READY"
        time.sleep(5.0)

        snap = _sched_snapshot(db)
        alive = [p.poll() is None for p in procs]
        assert alive == [True, True], [
            p.stderr.read() if p.poll() is not None else "" for p in procs
        ]
        boundaries = snap["good_next"] - first
        assert boundaries >= 3, snap
        # All three advanced together, in the same ticks.
        assert snap["bad_next"] == snap["zero_next"] == snap["good_next"], snap
        # The good schedule fired exactly once per boundary.
        assert snap["good_jobs"] == boundaries, snap
        assert snap["good_dead"] == 0, snap
        # Each bad boundary is one dead row, at that boundary, naming the
        # schedule; nothing was enqueued for it.
        assert snap["bad_jobs"] == snap["zero_jobs"] == 0, snap
        expected = list(range(first, snap["good_next"]))
        assert [d["run_at"] for d in snap["bad_dead"]] == expected, snap
        assert [d["run_at"] for d in snap["zero_dead"]] == expected, snap
        for d in snap["bad_dead"]:
            assert NOT_JSON in d["error"] and 'schedule "bad-payload"' in d["error"], d
        for d in snap["zero_dead"]:
            assert "max_attempts must be at least 1" in d["error"], d
            assert 'schedule "zero-attempts"' in d["error"], d

        # Fix both through the public API while the schedulers run.
        assert scheduler.update("bad-payload", payload={"fixed": True})
        assert scheduler.update("zero-attempts", max_attempts=3)
        time.sleep(2.5)

        after = _sched_snapshot(db)
        assert [p.poll() is None for p in procs] == [True, True]
        total = after["good_next"] - first
        assert after["good_jobs"] == total, after
        assert after["bad_jobs"] >= 1 and after["zero_jobs"] >= 1, after
        assert set(after["bad_payloads"]) == {'{"fixed": true}'}, after
        # Every boundary is accounted for exactly once: a job or a dead row.
        assert after["bad_jobs"] + len(after["bad_dead"]) == total, after
        assert after["zero_jobs"] + len(after["zero_dead"]) == total, after

        for p in procs:
            assert _line(p, timeout=12.0) == "STOPPED"
            _finish(p)
    finally:
        for p in procs:
            _kill(p)
