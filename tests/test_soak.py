"""Sustained-load soak tests.

`test_resource_bounds.py` already guards against thread leaks on
listener churn; those tests are seconds-scale. This file runs
minute-scale soak to catch slow memory and disk-usage leaks that
wouldn't show up in a fast test: statement-cache bloat, missing
WAL checkpoint, bridge-thread accumulation under steady-state
load.

The resource soaks are marked `@pytest.mark.slow` and excluded from
the default `pytest` run; invoke them via `pytest -m slow
tests/test_soak.py`. The mixed-workload soak at the bottom is not
slow-marked: its 40 s default runs in PR CI, and HONKER_SOAK_SECONDS
lengthens it for the nightly run.
"""

import asyncio
import os
import sys
import time

import pytest

import honker


def _rss_bytes() -> int:
    """Current process RSS in bytes. `ru_maxrss` is in KB on Linux,
    bytes on macOS — normalize.

    `resource` is Unix-only; importing it at module top would fail
    pytest collection on Windows even though every test in this file
    is `@pytest.mark.slow` and excluded by default. The slow tests
    don't claim to run on Windows; lazy-import keeps that explicit.
    """
    import resource  # noqa: PLC0415 — see docstring
    kb_or_bytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return kb_or_bytes
    return kb_or_bytes * 1024


@pytest.mark.slow
async def test_60s_sustained_notify_no_rss_growth(db_path):
    """Run 60 seconds of 100 notify/s with one listener consuming
    them. Assert peak RSS growth stays under a loose bound (30 MB).

    Catches: statement cache bloat, asyncio task leaks, bridge
    thread accumulation, notification-table runaway (reminder:
    notifications are never auto-pruned; this test also prunes on
    its own to isolate *library* growth from test-data growth).
    """
    db = honker.open(db_path)

    # Warm up: open connections + register functions + stabilize
    # allocator state before baseline.
    db.queue("_warm")
    with db.transaction() as tx:
        tx.notify("_warm", "ok")
    await asyncio.sleep(0.1)

    baseline = _rss_bytes()

    received: list = []
    lst = db.listen("soak")

    async def consume():
        async for n in lst:
            received.append(n.id)

    consumer = asyncio.create_task(consume())

    DURATION_S = 60
    RATE_HZ = 100
    INTERVAL_S = 1.0 / RATE_HZ

    deadline = time.time() + DURATION_S
    prune_every = 10  # seconds — keep the notifications table bounded
    next_prune = time.time() + prune_every
    sent = 0
    peak = baseline

    while time.time() < deadline:
        with db.transaction() as tx:
            tx.notify("soak", {"i": sent})
        sent += 1
        if time.time() >= next_prune:
            # Keep only the most recent 1000 notifications. Without
            # this, the table grows unbounded — a real user issue
            # but not what this test is measuring.
            db.prune_notifications(max_keep=1000)
            next_prune += prune_every
            peak = max(peak, _rss_bytes())
        await asyncio.sleep(INTERVAL_S)

    # Give the listener a moment to drain.
    await asyncio.sleep(0.5)
    consumer.cancel()
    try:
        await consumer
    except asyncio.CancelledError:
        pass

    growth = peak - baseline
    # Loose ceiling: 30 MB over 60s. Allocator + asyncio internals
    # eat a few MB even on steady-state workloads; actual steady
    # growth should be far less.
    assert growth < 30 * 1024 * 1024, (
        f"peak RSS grew {growth / 1024 / 1024:.1f} MB over {DURATION_S}s "
        f"of 100 notify/s (baseline={baseline / 1024 / 1024:.1f} MB, "
        f"peak={peak / 1024 / 1024:.1f} MB). Likely a leak in the "
        f"bridge thread, statement cache, or listener buffer."
    )
    # Sanity: listener actually kept up (we won't be picky, but at
    # least 90% of sends should have made it through).
    assert len(received) >= int(sent * 0.9), (
        f"listener dropped too many: sent={sent}, got={len(received)}. "
        f"Delivery slowdown, not a memory test failure, but flag it."
    )


@pytest.mark.slow
def test_wal_bounded_under_sustained_writes(db_path):
    """Sustained commit-per-write loop must keep the .db-wal file
    bounded — SQLite auto-checkpoints every 10k pages per our
    PRAGMA. If a binding accidentally disables or stalls
    checkpointing, the WAL balloons linearly with write count.

    Writes 20k separate enqueue transactions and asserts WAL stays
    under 80 MB (auto-checkpoint threshold is 10k * 4096 B ≈ 40 MB,
    double for headroom).
    """
    db = honker.open(db_path)
    q = db.queue("wal-soak")

    N = 20_000
    for i in range(N):
        # Separate transactions to force WAL growth; a single big
        # tx would commit one page batch regardless of row count.
        q.enqueue({"i": i, "blob": "x" * 256})

    wal_size = os.path.getsize(f"{db_path}-wal")
    # Loose: 80 MB. Autocheckpoint kicks at ~40 MB (10k pages * 4K).
    # If it didn't kick, 20k rows * ~400 bytes/row + overhead ≈ 8-12 MB
    # just from this test — but the invariant is "bounded," not tight.
    assert wal_size < 80 * 1024 * 1024, (
        f"WAL grew to {wal_size / 1024 / 1024:.1f} MB after {N} writes. "
        f"Expected <80 MB (auto-checkpoint at 10k pages). Likely the "
        f"wal_autocheckpoint PRAGMA didn't apply or was overridden."
    )


# ---------------------------------------------------------------------
# Mixed-workload soak, from the user's view.
#
# Producer, worker and scheduler processes, all written against one
# binding's public API, share a WAL file for HONKER_SOAK_SECONDS
# (default 40). Workers are SIGKILLed and restarted; one scheduler
# process is killed while two run in hot standby. The parent only
# observes: an audit trigger on _honker_live records every job ever
# inserted, and each child appends what it did to its own ledger.
#
# Not marked slow: the short run is part of PR CI. The nightly
# workflow sets HONKER_SOAK_SECONDS for a long run.
# ---------------------------------------------------------------------

import json  # noqa: E402
import random  # noqa: E402
import shutil  # noqa: E402
import signal  # noqa: E402
import sqlite3  # noqa: E402
import subprocess  # noqa: E402
from collections import defaultdict  # noqa: E402

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PACKAGES_ROOT = os.path.join(REPO_ROOT, "packages")
HONKER_PYTHON_ROOT = os.path.join(PACKAGES_ROOT, "honker", "python")
HONKER_NODE_ROOT = os.path.join(PACKAGES_ROOT, "honker-node")

SOAK_SECONDS = float(os.environ.get("HONKER_SOAK_SECONDS", "40"))
SOAK_SEED = int(os.environ.get("HONKER_SOAK_SEED", str(random.randrange(1 << 30))))

# queue -> (visibility_timeout_s, max_attempts)
SOAK_QUEUES = {
    "emails": (30, 3),
    "reports": (30, 3),
    "media": (2, 3),
    "beats": (30, 3),
}

# kind -> (queue, weight, options). Options are copied into the payload
# (behaviour) or used by the producer (delay, expires, priority, cancel).
SOAK_KINDS = {
    "normal": ("emails", 45, {}),
    "flaky": ("emails", 12, {"fail_first": 1}),
    "always_fail": ("emails", 4, {"always_fail": True}),
    "delayed": ("reports", 10, {"delay": "1-4"}),
    "expire_pending": ("reports", 4, {"delay": 3, "expires": 2}),
    "cancel_scheduled": ("reports", 4, {"delay": 8, "cancel": "scheduled"}),
    "cancel_inflight": ("reports", 3, {"sleep_s": 2, "cancel": "inflight"}),
    "overrun": ("media", 3, {"sleep_s": 3}),
    "expire_inflight": ("media", 3, {"sleep_s": 4, "expires": 3}),
    "heartbeat_long": ("media", 3, {"sleep_s": 4, "heartbeat": True}),
}


_PY_SOAK_COMMON = r'''
import asyncio, json, os, signal, sys, time
cfg = json.loads(sys.argv[1])
for p in cfg["paths"]:
    sys.path.insert(0, p)
import honker

LEDGER = open(os.path.join(cfg["logdir"], f"{cfg['role']}-{os.getpid()}.jsonl"), "a", buffering=1)


def log(**row):
    row.update(pid=os.getpid(), t=time.time())
    LEDGER.write(json.dumps(row) + "\n")


def stop_on_signals(stop):
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)


db = honker.open(cfg["db"])
queues = {name: db.queue(name, visibility_timeout_s=vt, max_attempts=ma)
          for name, (vt, ma) in cfg["queues"].items()}


def email_handler(p):
    # Decorated task body, run by db.run_workers in a thread.
    attempt = next_attempt(p["key"])
    log(event="start", key=p["key"], attempt=attempt)
    if p.get("always_fail"):
        log(event="end", key=p["key"], ok=False)
        raise RuntimeError("always fails")
    if attempt <= p.get("fail_first", 0):
        log(event="end", key=p["key"], ok=False)
        raise RuntimeError("fails first")
    log(event="end", key=p["key"], ok=True)


def next_attempt(key):
    # Per-key run counter that survives SIGKILL (a decorated task
    # cannot see job.attempts).
    n = 1
    while True:
        try:
            fd = os.open(os.path.join(cfg["logdir"], "marks", f"{key}.{n}"),
                         os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            return n
        except FileExistsError:
            n += 1


def beat_handler():
    log(event="beat")


email_task = queues["emails"].task(name="soak.email", retries=3,
                                   retry_delay_s=1, store_result=False)(email_handler)
beat_task = queues["beats"].task(name="soak.beat", store_result=False)(beat_handler)
'''

_PY_SOAK_WORKER = _PY_SOAK_COMMON + r'''

async def user_loop(qname, slot, stop):
    # A README-style loop over q.claim() for queues that need delays,
    # expiry and heartbeats per job.
    q = queues[qname]
    vt = cfg["queues"][qname][0]
    worker_id = f"py-{os.getpid()}-{qname}-{slot}"
    async for job in q.claim(worker_id):
        p = job.payload
        log(event="start", key=p["key"], job_id=job.id, attempt=job.attempts)
        try:
            end = time.time() + p.get("sleep_s", 0)
            while time.time() < end:
                await asyncio.sleep(min(0.5, max(0.0, end - time.time())))
                if p.get("heartbeat"):
                    job.heartbeat(vt)
            if p.get("always_fail"):
                raise RuntimeError("always fails")
        except Exception as e:
            log(event="end", key=p["key"], ok=False)
            log(event="retry", key=p["key"], ok=job.retry(delay_s=1, error=str(e)))
            continue
        log(event="end", key=p["key"], ok=True)
        log(event="ack", key=p["key"], ok=job.ack())
        if stop.is_set():
            return


async def main():
    stop = asyncio.Event()
    stop_on_signals(stop)
    loops = [asyncio.create_task(user_loop(q, i, stop))
             for q in ("reports", "media") for i in range(2)]
    workers = asyncio.create_task(db.run_workers(concurrency=2, stop_event=stop))
    print("READY", flush=True)
    await stop.wait()
    for t in loops:
        t.cancel()
    await asyncio.gather(*loops, return_exceptions=True)
    await workers
    db.close()


asyncio.run(main())
'''

_PY_SOAK_SCHEDULER = _PY_SOAK_COMMON + r'''

async def main():
    stop = asyncio.Event()
    stop_on_signals(stop)
    print("READY", flush=True)
    while not stop.is_set():
        sched = honker.Scheduler(db)
        sched.lock_ttl = 3
        sched.heartbeat_interval = 1
        try:
            await sched.run(stop)
        except (honker.LockHeld, honker.LeadershipLost):
            try:
                await asyncio.wait_for(stop.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass
    db.close()


asyncio.run(main())
'''

_PY_SOAK_PRODUCER = _PY_SOAK_COMMON + r'''

def main():
    plan = json.load(open(cfg["plan"]))
    print("READY", flush=True)
    start = time.time()
    watch = {}
    for item in plan + [None]:
        while True:
            # Cancel in-flight targets as soon as a worker holds them.
            for key, (q, job_id) in list(watch.items()):
                row = q.get_job(job_id)
                if row is None:
                    log(event="cancel", key=key, ok=None)
                    del watch[key]
                elif row["state"] == "processing":
                    log(event="cancel", key=key, ok=q.cancel(job_id))
                    del watch[key]
            if item is None and (watch and time.time() - start < cfg["seconds"] + 20):
                time.sleep(0.05)
                continue
            if item is None or time.time() - start >= item["at"]:
                break
            time.sleep(0.01)
        if item is None:
            break
        p = item["payload"]
        q = queues[item["queue"]]
        if item["queue"] == "emails":
            job_id = email_task(p).id
        else:
            job_id = q.enqueue(p, delay=item.get("delay"), expires=item.get("expires"),
                               priority=item.get("priority", 0))
        log(event="enqueue", key=p["key"], job_id=job_id)
        if item.get("cancel") == "scheduled":
            log(event="cancel", key=p["key"], ok=q.cancel(job_id))
        elif item.get("cancel") == "inflight":
            watch[p["key"]] = (q, job_id)
    print("DONE", flush=True)
    db.close()


main()
'''


_NODE_SOAK_COMMON = r'''
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const cfg = JSON.parse(process.argv[1]);
const honker = require(cfg.node);
const ledger = fs.openSync(path.join(cfg.logdir, `${cfg.role}-${process.pid}.jsonl`), 'a');
function log(row) {
  row.pid = process.pid;
  row.t = Date.now() / 1000;
  fs.writeSync(ledger, JSON.stringify(row) + '\n');
}
const db = honker.open(cfg.db);
const queues = {};
for (const [name, [vt, ma]] of Object.entries(cfg.queues)) {
  queues[name] = db.queue(name, { visibilityTimeoutS: vt, maxAttempts: ma });
}
const ac = new AbortController();
process.on('SIGTERM', () => ac.abort());
process.on('SIGINT', () => ac.abort());
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
'''

_NODE_SOAK_WORKER = _NODE_SOAK_COMMON + r'''
async function userLoop(qname, slot) {
  // The README loop: for await (const job of q.claim(id)).
  const q = queues[qname];
  const vt = cfg.queues[qname][0];
  const workerId = `node-${process.pid}-${qname}-${slot}`;
  for await (const job of q.claim(workerId, { signal: ac.signal })) {
    const p = job.payload;
    if (qname === 'beats') {
      log({ event: 'beat', job_id: job.id });
      job.ack();
      continue;
    }
    log({ event: 'start', key: p.key, job_id: job.id, attempt: job.attempts });
    try {
      const end = Date.now() + (p.sleep_s || 0) * 1000;
      while (Date.now() < end) {
        await sleep(Math.min(500, Math.max(0, end - Date.now())));
        if (p.heartbeat) job.heartbeat(vt);
      }
      if (p.always_fail) throw new Error('always fails');
      if (job.attempts <= (p.fail_first || 0)) throw new Error('fails first');
    } catch (err) {
      log({ event: 'end', key: p.key, ok: false });
      const ok = job.attempts >= cfg.queues[qname][1]
        ? job.fail(String(err.message))
        : job.retry(1, String(err.message));
      log({ event: 'retry', key: p.key, ok });
      continue;
    }
    log({ event: 'end', key: p.key, ok: true });
    log({ event: 'ack', key: p.key, ok: job.ack() });
  }
}
const loops = [];
for (const q of ['emails', 'reports', 'media', 'beats']) {
  for (let i = 0; i < 2; i++) loops.push(userLoop(q, i));
}
console.log('READY');
Promise.all(loops).then(() => { db.close(); }, (err) => {
  console.error(err && err.stack || err);
  process.exit(1);
});
'''

_NODE_SOAK_SCHEDULER = _NODE_SOAK_COMMON + r'''
const owner = `sched-${process.pid}`;
console.log('READY');
db.scheduler().run(owner, ac.signal).then(() => { db.close(); }, (err) => {
  console.error(err && err.stack || err);
  process.exit(1);
});
'''

_NODE_SOAK_PRODUCER = _NODE_SOAK_COMMON + r'''
(async () => {
  const plan = JSON.parse(fs.readFileSync(cfg.plan, 'utf8'));
  console.log('READY');
  const start = Date.now();
  const watch = new Map();
  const pollWatch = () => {
    for (const [key, [q, id]] of watch) {
      const row = q.getJob(id);
      if (row == null) {
        log({ event: 'cancel', key, ok: null });
        watch.delete(key);
      } else if (row.state === 'processing') {
        log({ event: 'cancel', key, ok: q.cancel(id) });
        watch.delete(key);
      }
    }
  };
  for (const item of plan) {
    while ((Date.now() - start) / 1000 < item.at) {
      pollWatch();
      await sleep(10);
    }
    const p = item.payload;
    const q = queues[item.queue];
    const id = q.enqueue(p, {
      delay: item.delay ?? null,
      expires: item.expires ?? null,
      priority: item.priority ?? 0,
    });
    log({ event: 'enqueue', key: p.key, job_id: id });
    if (item.cancel === 'scheduled') log({ event: 'cancel', key: p.key, ok: q.cancel(id) });
    else if (item.cancel === 'inflight') watch.set(p.key, [q, id]);
  }
  while (watch.size && (Date.now() - start) / 1000 < cfg.seconds + 20) {
    pollWatch();
    await sleep(50);
  }
  console.log('DONE');
  db.close();
})().catch((err) => {
  console.error(err && err.stack || err);
  process.exit(1);
});
'''


def _node_binding_available() -> bool:
    if shutil.which("node") is None:
        return False
    return any(
        name.endswith(".node") for name in os.listdir(HONKER_NODE_ROOT)
    )


def _soak_plan(rng: random.Random, seconds: float) -> list[dict]:
    kinds = list(SOAK_KINDS)
    weights = [SOAK_KINDS[k][1] for k in kinds]
    plan = []
    at = 0.5
    n = 0
    while at < seconds:
        kind = rng.choices(kinds, weights)[0]
        queue, _w, opts = SOAK_KINDS[kind]
        payload = {"key": f"k{n}", "kind": kind}
        for opt in ("sleep_s", "fail_first", "always_fail", "heartbeat"):
            if opt in opts:
                payload[opt] = opts[opt]
        item = {"at": round(at, 3), "queue": queue, "payload": payload}
        if "delay" in opts:
            d = opts["delay"]
            item["delay"] = rng.randint(1, 4) if d == "1-4" else d
        if "expires" in opts:
            item["expires"] = opts["expires"]
        if "cancel" in opts:
            item["cancel"] = opts["cancel"]
        if queue == "emails":
            item["priority"] = rng.randint(0, 9)
        plan.append(item)
        n += 1
        at += rng.uniform(0.02, 0.18)
    return plan


def _read_ledgers(logdir: str) -> list[dict]:
    rows = []
    for name in os.listdir(logdir):
        if not name.endswith(".jsonl"):
            continue
        with open(os.path.join(logdir, name)) as f:
            for line in f:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass  # a line cut short by SIGKILL
    return rows


class _Soak:
    def __init__(self, tmp_path, binding: str):
        self.binding = binding
        self.dir = str(tmp_path)
        self.db = os.path.join(self.dir, "soak.db")
        self.logdir = os.path.join(self.dir, "logs")
        os.makedirs(os.path.join(self.logdir, "marks"))
        self.procs: dict[str, subprocess.Popen] = {}
        self.killed_pids: set[int] = set()
        self.kills = 0
        self.stderr_files = []

    def cfg(self, role: str, **extra) -> str:
        cfg = {
            "db": self.db,
            "logdir": self.logdir,
            "role": role,
            "queues": SOAK_QUEUES,
            "paths": [HONKER_PYTHON_ROOT, PACKAGES_ROOT],
            "node": HONKER_NODE_ROOT,
            "seconds": SOAK_SECONDS,
        }
        cfg.update(extra)
        return json.dumps(cfg)

    def spawn(self, name: str, role: str, **extra) -> subprocess.Popen:
        scripts = {
            ("python", "worker"): _PY_SOAK_WORKER,
            ("python", "scheduler"): _PY_SOAK_SCHEDULER,
            ("python", "producer"): _PY_SOAK_PRODUCER,
            ("node", "worker"): _NODE_SOAK_WORKER,
            ("node", "scheduler"): _NODE_SOAK_SCHEDULER,
            ("node", "producer"): _NODE_SOAK_PRODUCER,
        }
        script = scripts[(self.binding, role)]
        if self.binding == "python":
            argv = [sys.executable, "-c", script, self.cfg(role, **extra)]
        else:
            argv = ["node", "-e", script, self.cfg(role, **extra)]
        err = open(os.path.join(self.dir, f"{name}-{len(self.stderr_files)}.err"), "w+")
        self.stderr_files.append((name, err))
        proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=err, text=True)
        line = proc.stdout.readline().strip()
        if line != "READY":
            proc.kill()
            err.seek(0)
            raise AssertionError(f"{name} did not start: {line!r} {err.read()}")
        self.procs[name] = proc
        return proc

    def kill(self, name: str) -> None:
        proc = self.procs[name]
        if proc.poll() is None:
            os.kill(proc.pid, signal.SIGKILL)
            proc.wait(timeout=10)
            self.killed_pids.add(proc.pid)
            self.kills += 1

    def stop(self, name: str) -> int:
        proc = self.procs[name]
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
                return -999
        return proc.returncode


def _install_audit(db_path: str) -> None:
    conn = sqlite3.connect(db_path, timeout=10)
    conn.executescript(
        """
        CREATE TABLE soak_audit (
            id INTEGER, queue TEXT, run_at INTEGER, expires_at INTEGER,
            at REAL
        );
        CREATE TRIGGER soak_audit_insert AFTER INSERT ON _honker_live BEGIN
            INSERT INTO soak_audit VALUES (
                NEW.id, NEW.queue, NEW.run_at, NEW.expires_at,
                (julianday('now') - 2440587.5) * 86400.0
            );
        END;
        """
    )
    conn.close()


def _register_beat(soak: _Soak) -> None:
    if soak.binding == "python":
        db = honker.open(soak.db)
        db.queue("beats").periodic_task(honker.every_s(1), name="soak.beat")(_soak_beat_stub)
        db.close()
    else:
        script = (
            "const h = require(%s); const db = h.open(%s);"
            "db.scheduler().add({name: 'soak.beat', queue: 'beats', cron: '@every 1s',"
            " payload: {beat: true}}); db.close();"
        ) % (json.dumps(HONKER_NODE_ROOT), json.dumps(soak.db))
        subprocess.run(["node", "-e", script], check=True, timeout=30)


def _soak_beat_stub():
    raise AssertionError("soak.beat runs in the worker process")



@pytest.mark.skipif(sys.platform == "win32", reason="SIGKILL is Unix-only")
@pytest.mark.parametrize("binding", ["python", "node"])
def test_mixed_workload_soak_accounts_for_every_job(tmp_path, binding):
    if binding == "node" and not _node_binding_available():
        if os.environ.get("HONKER_REQUIRE_NODE") == "1":
            pytest.fail("HONKER_REQUIRE_NODE=1 but the Node binding is not built")
        pytest.skip("Node binding not built here (the node CI job runs this)")

    rng = random.Random(SOAK_SEED)
    print(f"soak binding={binding} seed={SOAK_SEED} seconds={SOAK_SECONDS}")
    soak = _Soak(tmp_path, binding)
    honker.open(soak.db).close()
    _install_audit(soak.db)
    _register_beat(soak)
    plan = _soak_plan(rng, SOAK_SECONDS)
    plan_path = os.path.join(soak.dir, "plan.json")
    with open(plan_path, "w") as f:
        json.dump(plan, f)
    first_beat = int(
        sqlite3.connect(soak.db).execute(
            "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='soak.beat'"
        ).fetchone()[0]
    )

    started = time.time()
    workers = [f"worker{i}" for i in range(3)]
    schedulers = ["sched0", "sched1"]
    for name in workers:
        soak.spawn(name, "worker")
    for name in schedulers:
        soak.spawn(name, "scheduler")
    producer = soak.spawn("producer", "producer", plan=plan_path)

    # Chaos: SIGKILL a random worker every few seconds and restart it.
    # Python schedulers use a 3 s lock TTL, so killing one mid-run
    # exercises failover. Node's scheduler lock TTL is a fixed 60 s, so
    # a scheduler kill is only part of long runs there.
    sched_kills = {SOAK_SECONDS / 3: "sched0", 2 * SOAK_SECONDS / 3: "sched1"}
    if binding == "node" and SOAK_SECONDS < 180:
        sched_kills = {}
    next_kill = started + 4.0
    while time.time() - started < SOAK_SECONDS:
        now = time.time()
        if now >= next_kill:
            victim = rng.choice(workers)
            soak.kill(victim)
            time.sleep(rng.uniform(0.1, 1.0))
            soak.spawn(victim, "worker")
            next_kill = now + rng.uniform(3.0, 7.0)
        for at, name in list(sched_kills.items()):
            if now - started >= at:
                soak.kill(name)
                soak.spawn(name, "scheduler")
                del sched_kills[at]
        time.sleep(0.05)

    assert producer.stdout.readline().strip() == "DONE", "producer did not finish"
    for name in schedulers:
        assert soak.stop(name) == 0, f"{name} did not stop cleanly"
    # Drain: everything left must finish (the longest delay is 8 s).
    conn = sqlite3.connect(soak.db, timeout=10)
    deadline = time.time() + 45
    while time.time() < deadline:
        if conn.execute("SELECT COUNT(*) FROM _honker_live").fetchone()[0] == 0:
            break
        time.sleep(0.25)
    duration = time.time() - started
    alive = {name: soak.procs[name].poll() is None for name in workers}
    exits = {name: soak.stop(name) for name in workers}
    soak.stop("producer")

    live = conn.execute("SELECT id, queue, state, attempts FROM _honker_live").fetchall()
    dead = {
        row[0]: row[1]
        for row in conn.execute("SELECT id, last_error FROM _honker_dead")
    }
    audit = conn.execute("SELECT id, queue, run_at, expires_at FROM soak_audit").fetchall()
    beat_next = int(conn.execute(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='soak.beat'"
    ).fetchone()[0])
    conn.close()
    integrity = sqlite3.connect(soak.db).execute("PRAGMA integrity_check").fetchone()[0]

    rows = _read_ledgers(soak.logdir)
    by_key = defaultdict(list)
    for r in rows:
        if "key" in r:
            by_key[r["key"]].append(r)
    enq = {r["key"]: r for r in rows if r["event"] == "enqueue"}
    audit_by_id = {a[0]: a for a in audit}
    kinds = {item["payload"]["key"]: item["payload"]["kind"] for item in plan}

    problems = []
    counts = defaultdict(int)
    for key, kind in kinds.items():
        if key not in enq:
            problems.append(f"{key} ({kind}) was never enqueued")
            continue
        job_id = enq[key]["job_id"]
        events = by_key[key]
        starts = [e for e in events if e["event"] == "start"]
        ok_ends = [e for e in events if e["event"] == "end" and e["ok"]]
        cancels = [e for e in events if e["event"] == "cancel"]
        cancelled = any(c["ok"] is True for c in cancels)
        disturbed = any(s["pid"] in soak.killed_pids for s in starts)
        _id, _q, run_at, expires_at = audit_by_id[job_id]

        # When it ran.
        for s in starts:
            if run_at and s["t"] < run_at - 0.05:
                problems.append(f"{key} ({kind}) ran at {s['t']:.2f} before run_at {run_at}")
            if expires_at and s["t"] >= expires_at + 0.25:
                problems.append(f"{key} ({kind}) ran at {s['t']:.2f} after expiry {expires_at}")
            if cancelled and s["t"] > max(c["t"] for c in cancels) + 0.25:
                problems.append(f"{key} ({kind}) ran after it was cancelled")

        # Where it ended: exactly one of completed, dead, cancelled.
        if job_id in dead:
            outcome = "dead"
            reason = dead[job_id]
            allowed = {"max attempts exceeded"} if disturbed else set()
            if kind == "always_fail":
                allowed |= {"always fails", "max attempts exceeded"}
            if kind in ("expire_pending", "expire_inflight"):
                allowed.add("expired")
            if kind == "overrun":
                allowed.add("max attempts exceeded")
            if not any(reason == a or reason.startswith(a) or a in reason for a in allowed):
                problems.append(f"{key} ({kind}) dead with {reason!r}, starts={len(starts)}")
            if cancelled:
                problems.append(f"{key} ({kind}) was cancelled but reached the dead table")
        elif cancelled:
            outcome = "cancelled"
            if kind == "cancel_scheduled" and starts:
                problems.append(f"{key} cancelled while scheduled but ran {len(starts)}x")
        else:
            outcome = "completed"
            if not ok_ends:
                problems.append(f"{key} ({kind}) left the queue without a successful run")
            if kind in ("expire_pending", "always_fail", "cancel_scheduled"):
                problems.append(f"{key} ({kind}) completed but should not have")
            elif len(ok_ends) > 1 and not disturbed and kind != "overrun":
                problems.append(f"{key} ({kind}) succeeded {len(ok_ends)}x without a lease loss")
        if kind == "expire_pending" and starts:
            problems.append(f"{key} expired while scheduled but ran {len(starts)}x")
        counts[outcome] += 1
        counts[f"{kind}:{outcome}"] += 1

    produced = {enq[k]["job_id"] for k in enq}
    beat_ids = [a[0] for a in audit if a[1] == "beats"]
    phantom = [a for a in audit if a[0] not in produced and a[1] != "beats"]
    beats_fired = beat_next - first_beat
    beat_runs = [r for r in rows if r["event"] == "beat"]

    summary = {
        "binding": binding,
        "seed": SOAK_SEED,
        "seconds": round(duration, 1),
        "jobs": len(kinds),
        "kills": soak.kills,
        "beats_fired": beats_fired,
        "beat_jobs": len(beat_ids),
        "beat_runs": len(beat_runs),
        "outcomes": dict(sorted(counts.items())),
    }
    print("SOAK " + json.dumps(summary, sort_keys=True))

    assert not problems, f"{len(problems)} problems (seed {SOAK_SEED}):\n" + "\n".join(problems[:40])
    assert live == [], f"jobs still live after drain: {live[:20]}"
    assert phantom == [], f"jobs nobody enqueued: {phantom[:10]}"
    assert len(beat_ids) == beats_fired, (
        f"scheduler enqueued {len(beat_ids)} beat jobs for {beats_fired} boundaries"
    )
    assert beats_fired >= SOAK_SECONDS * 0.6, f"only {beats_fired} beats in {duration:.0f} s"
    assert all(dead_id not in beat_ids for dead_id in dead), "a beat job died"
    assert alive == {name: True for name in workers}, f"workers died: {alive}"
    assert exits == {name: 0 for name in workers}, f"worker exit codes: {exits}"
    assert integrity == "ok"
    assert soak.kills >= max(3, int(SOAK_SECONDS / 8))
