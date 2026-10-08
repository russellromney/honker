"""Model-checked, multi-process lifecycle torture run for Honker's job queue.

This module has three parts:

* ``worker_main``: one OS process. It loads the raw loadable extension
  through stdlib ``sqlite3`` and runs a seeded random loop of enqueue,
  claim, ack, retry, fail, heartbeat, stall-past-the-lease, abandon (the
  handler drops the job without a call, like a crashed handler thread)
  and cancel. About a third of the jobs expire 1-3 s after enqueue, so
  abandoned, stalled and SIGKILLed claims often expire in flight.
  Every call is written to a per-process JSONL ledger twice: a ``pre``
  record before the call and a ``post`` record with the return value or
  error after it. A process SIGKILLed mid-call therefore leaves a
  ``pre`` with no ``post``, which the checker treats as "outcome
  unknown" instead of guessing.
* ``run_torture``: the coordinator. It starts N worker processes (some
  share a worker id), SIGKILLs some mid-run and starts replacements with
  the same worker id, then quiesces, sweeps expired jobs and drains with
  a fresh worker.
* ``check``: reads every ledger and the final database and reports
  violations of the lifecycle invariants (see ``INVARIANTS``).

Handlers call the fenced forms by default (``honker_ack(id, worker,
attempt)`` and friends, passing the ``attempts`` their claim returned).
``fenced=False`` drives the legacy unfenced forms instead, which fail
the fencing invariant when two processes share a worker id (#176).

No binding code is used. Only the extension's SQL functions.

Run one worker by hand: ``python tests/lifecycle_torture.py worker <json-config>``.
"""

from __future__ import annotations

import json
import os
import random
import signal
import sqlite3
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

QUEUES = ("q0", "q1")

INVARIANTS = {
    "1_one_end_state": "every job ends in exactly one place",
    "2_fencing": "a successful ack/retry/fail/heartbeat comes from the latest claim",
    "3_no_resurrection": "a cancelled job is never dead, claimed or acted on again",
    "3b_cancel_takes_effect": "a cancel of a job that is live before and after it returns 1",
    "4_attempts": "attempts <= max_attempts and strictly increase per job",
    "5_expiry": "no claim at/after expires_at; no expired job left live after quiesce",
    "6_liveness": "after quiesce and drain nothing is processing or claimable",
    "7_integrity": "PRAGMA integrity_check is ok",
}


# ---------------------------------------------------------------------
# Extension loading
# ---------------------------------------------------------------------


def find_extension() -> str | None:
    env = os.environ.get("HONKER_EXTENSION_PATH")
    if env:
        return env if os.path.exists(env) else None
    for name in ("libhonker_ext.dylib", "libhonker_ext.so", "honker_ext.dll"):
        cand = os.path.join(REPO_ROOT, "target", "release", name)
        if os.path.exists(cand):
            return cand
    return None


def connect(db_path: str, ext_path: str) -> sqlite3.Connection:
    # Autocommit: each `SELECT honker_x(...)` is its own transaction,
    # which is how most ORM users call the extension.
    conn = sqlite3.connect(db_path, isolation_level=None, timeout=10.0)
    conn.enable_load_extension(True)
    try:
        conn.load_extension(ext_path, entrypoint="sqlite3_honkerext_init")
    except TypeError:  # Python < 3.12 has no entrypoint argument
        conn.load_extension(ext_path)
    conn.enable_load_extension(False)
    conn.execute("PRAGMA busy_timeout = 10000")
    return conn


# ---------------------------------------------------------------------
# Ledger
# ---------------------------------------------------------------------


class Ledger:
    """Append-only JSONL. One os.write per record, so a SIGKILL can at
    worst lose the record being written, never corrupt an earlier one.
    The record reaches the kernel before the call it describes starts."""

    def __init__(self, path: str, tag: str, wid: str):
        self.fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        self.tag = tag
        self.wid = wid
        self.seq = 0

    def write(self, rec: dict) -> None:
        rec.setdefault("p", self.tag)
        rec.setdefault("wid", self.wid)
        rec.setdefault("t", time.time())
        os.write(self.fd, (json.dumps(rec, separators=(",", ":")) + "\n").encode())

    def call(self, op: str, fn, **args):
        """Record pre, run fn, record post. Returns (ok, value)."""
        self.seq += 1
        k = self.seq
        self.write({"k": k, "ph": "pre", "op": op, **args})
        try:
            value = fn()
        except sqlite3.Error as e:
            self.write({"k": k, "ph": "post", "op": op, "err": str(e), **args})
            return False, None
        post = {"k": k, "ph": "post", "op": op, **args}
        if isinstance(value, dict):
            post.update(value)
        else:
            post["ret"] = value
        self.write(post)
        return True, value


# ---------------------------------------------------------------------
# Worker process
# ---------------------------------------------------------------------


def _scalar(conn, sql, params=()):
    return conn.execute(sql, params).fetchone()[0]


def _enqueue(conn, rng, tag, n):
    queue = rng.choice(QUEUES)
    tok = f"{tag}-{n}"
    r = rng.random()
    delay = None if r < 0.5 else rng.randint(0, 2)
    expires = rng.randint(1, 3) if rng.random() < 0.35 else None
    max_attempts = rng.randint(1, 4)
    priority = rng.randint(0, 1)
    # "exp" lets the handler know the job expires (see _handle).
    payload = json.dumps({"tok": tok, "exp": expires})

    def do():
        conn.execute("BEGIN IMMEDIATE")
        try:
            jid = _scalar(
                conn,
                "SELECT honker_enqueue(?, ?, NULL, ?, ?, ?, ?)",
                (queue, payload, delay, priority, max_attempts, expires),
            )
            run_at, expires_at, ma = conn.execute(
                "SELECT run_at, expires_at, max_attempts FROM _honker_live WHERE id = ?",
                (jid,),
            ).fetchone()
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        return {"id": jid, "run_at": run_at, "expires_at": expires_at, "max_attempts": ma}

    return do, dict(queue=queue, tok=tok, delay=delay, expires=expires, max_attempts=max_attempts)


def _claim(conn, queue, wid, n, lease):
    def do():
        raw = _scalar(conn, "SELECT honker_claim_batch(?, ?, ?, ?)", (queue, wid, n, lease))
        jobs = []
        for j in json.loads(raw):
            jobs.append(
                {
                    "id": j["id"],
                    "att": j["attempts"],
                    "claimed_at": j["claimed_at"],
                    "cexp": j["claim_expires_at"],
                    "tok": json.loads(j["payload"]).get("tok"),
                    "exp": json.loads(j["payload"]).get("exp"),
                }
            )
        return {"jobs": jobs}

    return do


def _lifecycle(conn, op, jid, att, wid, rng, fenced):
    # Fenced forms take the claim's attempts as a trailing token.
    tok = ", ?" if fenced else ""
    extra = (att,) if fenced else ()
    if op == "ack":
        return lambda: _scalar(conn, f"SELECT honker_ack(?, ?{tok})", (jid, wid, *extra)), {}
    if op == "retry":
        d = rng.randint(0, 1)
        return (
            lambda: _scalar(
                conn, f"SELECT honker_retry(?, ?, ?, 'torture retry'{tok})", (jid, wid, d, *extra)
            ),
            {"delay": d},
        )
    if op == "fail":
        return (
            lambda: _scalar(conn, f"SELECT honker_fail(?, ?, 'torture fail'{tok})", (jid, wid, *extra)),
            {},
        )
    if op == "heartbeat":
        e = rng.randint(1, 2)
        return (
            lambda: _scalar(conn, f"SELECT honker_heartbeat(?, ?, ?{tok})", (jid, wid, e, *extra)),
            {"extend": e},
        )
    raise ValueError(op)


def _handle(conn, led, rng, wid, job, fenced):
    jid, att = job["id"], job["att"]
    # A little "work" so SIGKILLs often land mid-handler.
    time.sleep(rng.uniform(0.0, 0.15))
    # The handler dies without a word, and the process lives on. The
    # lease lapses with nobody to finish the job; if it also expires
    # before a reclaim, only the expiry rule can end it (#177). Jobs that
    # expire are abandoned more often, so every run sees that case.
    if rng.random() < (0.35 if job.get("exp") else 0.05):
        led.write({"ev": "abandon", "id": jid, "att": att})
        return
    r = rng.random()
    if r < 0.28:
        plan = ["ack"]
    elif r < 0.42:
        plan = ["retry"]
    elif r < 0.50:
        plan = ["fail"]
    elif r < 0.62:
        plan = ["heartbeat", "ack"]
    else:
        # Stall past the lease, then act as the stale owner. Reclaim
        # needs unixepoch() > claim_expires_at, so wake at least one
        # full second after the integer deadline.
        stall = job["cexp"] + 1 + rng.uniform(0.05, 1.0) - time.time()
        led.write({"ev": "stall", "id": jid, "att": att, "secs": round(stall, 3)})
        if stall > 0:
            time.sleep(stall)
        plan = [rng.choice(["ack", "retry", "fail", "heartbeat"])]
        if plan[0] == "heartbeat":
            plan.append("ack")
    for op in plan:
        fn, extra = _lifecycle(conn, op, jid, att, wid, rng, fenced)
        ok, ret = led.call(op, fn, id=jid, att=att, fenced=fenced, **extra)
        if op == "heartbeat":
            if not ok or ret != 1:
                return
            time.sleep(rng.uniform(0.0, 0.3))


def _cancel_target(conn, rng):
    if rng.random() < 0.5:
        row = conn.execute(
            "SELECT id FROM _honker_live WHERE state = 'processing' ORDER BY random() LIMIT 1"
        ).fetchone()
        if row:
            return row[0]
    hi = conn.execute("SELECT seq FROM sqlite_sequence WHERE name = '_honker_live'").fetchone()
    hi = hi[0] if hi else 1
    return rng.randint(1, max(1, hi))


def worker_main(cfg: dict) -> None:
    rng = random.Random(cfg["seed"])
    wid = cfg["wid"]
    tag = cfg["tag"]
    fenced = cfg.get("fenced", True)
    led = Ledger(cfg["ledger"], tag, wid)
    conn = connect(cfg["db"], cfg["ext"])
    led.write({"ev": "start", "seed": cfg["seed"], "pid": os.getpid()})
    n = 0
    deadline = cfg["deadline"]
    while time.time() < deadline:
        r = rng.random()
        try:
            if r < 0.32:
                n += 1
                fn, args = _enqueue(conn, rng, tag, n)
                led.call("enqueue", fn, **args)
            elif r < 0.88:
                queue = rng.choice(QUEUES)
                lease = rng.randint(1, 2)
                batch = rng.randint(1, 3)
                ok, res = led.call(
                    "claim", _claim(conn, queue, wid, batch, lease), queue=queue, n=batch, lease=lease
                )
                if ok:
                    for job in res["jobs"]:
                        _handle(conn, led, rng, wid, job, fenced)
                if not ok or not res["jobs"]:
                    time.sleep(rng.uniform(0.01, 0.08))
            else:
                target = _cancel_target(conn, rng)
                led.call("cancel", lambda: _scalar(conn, "SELECT honker_cancel(?)", (target,)), id=target)
        except sqlite3.Error as e:  # e.g. busy on the cancel-target read
            led.write({"ev": "loop_error", "err": str(e)})
            time.sleep(0.05)
    led.write({"ev": "exit"})
    conn.close()


# ---------------------------------------------------------------------
# Coordinator
# ---------------------------------------------------------------------


@dataclass
class RunResult:
    seed: int
    seconds: float
    workdir: str
    db_path: str
    ledgers: list
    kills: int = 0
    notes: list = field(default_factory=list)


def _spawn(cfg, log_dir):
    out = open(os.path.join(log_dir, f"{cfg['tag']}.stderr"), "wb")
    proc = subprocess.Popen(
        [sys.executable, os.path.abspath(__file__), "worker", json.dumps(cfg)],
        stdout=out,
        stderr=subprocess.STDOUT,
    )
    out.close()
    return proc


def run_torture(
    workdir: str,
    ext_path: str,
    seed: int,
    seconds: float,
    procs: int = 6,
    shared_ids: bool = True,
    fenced: bool = True,
) -> RunResult:
    rng = random.Random(seed)
    db_path = os.path.join(workdir, "torture.db")
    conn = connect(db_path, ext_path)
    assert conn.execute("PRAGMA journal_mode = WAL").fetchone()[0] == "wal"
    conn.execute("SELECT honker_bootstrap()")

    # Worker ids: pairs share an id, so a stalled handler can watch its
    # "restarted self" reclaim the job. Replacements after a SIGKILL
    # reuse the killed worker's id.
    wids = []
    for i in range(procs):
        wids.append(f"w{i // 2}" if (shared_ids and i < 4) else f"w{i}")

    coord = Ledger(os.path.join(workdir, "coord.jsonl"), "coord", "-")
    coord.write(
        {"ev": "run", "seed": seed, "seconds": seconds, "procs": procs, "wids": wids, "fenced": fenced}
    )
    deadline = time.time() + seconds
    ledgers = []
    live = {}  # slot -> (proc, cfg)
    gen = 0

    def start(slot):
        nonlocal gen
        gen += 1
        tag = f"p{slot}g{gen}"
        cfg = {
            "seed": rng.randrange(1 << 31),
            "wid": wids[slot],
            "tag": tag,
            "db": db_path,
            "ext": ext_path,
            "ledger": os.path.join(workdir, f"{tag}.jsonl"),
            "deadline": deadline,
            "fenced": fenced,
        }
        ledgers.append(cfg["ledger"])
        live[slot] = (_spawn(cfg, workdir), cfg)
        coord.write({"ev": "spawn", "tag": tag, "slot": slot, "worker": wids[slot]})

    for slot in range(procs):
        start(slot)

    kills = 0
    next_kill = time.time() + rng.uniform(1.5, 3.5)
    while time.time() < deadline:
        time.sleep(0.05)
        for slot, (proc, cfg) in list(live.items()):
            if proc.poll() is not None and time.time() < deadline - 1:
                # A worker that exits early crashed. Record and replace.
                coord.write({"ev": "early_exit", "tag": cfg["tag"], "rc": proc.returncode})
                start(slot)
        if time.time() >= next_kill and time.time() < deadline - 1.0:
            slot = rng.randrange(procs)
            proc, cfg = live[slot]
            if proc.poll() is None:
                proc.send_signal(signal.SIGKILL)
                proc.wait()
                kills += 1
                coord.write({"ev": "kill", "tag": cfg["tag"], "slot": slot})
                start(slot)
            next_kill = time.time() + rng.uniform(1.5, 4.0)

    # Let workers finish their current handler (a stall is at most ~4 s).
    for slot, (proc, cfg) in live.items():
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.send_signal(signal.SIGKILL)
            proc.wait()
            coord.write({"ev": "kill", "tag": cfg["tag"], "slot": slot, "reason": "straggler"})
        if proc.returncode not in (0, -signal.SIGKILL):
            coord.write({"ev": "bad_exit", "tag": cfg["tag"], "rc": proc.returncode})

    _quiesce_and_drain(conn, workdir, ledgers)
    conn.close()
    return RunResult(seed, seconds, workdir, db_path, ledgers, kills)


def _quiesce_and_drain(conn, workdir, ledgers):
    path = os.path.join(workdir, "drain.jsonl")
    ledgers.append(path)
    wid = f"drain-{os.getpid()}"
    led = Ledger(path, "drain", wid)

    def now():
        return _scalar(conn, "SELECT unixepoch()")

    # 1. Let every lease and every delay lapse.
    last = _scalar(
        conn,
        "SELECT max(COALESCE(MAX(claim_expires_at), 0), COALESCE(MAX(run_at), 0)) FROM _honker_live",
    )
    while now() <= last:
        time.sleep(0.2)
    led.write({"ev": "quiesced", "now": now()})

    # 2. Sweep expired per queue.
    for q in QUEUES:
        led.call("sweep", lambda q=q: _scalar(conn, "SELECT honker_sweep_expired(?)", (q,)), queue=q)

    # 3. Drain with a fresh worker until nothing is claimable.
    give_up = time.time() + 20
    while time.time() < give_up:
        got = 0
        for q in QUEUES:
            ok, res = led.call(
                "claim", _claim(conn, q, wid, 50, 30), queue=q, n=50, lease=30
            )
            if not ok:
                continue
            for job in res["jobs"]:
                got += 1
                led.call(
                    "ack",
                    lambda j=job: _scalar(conn, "SELECT honker_ack(?, ?)", (j["id"], wid)),
                    id=job["id"],
                    att=job["att"],
                )
        future = _scalar(
            conn,
            "SELECT count(*) FROM _honker_live "
            "WHERE state IN ('pending', 'scheduled') AND run_at > unixepoch()",
        )
        if got == 0 and future == 0:
            break
        time.sleep(0.2)
    led.write({"ev": "drained", "now": now()})


# ---------------------------------------------------------------------
# Checker
# ---------------------------------------------------------------------


@dataclass
class Violation:
    invariant: str
    job: int | None
    msg: str


@dataclass
class Report:
    seed: int
    violations: list
    stats: dict
    job_lines: dict  # job id -> [raw ledger lines]

    def by_invariant(self, inv):
        return [v for v in self.violations if v.invariant == inv]

    def describe(self, inv, limit=5):
        out = [f"seed={self.seed} invariant {inv}: {INVARIANTS[inv]}", f"stats: {json.dumps(self.stats)}"]
        vs = self.by_invariant(inv)
        out.append(f"{len(vs)} violation(s)")
        for v in vs[:limit]:
            out.append(f"\n-- job {v.job}: {v.msg}")
            for line in self.job_lines.get(v.job, [])[:40]:
                out.append("   " + line)
        return "\n".join(out)


def _read_ledgers(paths):
    recs = []
    for path in paths:
        if not os.path.exists(path):
            continue
        with open(path) as f:
            for raw in f:
                raw = raw.rstrip("\n")
                try:
                    rec = json.loads(raw)
                except json.JSONDecodeError:
                    continue  # a record cut short by SIGKILL
                rec["_raw"] = raw
                recs.append(rec)
    return recs


def _job_ids(rec):
    ids = []
    if "id" in rec and isinstance(rec["id"], int):
        ids.append(rec["id"])
    for j in rec.get("jobs", []) or []:
        ids.append(j["id"])
    return ids


def check(result: RunResult) -> Report:
    recs = _read_ledgers(result.ledgers)
    killed = set()
    coord = _read_ledgers([os.path.join(result.workdir, "coord.jsonl")])
    for c in coord:
        if c.get("ev") == "kill":
            killed.add(c["tag"])

    job_lines = defaultdict(list)
    for r in recs:
        for jid in _job_ids(r):
            job_lines[jid].append((r.get("t", 0), r["_raw"]))

    # Join pre/post by (process, seq).
    calls = {}
    for r in recs:
        if "k" not in r:
            continue
        key = (r["p"], r["k"])
        c = calls.setdefault(key, {"p": r["p"], "wid": r["wid"], "op": r["op"]})
        if r["ph"] == "pre":
            c["pre"] = r
        else:
            c["post"] = r
    calls = [c for c in calls.values() if "pre" in c]
    for c in calls:
        c["t0"] = c["pre"]["t"]
        c["t1"] = c["post"]["t"] if "post" in c else None
        c["unknown"] = "post" not in c
        c["ret"] = c["post"].get("ret") if "post" in c else None
        c["err"] = c["post"].get("err") if "post" in c else None

    conn = sqlite3.connect(result.db_path)
    now = conn.execute("SELECT unixepoch()").fetchone()[0]
    integrity = conn.execute("PRAGMA integrity_check").fetchall()
    live = {
        r[0]: dict(zip(("id", "queue", "state", "run_at", "attempts", "max_attempts", "expires_at", "claim_expires_at", "payload"), r))
        for r in conn.execute(
            "SELECT id, queue, state, run_at, attempts, max_attempts, expires_at, claim_expires_at, payload FROM _honker_live"
        )
    }
    dead = {
        r[0]: dict(zip(("id", "attempts", "max_attempts", "last_error", "payload"), r))
        for r in conn.execute("SELECT id, attempts, max_attempts, last_error, payload FROM _honker_dead")
    }
    conn.close()

    V = []

    # Per-job facts from the ledgers.
    enq = {}  # id -> post record
    claims = defaultdict(list)  # id -> [claim dicts]
    lifecycle = defaultdict(list)  # id -> [call]
    cancels = defaultdict(list)
    for c in calls:
        op = c["op"]
        if op == "enqueue" and c.get("post") and "id" in c["post"]:
            enq[c["post"]["id"]] = c["post"]
        elif op == "claim" and c.get("post") and "jobs" in c["post"]:
            for j in c["post"]["jobs"]:
                claims[j["id"]].append(
                    {**j, "p": c["p"], "wid": c["wid"], "t0": c["t0"], "t1": c["t1"]}
                )
        elif op in ("ack", "retry", "fail", "heartbeat"):
            lifecycle[c["pre"]["id"]].append(c)
        elif op == "cancel":
            cancels[c["pre"]["id"]].append(c)

    def max_attempts_of(jid):
        if jid in enq:
            return enq[jid]["max_attempts"]
        if jid in live:
            return live[jid]["max_attempts"]
        if jid in dead:
            return dead[jid]["max_attempts"]
        return None

    def expires_of(jid):
        if jid in enq:
            return enq[jid]["expires_at"], True
        if jid in live:
            return live[jid]["expires_at"], True
        return None, False

    jobs = set(enq) | set(claims) | set(live) | set(dead)
    jobs |= {j for j, cs in cancels.items() if any(c["ret"] == 1 for c in cs)}

    # 1. Exactly one end state.
    for jid in sorted(jobs):
        ends = []
        ends += [f"ack=1 by {c['p']} (att {c['pre'].get('att')})" for c in lifecycle[jid] if c["op"] == "ack" and c["ret"] == 1]
        ends += [f"cancel=1 by {c['p']}" for c in cancels[jid] if c["ret"] == 1]
        if jid in dead:
            ends.append(f"dead ({dead[jid]['last_error']})")
        if jid in live:
            ends.append(f"live ({live[jid]['state']})")
        maybe = [c for c in lifecycle[jid] + cancels[jid] if c["unknown"] and c["op"] in ("ack", "cancel")]
        if len(ends) > 1:
            V.append(Violation("1_one_end_state", jid, "two end states: " + "; ".join(ends)))
        elif not ends and not maybe:
            V.append(Violation("1_one_end_state", jid, "job vanished: no ack, no cancel, not dead, not live"))
        if jid in live and live[jid]["state"] not in ("scheduled", "pending", "processing"):
            V.append(Violation("1_one_end_state", jid, f"live in invalid state {live[jid]['state']!r}"))

    # 2. Fencing.
    for jid, cl in claims.items():
        for c in lifecycle[jid]:
            if c["ret"] != 1:
                continue
            a1 = c["pre"]["att"]
            for later in cl:
                if later["att"] <= a1:
                    continue
                # ack/fail delete the row, and ids are never reused, so
                # any later-attempt claim must have come first.
                deleting = c["op"] in ("ack", "fail")
                if deleting or (later["t1"] is not None and later["t1"] < c["t0"]):
                    V.append(
                        Violation(
                            "2_fencing",
                            jid,
                            f"{c['op']}=1 by {c['p']} ({c['wid']}) for attempt {a1} after attempt "
                            f"{later['att']} was claimed by {later['p']} ({later['wid']})",
                        )
                    )
                    break

    # 3. No resurrection after a successful cancel.
    for jid, cs in cancels.items():
        ok = [c for c in cs if c["ret"] == 1]
        if not ok:
            continue
        tc = min(c["t1"] for c in ok)
        if jid in dead:
            V.append(Violation("3_no_resurrection", jid, f"cancelled but in _honker_dead ({dead[jid]['last_error']})"))
        if jid in live:
            V.append(Violation("3_no_resurrection", jid, f"cancelled but live ({live[jid]['state']})"))
        for cl in claims.get(jid, []):
            if cl["t0"] > tc:
                V.append(Violation("3_no_resurrection", jid, f"claimed by {cl['p']} after cancel"))
        for c in lifecycle[jid]:
            if c["ret"] == 1 and c["t0"] > tc:
                V.append(Violation("3_no_resurrection", jid, f"{c['op']}=1 by {c['p']} after cancel"))

    # 3b. A cancel that returned 0 must not have hit a live job. Rows
    # leave _honker_live once and ids are never reused, so a row seen
    # before the cancel (enqueued or claimed) and seen again after it
    # (claimed, a successful call, or live at the end) was live, in
    # pending or processing, for the whole cancel.
    for jid, cs in cancels.items():
        for c in cs:
            if c["ret"] != 0:
                continue
            before = (jid in enq and enq[jid]["t"] < c["t0"]) or any(
                cl["t1"] is not None and cl["t1"] < c["t0"] for cl in claims.get(jid, [])
            )
            after = (
                jid in live
                or any(cl["t0"] > c["t1"] for cl in claims.get(jid, []))
                or any(o["ret"] == 1 and o["t0"] > c["t1"] for o in lifecycle[jid])
            )
            if before and after:
                V.append(
                    Violation(
                        "3b_cancel_takes_effect",
                        jid,
                        f"cancel by {c['p']} returned 0 although the job was live before and after it",
                    )
                )
                break

    # 4. Attempts.
    for jid, cl in claims.items():
        ma = max_attempts_of(jid)
        for c in cl:
            if ma is not None and c["att"] > ma:
                V.append(Violation("4_attempts", jid, f"claim attempt {c['att']} > max_attempts {ma}"))
        ordered = sorted(cl, key=lambda c: c["t1"])
        for a, b in zip(ordered, ordered[1:]):
            if b["att"] <= a["att"]:
                V.append(
                    Violation("4_attempts", jid, f"attempts not increasing: {a['att']} ({a['p']}) then {b['att']} ({b['p']})")
                )

    # 5. Expiry: no claim at/after expires_at; nothing expired left live.
    for jid, cl in claims.items():
        exp, _ = expires_of(jid)
        if exp is None:
            continue
        for c in cl:
            if c["claimed_at"] >= exp:
                V.append(Violation("5_expiry", jid, f"claimed at {c['claimed_at']} >= expires_at {exp} by {c['p']}"))
    stuck_expired = set()
    for jid, row in live.items():
        if row["expires_at"] is not None and row["expires_at"] <= now:
            stuck_expired.add(jid)
            V.append(
                Violation(
                    "5_expiry",
                    jid,
                    f"expired (expires_at {row['expires_at']} <= now {now}) but still live in "
                    f"{row['state']!r} after quiesce + sweep_expired + drain",
                )
            )

    # 6. Liveness. Rows already reported as stuck-expired under 5 are
    # not double-counted here.
    for jid, row in live.items():
        if jid in stuck_expired:
            continue
        V.append(
            Violation(
                "6_liveness",
                jid,
                f"still live after drain: state={row['state']} attempts={row['attempts']}/"
                f"{row['max_attempts']} run_at={row['run_at']} now={now}",
            )
        )

    # 7. Integrity.
    if integrity != [("ok",)]:
        V.append(Violation("7_integrity", None, f"integrity_check: {integrity[:5]}"))

    # How many SIGKILLs landed mid-handler: the killed process held a
    # claim it had not finished, or died inside a call.
    by_proc = defaultdict(list)
    for r in recs:
        by_proc[r.get("p")].append(r)
    mid_handler = 0
    for tag in killed:
        open_jobs, inside_call = set(), False
        for r in by_proc.get(tag, []):
            if "k" in r:
                inside_call = r["ph"] == "pre"
                if r["ph"] == "post" and r["op"] == "claim":
                    open_jobs |= {j["id"] for j in r.get("jobs", [])}
                elif r["ph"] == "post" and r["op"] in ("ack", "retry", "fail"):
                    open_jobs.discard(r["id"])
                elif r["ph"] == "post" and r["op"] == "heartbeat" and r.get("ret") != 1:
                    open_jobs.discard(r["id"])
        if open_jobs or inside_call:
            mid_handler += 1

    # Jobs that expired in flight: after their last claim nothing ended
    # or re-queued them (no successful ack/retry/fail/cancel), and they
    # are past expires_at at the end. With the fix they are dead
    # ('expired'); a stuck one is still live (also a 5_expiry violation).
    expired_in_flight = 0
    for jid, cl in claims.items():
        last = max(c["t0"] for c in cl)
        settled = any(
            c["ret"] == 1 and c["op"] in ("ack", "retry", "fail") and c["t0"] >= last
            for c in lifecycle[jid]
        ) or any(c["ret"] == 1 for c in cancels[jid])
        if settled:
            continue
        exp, _ = expires_of(jid)
        if (jid in dead and dead[jid]["last_error"] == "expired") or (
            jid in live and exp is not None and exp <= now
        ):
            expired_in_flight += 1

    stats = {
        "jobs": len(jobs),
        "expired_in_flight": expired_in_flight,
        "abandoned": sum(1 for r in recs if r.get("ev") == "abandon"),
        "enqueued": len(enq),
        "claims": sum(len(v) for v in claims.values()),
        "acks_ok": sum(1 for c in calls if c["op"] == "ack" and c["ret"] == 1),
        "cancels_ok": sum(1 for c in calls if c["op"] == "cancel" and c["ret"] == 1),
        "dead": len(dead),
        "live_end": len(live),
        "errors": sum(1 for c in calls if c["err"]),
        "unknown_calls": sum(1 for c in calls if c["unknown"]),
        "kills": len(killed),
        "kills_mid_handler": mid_handler,
        "stale_calls": sum(1 for r in recs if r.get("ev") == "stall"),
        "lifecycle_by_op": {
            op: {
                "ok": sum(1 for c in calls if c["op"] == op and (c["ret"] == 1 or (op in ("claim", "enqueue") and c.get("post") and not c["err"]))),
                "miss": sum(1 for c in calls if c["op"] == op and c["ret"] == 0),
                "err": sum(1 for c in calls if c["op"] == op and c["err"]),
            }
            for op in ("ack", "retry", "fail", "heartbeat", "cancel", "claim", "enqueue")
        },
        "error_samples": sorted({c["err"] for c in calls if c["err"]})[:5],
    }
    lines = {j: [raw for _, raw in sorted(v)] for j, v in job_lines.items()}
    return Report(result.seed, V, stats, lines)


def main(argv):
    if argv[1] == "worker":
        worker_main(json.loads(argv[2]))
        return 0
    if argv[1] == "run":
        # python tests/lifecycle_torture.py run <seed> <seconds> <workdir>
        seed, seconds, workdir = int(argv[2]), float(argv[3]), argv[4]
        os.makedirs(workdir, exist_ok=True)
        ext = find_extension()
        shared = os.environ.get("HONKER_TORTURE_SHARED_IDS", "1") != "0"
        fenced = os.environ.get("HONKER_TORTURE_FENCED", "1") != "0"
        res = run_torture(workdir, ext, seed, seconds, shared_ids=shared, fenced=fenced)
        rep = check(res)
        print(json.dumps(rep.stats, indent=1))
        for inv in INVARIANTS:
            n = len(rep.by_invariant(inv))
            print(f"{inv}: {'ok' if n == 0 else f'{n} violation(s)'}")
            if n:
                print(rep.describe(inv, limit=2))
        return 0
    raise SystemExit(f"unknown command {argv[1]!r}")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
