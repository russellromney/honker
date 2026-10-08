"""Performance floor tests.

These pin a loose throughput floor for hot paths so a 10x+ regression
(unindexed query, lost `prepare_cached`, extra JSON round-trip per
row) trips CI instead of shipping silently.

Thresholds are set ~3-5x below measured throughput on an M-series
laptop so they don't flake on slower CI hardware, but tight enough
that real regressions show up:

  Path                     measured (M-series)    floor
  enqueue 10k in one tx    ~21k/s                  3.3k/s
  claim_batch 10k (100/ea) ~44k/s                  3.3k/s
  100 notifies → listener  ~27k/s                  100/s

The aim is not to benchmark; run `bench/wake_latency_bench.py`
for that. These catch order-of-magnitude regressions only.
"""

import asyncio
import os
import sqlite3
import statistics
import time

import pytest

import honker

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_enqueue_throughput_floor_one_tx(db_path):
    """10,000 enqueues inside one transaction must finish in under
    3 seconds. Measured ~0.5s on M-series. A 6x slowdown trips."""
    db = honker.open(db_path)
    q = db.queue("perf-enqueue")
    t0 = time.perf_counter()
    with db.transaction() as tx:
        for i in range(10_000):
            q.enqueue({"i": i}, tx=tx)
    elapsed = time.perf_counter() - t0
    assert elapsed < 3.0, (
        f"enqueue 10k in one tx took {elapsed:.3f}s (floor: 3.0s). "
        f"Likely regression in honker_enqueue or the PyO3 param-marshaling."
    )
    # Sanity: rows actually landed.
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _honker_live WHERE queue='perf-enqueue'"
    )
    assert rows[0]["c"] == 10_000


def test_claim_batch_throughput_floor(db_path):
    """Seed 10k jobs, drain in batches of 100. Must finish in under
    3 seconds. Measured ~0.23s on M-series. A 13x slowdown trips.
    The claim path touches the partial index on every batch; if the
    index gets dropped or the planner picks a table scan, this
    floor trips."""
    db = honker.open(db_path)
    q = db.queue("perf-claim", visibility_timeout_s=300)
    with db.transaction() as tx:
        for i in range(10_000):
            q.enqueue({"i": i}, tx=tx)

    t0 = time.perf_counter()
    claimed = 0
    while True:
        jobs = q.claim_batch("w1", 100)
        if not jobs:
            break
        claimed += len(jobs)
    elapsed = time.perf_counter() - t0
    assert claimed == 10_000
    assert elapsed < 3.0, (
        f"claim_batch 10k took {elapsed:.3f}s (floor: 3.0s). "
        f"Likely regression in the _honker_live_ready partial index "
        f"or honker_claim_batch."
    )


async def test_notify_listener_receive_floor(db_path):
    """100 notifies delivered to a listener must be observed within
    1 second end-to-end. Measured ~4ms on M-series. A 250x slowdown
    trips. Catches regressions in the listener buffer, update watcher
    fanout, or the cross-thread asyncio.Queue bridge."""
    db = honker.open(db_path)

    received: list = []
    lst = db.listen("perf-notify")

    async def consume():
        async for n in lst:
            received.append(n)
            if len(received) == 100:
                return

    task = asyncio.create_task(consume())
    # Give the listener a moment to attach + read MAX(id).
    await asyncio.sleep(0.05)

    t0 = time.perf_counter()
    with db.transaction() as tx:
        for i in range(100):
            tx.notify("perf-notify", {"i": i})
    await asyncio.wait_for(task, timeout=5.0)
    elapsed = time.perf_counter() - t0

    assert elapsed < 1.0, (
        f"100 notify → listener receive took {elapsed:.3f}s "
        f"(floor: 1.0s). Likely regression in listener polling, "
        f"update watcher fanout, or the asyncio bridge."
    )
    assert len(received) == 100


# ---------------------------------------------------------------------
# Claim latency does not grow with the backlog (claim v2)
# ---------------------------------------------------------------------
#
# Through the real loadable extension and stdlib sqlite3, on a file-backed
# WAL database. Each scenario puts a backlog of N rows in the queue that a
# claim must not pay for, then times claims of one fresh due job:
#
#   due         N due jobs (each claim takes one of them)
#   inflight    N jobs held by another worker with a valid lease
#   delayed_hi  N scheduled jobs at a higher priority, due tomorrow
#   expired_hi  N expired jobs at a higher priority
#
# Measured on an M-series laptop, p50 per claim:
#
#   scenario     main 1k   main 50k   claim v2 1k   claim v2 50k
#   due          0.87 ms   16.0 ms    0.74 ms       0.73 ms
#   inflight     0.72 ms   18.8 ms    0.82 ms       0.75 ms
#   delayed_hi   0.99 ms   21.4 ms    1.06 ms       0.71 ms
#   expired_hi   0.86 ms   26.0 ms    0.84 ms       0.81 ms
#
# The floor: p50 at 50k stays under 3x p50 at 1k. On main every scenario
# is 18-30x. `expired_hi` is the one with a tail: the first N/1000 claims
# each move up to 1000 expired rows to _honker_dead (CLAIM_HOUSEKEEPING_
# LIMIT) and walk the rest, about 5-23 ms each, then it is flat. p50 is
# the right statistic for "does a claim pay for the backlog".

_EXT_CANDIDATES = [
    os.path.join(REPO_ROOT, "target", "release", name)
    for name in ("libhonker_ext.dylib", "libhonker_ext.so")
]
_EXT = os.environ.get("HONKER_EXTENSION_PATH") or next(
    (p for p in _EXT_CANDIDATES if os.path.exists(p)), None
)
_CAN_LOAD = hasattr(sqlite3.connect(":memory:"), "enable_load_extension")


def _ext_db(path):
    c = sqlite3.connect(path, isolation_level=None)
    c.enable_load_extension(True)
    c.load_extension(_EXT)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("SELECT honker_bootstrap()")
    return c


def _fill(c, n, delay=None, priority=0):
    c.execute("BEGIN IMMEDIATE")
    c.execute(
        "WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM s WHERE x < ?) "
        "SELECT count(honker_enqueue('q', '{}', NULL, ?, ?, 3, NULL)) FROM s",
        (n, delay, priority),
    ).fetchone()
    c.execute("COMMIT")


def _claim_p50_ms(tmp_path, scenario, n, reps=150):
    c = _ext_db(str(tmp_path / f"{scenario}-{n}.db"))
    if scenario == "due":
        _fill(c, n)
    elif scenario == "inflight":
        _fill(c, n)
        c.execute(
            "UPDATE _honker_live SET state = 'processing', worker_id = 'other', "
            "claim_expires_at = unixepoch() + 3600, attempts = 1"
        )
    elif scenario == "delayed_hi":
        _fill(c, n, delay=86400, priority=5)
    elif scenario == "expired_hi":
        _fill(c, n, priority=5)
        c.execute("UPDATE _honker_live SET expires_at = unixepoch() - 1")
    times = []
    for _ in range(reps):
        if scenario != "due":
            c.execute("SELECT honker_enqueue('q', '{}', NULL, NULL, 0, 3, NULL)")
        t = time.perf_counter()
        got = c.execute("SELECT honker_claim_batch('q', 'w', 1, 300)").fetchone()[0]
        times.append(time.perf_counter() - t)
        assert got != "[]", (scenario, n)
    c.close()
    return statistics.median(times) * 1e3


@pytest.mark.skipif(_EXT is None, reason="honker extension not built (target/release)")
@pytest.mark.skipif(not _CAN_LOAD, reason="this sqlite3 cannot load extensions")
@pytest.mark.parametrize("scenario", ["due", "inflight", "delayed_hi", "expired_hi"])
def test_claim_latency_is_flat_in_the_backlog(tmp_path, scenario):
    small = _claim_p50_ms(tmp_path, scenario, 1_000)
    large = _claim_p50_ms(tmp_path, scenario, 50_000)
    assert large < 3 * small, (
        f"{scenario}: claim p50 {large:.3f} ms with 50k rows vs {small:.3f} ms with 1k. "
        f"A claim is paying for rows it cannot take: check the _honker_live_ready / "
        f"_scheduled / _expiry / _processing_deadline plans in honker_claim_batch."
    )
