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
import json
import statistics
import time

import pytest

import honker


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
        f"Likely regression in the _honker_live_claim partial index "
        f"or honker_claim_batch."
    )


def test_disabled_queue_events_do_not_multiply_ack_batch_cost(db_path):
    """The disabled event path must stay close to the lean DELETE.

    Queue events originally materialized every job payload and queried
    configuration once per ack even while disabled, making a 5k ack batch
    roughly 6x slower. Compare the public UDF with its underlying DELETE so
    that regression trips independently of runner speed.
    """
    db = honker.open(db_path)
    direct_times = []
    udf_times = []

    for round_number in range(3):
        direct_q = db.queue(f"perf-ack-direct-{round_number}")
        udf_q = db.queue(f"perf-ack-udf-{round_number}")
        with db.transaction() as tx:
            for i in range(5_000):
                direct_q.enqueue({"i": i}, tx=tx)
                udf_q.enqueue({"i": i}, tx=tx)

        direct_jobs = direct_q.claim_batch("direct-worker", 5_000)
        udf_jobs = udf_q.claim_batch("udf-worker", 5_000)
        direct_ids = [job.id for job in direct_jobs]
        udf_ids = [job.id for job in udf_jobs]

        t0 = time.perf_counter()
        with db.transaction() as tx:
            tx.execute(
                "DELETE FROM _honker_live "
                "WHERE id IN (SELECT value FROM json_each(?)) "
                "AND worker_id = ? AND claim_expires_at >= unixepoch()",
                [json.dumps(direct_ids), "direct-worker"],
            )
        direct_times.append(time.perf_counter() - t0)
        assert db.query(
            "SELECT COUNT(*) AS c FROM _honker_live WHERE queue = ?",
            [direct_q.name],
        )[0]["c"] == 0

        t0 = time.perf_counter()
        assert udf_q.ack_batch(udf_ids, "udf-worker") == 5_000
        udf_times.append(time.perf_counter() - t0)

    direct_median = statistics.median(direct_times)
    udf_median = statistics.median(udf_times)
    bound = direct_median * 2.0 + 0.010
    assert udf_median < bound, (
        f"disabled queue-event ack_batch took {udf_median:.4f}s versus "
        f"{direct_median:.4f}s for the lean DELETE (bound: {bound:.4f}s). "
        "Likely repeated config reads or payload materialization."
    )


def test_enabled_queue_events_stay_amortized_at_retention_target(db_path):
    """Reaching retention must not add a trim query/delete per event.

    Compare equal transactional enqueue batches immediately before and after
    the feed reaches its target. This specifically exercises the steady-state
    path that a benchmark against an empty event feed misses.
    """
    retention_target = 2_000
    db = honker.open(db_path)
    with db.transaction() as tx:
        tx.query(
            "SELECT honker_queue_events_configure(1, ?, 0)",
            [retention_target],
        )
    # Reopen so the timed writer starts with a normal committed config cache.
    db.close()
    db = honker.open(db_path)
    queue = db.queue("perf-retention")

    t0 = time.perf_counter()
    with db.transaction() as tx:
        for i in range(retention_target):
            queue.enqueue({"phase": "fill", "i": i}, tx=tx)
    fill_seconds = time.perf_counter() - t0

    t0 = time.perf_counter()
    with db.transaction() as tx:
        for i in range(retention_target):
            queue.enqueue({"phase": "steady", "i": i}, tx=tx)
    steady_seconds = time.perf_counter() - t0

    assert steady_seconds < fill_seconds * 2.0 + 0.050, (
        f"queue events took {steady_seconds:.4f}s at the retention target "
        f"versus {fill_seconds:.4f}s while filling it. Likely trimming on "
        "every lifecycle event instead of in bounded chunks."
    )
    retained = db.query(
        "SELECT COUNT(*) AS c FROM _honker_stream "
        "WHERE topic = '_honker:queue-events:v1'"
    )[0]["c"]
    trim_interval = min(1_000, max(1, retention_target // 10))
    assert retention_target <= retained < retention_target + trim_interval


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
