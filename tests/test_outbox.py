"""Tests for honker.outbox."""

import asyncio

import pytest

import honker


async def test_outbox_delivery_called_and_acked(db_path):
    db = honker.open(db_path)
    delivered = []

    async def deliver(payload):
        delivered.append(payload)

    outbox = db.outbox("webhook", delivery=deliver)
    outbox.enqueue({"url": "https://a.com", "body": "1"})
    outbox.enqueue({"url": "https://b.com", "body": "2"})

    worker = asyncio.create_task(outbox.run_worker("w1"))
    try:
        for _ in range(100):
            if len(delivered) == 2:
                break
            await asyncio.sleep(0.02)
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)

    assert [d["body"] for d in delivered] == ["1", "2"]
    # ack DELETEs the processing row, so both jobs are gone from the view.
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _honker_jobs WHERE queue=?", ["_outbox:webhook"]
    )
    assert rows[0]["c"] == 0


async def test_outbox_retries_on_exception(db_path):
    db = honker.open(db_path)
    calls: list = []

    async def deliver(payload):
        calls.append(payload)
        if len(calls) < 3:
            raise RuntimeError("transient")

    outbox = db.outbox(
        "w",
        delivery=deliver,
        max_attempts=5,
        base_backoff_s=0,
    )
    outbox.enqueue({"n": 1})

    worker = asyncio.create_task(outbox.run_worker("w1"))
    try:
        for _ in range(300):
            if len(calls) >= 3:
                break
            await asyncio.sleep(0.02)
            # base_backoff 0 means retry is immediately eligible; nudge run_at
            # on the pending table (that's where retried jobs land).
            with db.transaction() as tx:
                tx.execute(
                    "UPDATE _honker_live SET run_at=unixepoch() - 1 WHERE queue=?",
                    ["_outbox:w"],
                )
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)

    assert len(calls) >= 3
    # After the 3rd call succeeds and the job is ack'd, it's deleted.
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _honker_jobs WHERE queue=?", ["_outbox:w"]
    )
    assert rows[0]["c"] == 0


async def test_outbox_rollback_drops_enqueue(db_path):
    db = honker.open(db_path)
    delivered = []

    async def deliver(payload):
        delivered.append(payload)

    outbox = db.outbox("w", delivery=deliver)

    with pytest.raises(RuntimeError):
        with db.transaction() as tx:
            tx.execute("CREATE TABLE orders (id INTEGER)")
            tx.execute("INSERT INTO orders (id) VALUES (?)", [1])
            outbox.enqueue({"order": 1}, tx=tx)
            raise RuntimeError("business error")

    rows = db.query(
        "SELECT COUNT(*) AS c FROM _honker_jobs WHERE queue=?", ["_outbox:w"]
    )
    assert rows[0]["c"] == 0


async def test_outbox_enqueue_in_tx_atomic(db_path):
    db = honker.open(db_path)
    delivered = []

    def deliver(payload):
        delivered.append(payload)

    outbox = db.outbox("w", delivery=deliver)
    with db.transaction() as tx:
        tx.execute("CREATE TABLE orders (id INTEGER)")
        tx.execute("INSERT INTO orders (id) VALUES (?)", [1])
        outbox.enqueue({"order": 1}, tx=tx)

    worker = asyncio.create_task(outbox.run_worker("w1"))
    try:
        for _ in range(100):
            if delivered:
                break
            await asyncio.sleep(0.02)
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)

    assert delivered == [{"order": 1}]
    assert db.query("SELECT COUNT(*) AS c FROM orders")[0]["c"] == 1


def test_outbox_instance_memoized(db_path):
    db = honker.open(db_path)

    def f(_):
        pass

    a = db.outbox("w", delivery=f)
    b = db.outbox("w", delivery=f)
    assert a is b


def test_outbox_requires_callable_delivery(db_path):
    """Fail fast: non-callable delivery blows up at construction, not in worker."""
    db = honker.open(db_path)
    with pytest.raises(TypeError):
        db.outbox("w", delivery=None)
    with pytest.raises(TypeError):
        db.outbox("w2", delivery="not a function")


async def test_outbox_eventually_dies_after_max_attempts(db_path):
    db = honker.open(db_path)
    calls: list = []

    async def always_fail(payload):
        calls.append(payload)
        raise RuntimeError("permanent")

    outbox = db.outbox(
        "w", delivery=always_fail, max_attempts=3, base_backoff_s=0
    )
    outbox.enqueue({"n": 1})

    worker = asyncio.create_task(outbox.run_worker("w1"))
    try:
        for _ in range(300):
            state = db.query(
                "SELECT state FROM _honker_jobs WHERE queue=?",
                ["_outbox:w"],
            )
            if state and state[0]["state"] == "dead":
                break
            await asyncio.sleep(0.02)
            # Retried jobs live in _honker_live until claimed again.
            with db.transaction() as tx:
                tx.execute(
                    "UPDATE _honker_live SET run_at=unixepoch() - 1 WHERE queue=?",
                    ["_outbox:w"],
                )
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)

    # Final row lives in _honker_dead after max_attempts reached.
    rows = db.query(
        "SELECT state, attempts, last_error FROM _honker_jobs WHERE queue=?",
        ["_outbox:w"],
    )
    assert rows[0]["state"] == "dead"
    assert rows[0]["attempts"] == 3
    assert "permanent" in (rows[0]["last_error"] or "")
    assert len(calls) == 3


async def test_outbox_sync_delivery_function(db_path):
    """Regression: plain sync callables must work, not just coroutine functions."""
    db = honker.open(db_path)
    delivered = []

    def sync_deliver(payload):
        delivered.append(payload)

    outbox = db.outbox("sync", delivery=sync_deliver)
    outbox.enqueue({"a": 1})

    worker = asyncio.create_task(outbox.run_worker("w1"))
    try:
        for _ in range(100):
            if delivered:
                break
            await asyncio.sleep(0.02)
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)

    assert delivered == [{"a": 1}]


async def test_stuck_handler_past_visibility_timeout_is_reclaimed(db_path):
    """The core durability story of the outbox + queue: if a worker's handler
    hangs past its visibility_timeout_s, a second worker can reclaim the job,
    and the stuck worker's eventual ack fails loudly."""
    db = honker.open(db_path)

    stuck_started = asyncio.Event()
    stuck_attempted_ack: list = []
    fast_delivered: list = []

    async def stuck_handler(payload):
        stuck_started.set()
        await asyncio.sleep(4.0)  # way past visibility timeout

    async def fast_handler(payload):
        fast_delivered.append(payload)

    # Two outboxes pointing at the SAME underlying queue name — this is
    # effectively "two workers on one queue, different handlers". We use the
    # Queue directly to control the worker ids; outbox just produces.
    q = db.queue("stuck-q", visibility_timeout_s=1, max_attempts=5)
    q.enqueue({"n": 1})

    stuck_cancel = asyncio.Event()

    async def stuck_worker():
        job = q.claim_one("stuck-w")
        assert job is not None
        try:
            await stuck_handler(job.payload)
            stuck_attempted_ack.append(job.ack())
        except asyncio.CancelledError:
            stuck_attempted_ack.append(job.ack())
            raise

    async def fast_worker():
        # Wait for the stuck worker to claim first.
        await stuck_started.wait()
        # Now wait for the visibility timeout to elapse so the job is
        # reclaimable by the predicate.
        await asyncio.sleep(1.5)
        job = q.claim_one("fast-w")
        if job is None:
            # Keep trying briefly — claim predicate uses unixepoch() which is
            # second-resolution, timing can be tight.
            for _ in range(20):
                await asyncio.sleep(0.1)
                job = q.claim_one("fast-w")
                if job is not None:
                    break
        assert job is not None, "fast worker could not reclaim stuck job"
        await fast_handler(job.payload)
        assert job.ack() is True
        fast_delivered.append("acked")

    stuck_task = asyncio.create_task(stuck_worker())
    fast_task = asyncio.create_task(fast_worker())

    await asyncio.wait_for(fast_task, timeout=8.0)
    # Cancel the stuck worker; its ack should return False (claim expired).
    stuck_task.cancel()
    try:
        await stuck_task
    except asyncio.CancelledError:
        pass

    assert fast_delivered[0] == {"n": 1}
    assert fast_delivered[1] == "acked"
    # Stuck worker's ack returned False — exactly the at-least-once contract.
    assert stuck_attempted_ack == [False]

    # The job is done exactly once; ack DELETEd the processing row, so
    # there's nothing left in any table.
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _honker_jobs WHERE queue=?", ["stuck-q"]
    )
    assert rows[0]["c"] == 0


@pytest.mark.parametrize("heartbeat", [True, False])
async def test_heartbeat_prevents_reclaim_for_long_running_job(db_path, heartbeat):
    """The flip side: a legitimately long handler that heartbeats must not
    get its claim stolen.

    Leases expire at one-second resolution and a claim is stealable
    only once unixepoch() > claim_expires_at, so a 1 s lease is
    guaranteed stealable 2.5 s after the claim. The intruder waits that
    long; the no-heartbeat control proves it would have stolen."""
    db = honker.open(db_path)
    q = db.queue("long", visibility_timeout_s=1)
    q.enqueue({"n": 1})

    job = q.claim_one("long-w")
    assert job is not None

    # Simulate a 3-second handler that heartbeats every 300 ms.
    heartbeats: list = []

    async def heartbeat_loop():
        for _ in range(10):
            if heartbeat:
                heartbeats.append(job.heartbeat(extend_s=1))
            await asyncio.sleep(0.3)

    async def intruder():
        await asyncio.sleep(2.5)  # always past a 1 s lease
        stolen = q.claim_one("intruder")
        return stolen

    hb_task = asyncio.create_task(heartbeat_loop())
    int_task = asyncio.create_task(intruder())

    stolen = await asyncio.wait_for(int_task, timeout=5.0)

    hb_task.cancel()
    try:
        await hb_task
    except asyncio.CancelledError:
        pass

    if heartbeat:
        assert stolen is None, "heartbeated job was stolen"
        # All heartbeats succeeded.
        assert heartbeats and all(heartbeats)
        assert job.ack() is True
    else:
        assert stolen is not None and stolen.attempts == 2
        assert job.ack() is False
        assert stolen.ack() is True


async def test_outbox_worker_stops_cleanly_on_cancel(db_path):
    """run_worker must exit promptly on asyncio cancellation."""
    db = honker.open(db_path)

    async def deliver(payload):
        pass

    outbox = db.outbox("cancel", delivery=deliver)
    worker = asyncio.create_task(outbox.run_worker("w1"))
    await asyncio.sleep(0.05)
    worker.cancel()
    # Should raise CancelledError quickly, not after the 5s idle poll.
    start = asyncio.get_running_loop().time()
    with pytest.raises(asyncio.CancelledError):
        await worker
    elapsed = asyncio.get_running_loop().time() - start
    assert elapsed < 2.0
