"""max_attempts must bound reclaim, not only polite retry().

Regression for: a worker that claims and then dies without
ack/retry/fail used to leave the row reclaimable forever —
claim_batch only enforced max_attempts inside retry(), so each
visibility-timeout reclaim bumped attempts past the budget with
no dead-letter path.
"""

import honker


def test_reclaim_past_max_attempts_moves_to_dead(db_path):
    """After max_attempts claims, an expired claim is dead-lettered
    instead of being handed to another worker.
    """
    db = honker.open(db_path)
    q = db.queue("crashy", max_attempts=2, visibility_timeout_s=300)

    job_id = q.enqueue({"n": 1})

    # First claim: attempts → 1. Worker "dies" without ack/retry/fail.
    job1 = q.claim_one("w1")
    assert job1 is not None
    assert job1.id == job_id
    assert job1.attempts == 1
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_live SET claim_expires_at = unixepoch() - 1 WHERE id=?",
            [job_id],
        )

    # Second claim (reclaim): attempts → 2. Still within budget.
    job2 = q.claim_one("w2")
    assert job2 is not None
    assert job2.id == job_id
    assert job2.attempts == 2
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_live SET claim_expires_at = unixepoch() - 1 WHERE id=?",
            [job_id],
        )

    # Third claim attempt: attempts already == max_attempts, so the
    # reclaim path must dead-letter, not return the job.
    job3 = q.claim_one("w3")
    assert job3 is None, "exhausted job must not be reclaimable"

    live = db.query(
        "SELECT COUNT(*) AS c FROM _honker_live WHERE id=?", [job_id]
    )[0]["c"]
    dead = db.query(
        "SELECT id, last_error, attempts FROM _honker_dead WHERE id=?",
        [job_id],
    )
    assert live == 0
    assert len(dead) == 1
    assert dead[0]["attempts"] == 2
    assert dead[0]["last_error"] == "max attempts exceeded"


def test_exhausted_pending_is_not_claimable(db_path):
    """Pending rows that already sit at attempts >= max_attempts
    (e.g. after a schema change lowering max_attempts, or a direct
    SQL write) must dead-letter on claim, not re-enter processing.
    """
    db = honker.open(db_path)
    q = db.queue("stale", max_attempts=3)
    job_id = q.enqueue({"n": 1})

    # Force the row into an exhausted-but-pending shape.
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_live "
            "SET state='pending', attempts=3, max_attempts=3, "
            "    worker_id=NULL, claim_expires_at=NULL, "
            "    run_at=unixepoch() "
            "WHERE id=?",
            [job_id],
        )

    assert q.claim_one("w") is None
    dead = db.query(
        "SELECT last_error FROM _honker_dead WHERE id=?", [job_id]
    )
    assert len(dead) == 1
    assert dead[0]["last_error"] == "max attempts exceeded"


def test_in_flight_claim_still_valid_is_not_dead_lettered(db_path):
    """A processing row that still owns a valid visibility timeout
    must not be dead-lettered just because attempts == max_attempts.
    The holder can still ack.
    """
    db = honker.open(db_path)
    q = db.queue("inflight", max_attempts=1, visibility_timeout_s=300)
    job_id = q.enqueue({"n": 1})

    job = q.claim_one("owner")
    assert job is not None
    assert job.attempts == 1  # == max_attempts after the only allowed claim

    # Another worker's claim must not steal or dead-letter the in-flight job.
    assert q.claim_one("other") is None
    live = db.query(
        "SELECT state, worker_id FROM _honker_live WHERE id=?", [job_id]
    )
    assert len(live) == 1
    assert live[0]["state"] == "processing"
    assert live[0]["worker_id"] == "owner"
    assert job.ack() is True


def test_enqueue_does_not_grow_notifications_table(db_path):
    """High-throughput enqueue must not write synthetic wake rows
    into `_honker_notifications`. Workers wake on data_version from
    the live-table commit instead.
    """
    db = honker.open(db_path)
    q = db.queue("bulk")
    before = db.query(
        "SELECT COUNT(*) AS c FROM _honker_notifications"
    )[0]["c"]
    for i in range(50):
        q.enqueue({"i": i})
    after = db.query(
        "SELECT COUNT(*) AS c FROM _honker_notifications"
    )[0]["c"]
    assert after == before
    # Jobs still claimable — wake path is independent of notifications.
    job = q.claim_one("w")
    assert job is not None
    job.ack()


def test_queue_next_claim_at_ignores_exhausted_rows(db_path):
    """Deadlines for exhausted processing rows must not wake workers
    forever after the attempt budget is spent.
    """
    db = honker.open(db_path)
    q = db.queue("wake", max_attempts=1, visibility_timeout_s=300)
    job_id = q.enqueue({"n": 1})
    job = q.claim_one("w")
    assert job is not None

    # Expire the claim but leave attempts at max. next_claim_at used to
    # return claim_expires_at+1 for this row; now it must return 0.
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_live "
            "SET claim_expires_at = unixepoch() + 30 "
            "WHERE id=?",
            [job_id],
        )
        # Force attempts == max_attempts while still processing with a
        # future claim_expires_at — matches post-last-claim state.
        tx.execute(
            "UPDATE _honker_live SET attempts = max_attempts WHERE id=?",
            [job_id],
        )

    assert q._next_claim_at() == 0
