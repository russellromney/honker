"""Issue #136: a claimed `Job` and a `get_job()` snapshot must both carry
the full twelve-field job shape the core already returns, and a typed
payload must survive the round trip through the database.

Same scenarios are mirrored in every binding's own test file.
"""

import json
import time

import honker

# The twelve fields honker_claim_batch and honker_get_job both return.
# `claimed_at` is deliberately absent — it does not exist yet (#135).
JOB_FIELDS = (
    "id",
    "queue",
    "payload",
    "state",
    "priority",
    "run_at",
    "worker_id",
    "claim_expires_at",
    "attempts",
    "max_attempts",
    "created_at",
    "expires_at",
)


def test_claimed_job_and_snapshot_carry_every_field(tmp_path):
    path = str(tmp_path / "t.db")
    # Two handles on one file: `worker` claims, `reader` reads. Separate
    # Database objects mean separate connections, so the reader sees only
    # what the worker committed.
    worker = honker.open(path)
    reader = honker.open(path)
    wq = worker.queue("emails", visibility_timeout_s=120, max_attempts=5)
    rq = reader.queue("emails", visibility_timeout_s=120, max_attempts=5)

    payload = {"recipient": "alice@example.com", "template": "welcome", "version": 2}
    before = int(time.time())
    jid = wq.enqueue(payload, priority=7, expires=600)
    after = int(time.time())
    assert jid > 0

    # ---- pending snapshot -------------------------------------------
    pending = rq.get_job(jid)
    assert pending is not None
    assert set(pending) == set(JOB_FIELDS), "snapshot must carry all twelve fields"
    assert pending["id"] == jid
    assert pending["queue"] == "emails"
    assert json.loads(pending["payload"]) == payload
    assert pending["state"] == "pending"
    assert pending["priority"] == 7
    assert before <= pending["run_at"] <= after
    assert pending["worker_id"] is None
    assert pending["claim_expires_at"] is None
    assert pending["attempts"] == 0
    assert pending["max_attempts"] == 5
    assert before <= pending["created_at"] <= after
    # enqueue derives run_at and expires_at from one unixepoch() read,
    # so the gap is exactly the requested TTL.
    assert pending["expires_at"] - pending["run_at"] == 600

    # ---- claimed job -------------------------------------------------
    claim_before = int(time.time())
    job = wq.claim_one("worker-py")
    claim_after = int(time.time())
    assert job is not None

    assert job.id == jid
    assert job.queue_name == "emails"
    assert job.payload == payload
    assert job.state == "processing"
    assert job.priority == 7
    assert job.run_at == pending["run_at"]
    assert job.worker_id == "worker-py"
    assert claim_before + 120 <= job.claim_expires_at <= claim_after + 120
    assert job.attempts == 1
    assert job.max_attempts == 5
    assert job.created_at == pending["created_at"]
    assert job.expires_at == pending["expires_at"]

    # ---- the reader sees the processing details ----------------------
    processing = rq.get_job(jid)
    assert processing is not None
    assert processing["state"] == "processing"
    assert processing["worker_id"] == "worker-py"
    assert processing["claim_expires_at"] == job.claim_expires_at
    assert processing["attempts"] == 1

    # ---- after ack the reader gets nothing ---------------------------
    assert job.ack() is True
    assert rq.get_job(jid) is None


def test_delayed_job_reports_its_run_at(tmp_path):
    db = honker.open(str(tmp_path / "t.db"))
    q = db.queue("delayed")

    before = int(time.time())
    jid = q.enqueue({"to": "later"}, delay=3600)
    after = int(time.time())

    row = q.get_job(jid)
    assert row is not None
    assert row["state"] == "pending"
    assert before + 3600 <= row["run_at"] <= after + 3600
    # Not claimable until run_at.
    assert q.claim_one("worker-py") is None


def test_job_without_expiry_reports_none(tmp_path):
    db = honker.open(str(tmp_path / "t.db"))
    q = db.queue("plain")
    jid = q.enqueue({"to": "x"})

    assert q.get_job(jid)["expires_at"] is None
    job = q.claim_one("worker-py")
    assert job.expires_at is None


def test_retry_moves_the_job_back_to_pending_with_the_new_run_at(tmp_path):
    """A second claim reports the state the first claim's retry wrote."""
    db = honker.open(str(tmp_path / "t.db"))
    q = db.queue("emails", max_attempts=5)
    jid = q.enqueue({"to": "x"}, priority=3)

    first = q.claim_one("worker-a")
    assert first.attempts == 1
    assert first.retry(delay_s=0, error="boom") is True

    row = q.get_job(jid)
    assert row["state"] == "pending"
    assert row["attempts"] == 1
    assert row["worker_id"] is None

    second = q.claim_one("worker-b")
    assert second is not None
    assert second.attempts == 2
    assert second.priority == 3
    assert second.max_attempts == 5
    assert second.worker_id == "worker-b"
    assert second.created_at == first.created_at


def test_typed_payload_hints_do_not_validate_at_runtime(tmp_path):
    """`Queue[T]` / `Job[T]` are compile-time hints only.

    Honker does not check payload shape in the database, so a payload
    that does not match the annotation still enqueues and claims fine.
    This test pins that documented behavior: it must NOT start failing
    because someone added runtime schema checking.
    """
    from typing import TypedDict

    class EmailPayload(TypedDict):
        recipient: str

    db = honker.open(str(tmp_path / "t.db"))
    q: honker.Queue[EmailPayload] = db.queue("emails")

    jid = q.enqueue({"totally": "different", "shape": [1, 2, 3]})
    job = q.claim_one("worker-py")
    assert job.id == jid
    assert job.payload == {"totally": "different", "shape": [1, 2, 3]}
    assert job.ack() is True
