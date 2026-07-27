"""Tests for Phase Mantle: schedule lifecycle (pause/resume/list/update)
and queue cancel/get_job. Same scenarios are mirrored in every binding's
own test file."""

import json
import time

import pytest

import honker
from honker import Scheduler, crontab, every_s


# ---------- schedule lifecycle ----------


def test_schedule_list_round_trips_all_fields(tmp_path):
    db = honker.open(str(tmp_path / "t.db"))
    sched = Scheduler(db)
    sched.add(name="daily-recap", queue="emails", schedule=crontab("0 9 * * *"), payload={"foo": 1}, priority=3)
    sched.add(name="hourly-sync", queue="syncs", schedule=every_s(3600))

    rows = sched.list()
    by_name = {r["name"]: r for r in rows}
    assert set(by_name) == {"daily-recap", "hourly-sync"}
    assert by_name["daily-recap"]["queue"] == "emails"
    assert by_name["daily-recap"]["priority"] == 3
    assert json.loads(by_name["daily-recap"]["payload"]) == {"foo": 1}
    assert by_name["daily-recap"]["enabled"] is True
    assert by_name["daily-recap"]["next_fire_at"] > 0


def test_schedule_pause_resume_idempotent(tmp_path):
    db = honker.open(str(tmp_path / "t.db"))
    sched = Scheduler(db)
    sched.add(name="a", queue="q", schedule=crontab("0 9 * * *"))

    assert sched.pause("a") is True
    assert sched.pause("a") is False  # already paused
    assert sched.pause("missing") is False

    paused = [s for s in sched.list() if s["name"] == "a"][0]
    assert paused["enabled"] is False

    assert sched.resume("a") is True
    assert sched.resume("a") is False  # already enabled

    enabled = [s for s in sched.list() if s["name"] == "a"][0]
    assert enabled["enabled"] is True


def test_schedule_update_mutates_fields(tmp_path):
    db = honker.open(str(tmp_path / "t.db"))
    sched = Scheduler(db)
    sched.add(name="t", queue="q", schedule=crontab("0 9 * * *"), payload={"v": 1}, priority=0)

    assert sched.update("t", payload={"v": 99}, priority=5) is True
    row = [s for s in sched.list() if s["name"] == "t"][0]
    assert json.loads(row["payload"]) == {"v": 99}
    assert row["priority"] == 5

    # Changing cron_expr recomputes next_fire_at from now.
    update_started = int(time.time())
    assert sched.update("t", schedule=every_s(1)) is True
    after = [s for s in sched.list() if s["name"] == "t"][0]
    assert after["cron_expr"] == "@every 1s"
    assert update_started + 1 <= after["next_fire_at"] <= update_started + 5

    assert sched.update("missing", payload={}) is False


def test_schedule_update_payload_null_vs_omitted(tmp_path):
    """`payload=None` writes JSON null; omitting the kwarg leaves the
    payload column alone. The _UNSET sentinel makes this distinction
    expressible from Python."""
    db = honker.open(str(tmp_path / "t.db"))
    sched = Scheduler(db)
    sched.add(name="t", queue="q", schedule=crontab("0 9 * * *"), payload={"v": 1})

    # Omitting payload leaves it untouched.
    assert sched.update("t", priority=7) is True
    row = sched.list()[0]
    assert json.loads(row["payload"]) == {"v": 1}

    # payload=None writes JSON null explicitly.
    assert sched.update("t", payload=None) is True
    row = sched.list()[0]
    assert json.loads(row["payload"]) is None


def test_schedule_update_no_fields_is_noop(tmp_path):
    """update() with no field args returns False without waking the
    leader or modifying state. Caller intent must be explicit."""
    db = honker.open(str(tmp_path / "t.db"))
    sched = Scheduler(db)
    sched.add(name="t", queue="q", schedule=crontab("0 9 * * *"), payload={"v": 1})
    before = sched.list()
    assert sched.update("t") is False
    after = sched.list()
    assert before == after, "no-op update must not mutate state"


def test_paused_schedule_does_not_emit(tmp_path):
    """Tick the scheduler with a due task that's been paused; assert no
    job lands in the queue."""
    db = honker.open(str(tmp_path / "t.db"))
    sched = Scheduler(db)
    # Schedule with next_fire_at in the past.
    sched.add(name="due", queue="emails", schedule=every_s(1), payload={"x": 1})
    time.sleep(1.1)
    sched.pause("due")

    # Manually tick by calling honker_scheduler_tick at the current time;
    # paused row should NOT enqueue anything.
    with db.transaction() as tx:
        rows = tx.query("SELECT honker_scheduler_tick(?) AS j", [int(time.time()) + 5])
    fires = json.loads(rows[0]["j"])
    assert fires == [], f"paused schedule should not emit; got {fires!r}"

    # Resume and tick again — now it should fire.
    sched.resume("due")
    with db.transaction() as tx:
        rows = tx.query("SELECT honker_scheduler_tick(?) AS j", [int(time.time()) + 5])
    fires2 = json.loads(rows[0]["j"])
    assert len(fires2) >= 1, f"resumed schedule should emit; got {fires2!r}"


# ---------- queue cancel / get_job ----------


def test_queue_get_job_returns_row(tmp_path):
    db = honker.open(str(tmp_path / "t.db"))
    q = db.queue("emails")
    with db.transaction() as tx:
        jid = q.enqueue({"to": "alice@example.com"}, tx=tx)

    row = q.get_job(jid)
    assert row is not None
    assert row["queue"] == "emails"
    assert row["state"] == "pending"
    assert json.loads(row["payload"]) == {"to": "alice@example.com"}
    assert row["id"] == jid


def test_queue_get_job_misses_after_ack(tmp_path):
    db = honker.open(str(tmp_path / "t.db"))
    q = db.queue("emails")
    with db.transaction() as tx:
        jid = q.enqueue({"to": "x"}, tx=tx)
    job = q.claim_one("worker-1")
    job.ack()
    assert q.get_job(jid) is None


def test_queue_cancel_pending(tmp_path):
    db = honker.open(str(tmp_path / "t.db"))
    q = db.queue("emails")
    with db.transaction() as tx:
        jid = q.enqueue({"to": "x"}, tx=tx)

    assert q.cancel(jid) is True
    assert q.cancel(jid) is False  # idempotent
    assert q.get_job(jid) is None
    # Worker shouldn't see it.
    assert q.claim_one("worker-1") is None


def test_queue_cancel_processing_invalidates_ack(tmp_path):
    db = honker.open(str(tmp_path / "t.db"))
    q = db.queue("emails")
    with db.transaction() as tx:
        jid = q.enqueue({"to": "x"}, tx=tx)

    job = q.claim_one("worker-1")
    assert job.id == jid
    # Operator cancels the in-flight job.
    assert q.cancel(jid) is True
    # Worker's ack now returns False — same shape as expired claim.
    assert job.ack() is False
