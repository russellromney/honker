"""First-time Queue construction inside an open transaction must fail fast.

See #63: nested schema-init used to hang forever on the single writer slot.
"""

import pytest

import honker


def test_queue_first_construct_inside_open_tx_raises(db_path):
    db = honker.open(db_path)
    with db.transaction() as tx:
        with pytest.raises(RuntimeError, match="cannot construct Queue"):
            db.queue("brand_new_inside_tx")
        # Outer tx still usable after the failed construct.
        assert tx.query("SELECT 1 AS n")[0]["n"] == 1


def test_queue_construct_outside_then_enqueue_in_tx(db_path):
    db = honker.open(db_path)
    q = db.queue("emails")
    with db.transaction() as tx:
        q.enqueue({"to": "a@example.com"}, tx=tx)
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _honker_live WHERE queue=?", ["emails"]
    )
    assert rows[0]["c"] == 1


def test_queue_cached_reopen_inside_tx_ok(db_path):
    db = honker.open(db_path)
    q1 = db.queue("emails")
    with db.transaction() as tx:
        q2 = db.queue("emails")
        assert q2 is q1
        q2.enqueue({"to": "b@example.com"}, tx=tx)
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _honker_live WHERE queue=?", ["emails"]
    )
    assert rows[0]["c"] == 1


def test_stream_construct_inside_tx_ok(db_path):
    db = honker.open(db_path)
    with db.transaction() as tx:
        s = db.stream("events")
        s.publish({"ok": True}, tx=tx)
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _honker_stream WHERE topic=?", ["events"]
    )
    assert rows[0]["c"] == 1


def test_outbox_first_construct_inside_open_tx_raises(db_path):
    db = honker.open(db_path)

    def delivery(_payload):
        pass

    with db.transaction():
        with pytest.raises(RuntimeError, match="cannot construct Queue"):
            db.outbox("hooks", delivery=delivery)


def test_tx_depth_restored_after_exception(db_path):
    db = honker.open(db_path)
    try:
        with db.transaction():
            raise ValueError("boom")
    except ValueError:
        pass
    assert db._tx_depth == 0
    # Construction works again after the failed outer tx.
    q = db.queue("after_exc")
    q.enqueue({"n": 1})
