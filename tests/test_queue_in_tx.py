"""First-time Queue construction inside an open transaction must fail fast.

See #63: nested schema-init used to hang forever on the single writer slot.
"""

import threading
import time

import pytest

import honker


def test_queue_first_construct_inside_open_tx_raises(db_path):
    db = honker.open(db_path)
    with db.transaction() as tx:
        with pytest.raises(RuntimeError, match="cannot construct Queue"):
            db.queue("brand_new_inside_tx")
        # Outer tx still usable after the failed construct.
        assert tx.query("SELECT 1 AS n")[0]["n"] == 1
    # Failed construct must not leave a half-built cache entry.
    assert "brand_new_inside_tx" not in db._queues
    # Retry outside the transaction works.
    q = db.queue("brand_new_inside_tx")
    q.enqueue({"ok": True})


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
    assert "hooks" not in db._outboxes


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


def test_nested_transaction_raises_not_hangs(db_path):
    """Same root cause as #63: nested transaction() on one thread deadlocks."""
    db = honker.open(db_path)
    with db.transaction() as tx:
        with pytest.raises(RuntimeError, match="nested db.transaction"):
            with db.transaction():
                pass
        assert tx.query("SELECT 1 AS n")[0]["n"] == 1
    assert db._tx_depth == 0


def test_queue_first_construct_waits_while_other_thread_holds_tx(db_path):
    """Depth is per-thread: another thread holding a write tx must not
    make first-time queue construct fail-fast — only wait for the writer.
    """
    db = honker.open(db_path)
    started = threading.Event()
    release = threading.Event()
    holder_err = []
    opener_result = {}

    def holder():
        try:
            with db.transaction() as tx:
                tx.execute("CREATE TABLE side (id INTEGER)")
                started.set()
                if not release.wait(timeout=5):
                    holder_err.append("release timeout")
        except Exception as e:
            holder_err.append(e)

    def opener():
        if not started.wait(timeout=5):
            opener_result["err"] = "started timeout"
            return
        t0 = time.time()
        try:
            q = db.queue("from_other_thread")
            opener_result["name"] = q.name
            opener_result["dt"] = time.time() - t0
        except Exception as e:
            opener_result["err"] = e

    th = threading.Thread(target=holder)
    to = threading.Thread(target=opener)
    th.start()
    to.start()
    # Opener is blocked on the writer while holder keeps the tx open.
    time.sleep(0.15)
    assert to.is_alive(), "opener should still be waiting on writer"
    assert "err" not in opener_result
    release.set()
    th.join(timeout=5)
    to.join(timeout=5)
    assert not holder_err, holder_err
    assert "err" not in opener_result, opener_result
    assert opener_result.get("name") == "from_other_thread"
    # Waited at least a slice of the hold window (not an instant false raise).
    assert opener_result.get("dt", 0) >= 0.05


def test_issue_63_repro_raises_not_hangs(db_path):
    """Exact shape from #63 — must raise, not hang."""
    db = honker.open(db_path)
    with db.transaction() as tx:
        with pytest.raises(RuntimeError, match="cannot construct Queue"):
            q = db.queue("brand_new_name")
            q.enqueue({"x": 1}, tx=tx)
