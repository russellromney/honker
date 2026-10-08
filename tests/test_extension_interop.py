"""Cross-binding interop: the SQLite loadable extension and the Python
binding must agree on schema and ack semantics. Root-caused an earlier
bug where the extension had a 6-column ``_honker_dead`` while Python
expected 10, and where ``honker_ack_batch`` UPDATEd rows to state='done'
while Python DELETEd them. Both now share
``honker-core::bootstrap_honker_schema`` and both DELETE on ack.
"""

import json
import os
import sqlite3
import subprocess
import sys
import time

import pytest

import honker


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Extension filename differs by platform — look for whichever exists.
_CANDIDATES = [
    os.path.join(REPO_ROOT, "target", "release", "libhonker_ext.dylib"),
    os.path.join(REPO_ROOT, "target", "release", "libhonker_ext.so"),
]
# HONKER_EXTENSION_PATH wins, as in tests/lifecycle_torture.py, so a
# build in a separate CARGO_TARGET_DIR can be tested.
_EXT_PATH = os.environ.get("HONKER_EXTENSION_PATH") or next(
    (p for p in _CANDIDATES if os.path.exists(p)), None
)
if _EXT_PATH is not None and not os.path.exists(_EXT_PATH):
    _EXT_PATH = None

_SKIP_REASON = (
    "honker-extension .dylib/.so not found under target/release — "
    "run `cargo build -p honker-extension --release` first"
)

# Some Python distributions (notably the python.org macOS builds used
# by actions/setup-python) compile stdlib sqlite3 without
# SQLITE_ENABLE_LOAD_EXTENSION, so `conn.enable_load_extension` is
# missing. Skip the interop tests in that case — the Python binding
# itself uses rusqlite under the hood and is fine; only this
# cross-binding test needs extension loading via stdlib sqlite3.
_HAS_LOAD_EXT = hasattr(sqlite3.connect(":memory:"), "enable_load_extension")
_LOAD_EXT_SKIP = (
    "Python's stdlib sqlite3 is compiled without "
    "SQLITE_ENABLE_LOAD_EXTENSION on this runner"
)

_SKIP = (_EXT_PATH is None) or (not _HAS_LOAD_EXT)
_SKIP_REASON = _SKIP_REASON if _EXT_PATH is None else _LOAD_EXT_SKIP


@pytest.fixture
def ext_db_path(tmp_path):
    return str(tmp_path / "interop.db")


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_bootstrap_succeeds_on_delete_connection(ext_db_path):
    """`honker_bootstrap` works before callers opt into WAL mode.
    The update watcher uses `PRAGMA data_version`, so DELETE-mode
    databases are still observable."""
    conn = sqlite3.connect(ext_db_path)
    conn.enable_load_extension(True)
    conn.load_extension(_EXT_PATH)
    # PRAGMA journal_mode returns the (new) mode; .fetchone() forces
    # the statement to actually execute on some stdlib sqlite3 builds.
    mode = conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
    if mode != "delete":
        pytest.skip(
            f"could not set journal_mode=DELETE on this sqlite3 build "
            f"(got {mode!r}); test-invariant precondition failed"
        )
    conn.execute("SELECT honker_bootstrap()")
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_bootstrap_succeeds_on_wal_connection(ext_db_path):
    """The positive counterpart: once WAL is set, `honker_bootstrap`
    installs the schema as normal. Regression guard in case the WAL
    check becomes over-eager. Phase Shakedown (d)."""
    conn = sqlite3.connect(ext_db_path)
    conn.enable_load_extension(True)
    conn.load_extension(_EXT_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("SELECT honker_bootstrap()")
    # Bootstrap idempotent — second call must also succeed.
    conn.execute("SELECT honker_bootstrap()")
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name LIKE '_honker_%' ORDER BY name"
    ).fetchall()
    names = [r[0] for r in rows]
    assert "_honker_live" in names
    assert "_honker_dead" in names
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_and_python_share_schema(ext_db_path):
    """``honker_bootstrap`` + Python's ``_init_schema`` must produce a
    schema Python can operate on without errors. The earlier extension
    had a 6-column ``_honker_dead`` and Python's ``fail()`` tripped
    on 'no column named priority'.
    """
    # Bootstrap via the extension, then open from Python honker.
    conn = sqlite3.connect(ext_db_path)
    conn.enable_load_extension(True)
    conn.load_extension(_EXT_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("SELECT honker_bootstrap()")
    conn.close()

    # Python side should be able to run through the full failure path
    # (which writes into _honker_dead) without schema errors.
    db = honker.open(ext_db_path)
    q = db.queue("interop", max_attempts=1)
    q.enqueue({"kind": "interop"})

    worker = "py-worker"
    job = q.claim_one(worker)
    assert job is not None

    # Explicit fail → INSERT into _honker_dead with 10 cols. Pre-fix
    # this raised "no column named priority".
    assert q.fail(job.id, worker, "forced failure for schema check")

    dead = db.query(
        "SELECT id, queue, payload, priority, run_at, max_attempts, "
        "attempts, last_error, created_at, died_at "
        "FROM _honker_dead"
    )
    assert len(dead) == 1
    assert dead[0]["queue"] == "interop"
    assert dead[0]["last_error"] == "forced failure for schema check"


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_honker_ack_deletes_row(ext_db_path):
    """``honker_ack_batch`` must DELETE the row (match Python ``Queue.ack``)
    rather than UPDATE ``state='done'`` and leave it in ``_honker_live``
    forever.
    """
    # Enqueue via Python honker.
    db = honker.open(ext_db_path)
    q = db.queue("ext-ack")
    q.enqueue({"i": 1})
    q.enqueue({"i": 2})
    q.enqueue({"i": 3})

    # Claim + ack via the extension.
    conn = sqlite3.connect(ext_db_path)
    conn.enable_load_extension(True)
    conn.load_extension(_EXT_PATH)

    rows_json = conn.execute(
        "SELECT honker_claim_batch('ext-ack', 'ext-worker', 10, 300)"
    ).fetchone()[0]
    claimed = json.loads(rows_json)
    assert len(claimed) == 3
    claimed_ids = [r["id"] for r in claimed]

    # Ack all three via the extension — must DELETE, not UPDATE.
    acked = conn.execute(
        "SELECT honker_ack_batch(?, 'ext-worker')",
        [json.dumps(claimed_ids)],
    ).fetchone()[0]
    assert acked == 3
    conn.close()

    # _honker_live must be empty — no state='done' residue.
    remaining = db.query(
        "SELECT COUNT(*) AS c FROM _honker_live WHERE queue = 'ext-ack'"
    )[0]["c"]
    assert remaining == 0, (
        f"extension-acked rows left behind in _honker_live (count={remaining}); "
        f"honker_ack_batch must DELETE, not UPDATE"
    )


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_registers_notify_function(ext_db_path):
    """Loading the extension must also register ``notify()`` + the
    ``_honker_notifications`` table. The extension's docstring has
    always advertised this; earlier builds didn't actually install it.
    """
    conn = sqlite3.connect(ext_db_path)
    conn.enable_load_extension(True)
    conn.load_extension(_EXT_PATH)

    conn.execute("BEGIN IMMEDIATE")
    row = conn.execute("SELECT notify('orders', '\"hello\"')").fetchone()
    assert row[0] >= 1  # returned inserted id
    conn.execute("COMMIT")

    count = conn.execute(
        "SELECT COUNT(*) FROM _honker_notifications WHERE channel='orders'"
    ).fetchone()[0]
    assert count == 1

    conn.close()


def _open_ext(path: str):
    """Open an sqlite3 conn with the extension loaded and the schema
    bootstrapped. Helper for the interop tests below.

    Sets `journal_mode=WAL` before bootstrap because `honker_bootstrap`
    now asserts WAL (Phase Shakedown (d)) — without WAL, cross-process
    wakes can't fire and the honker tables silently become useless."""
    conn = sqlite3.connect(path)
    conn.enable_load_extension(True)
    conn.load_extension(_EXT_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("SELECT honker_bootstrap()")
    conn.commit()
    return conn


# ---------- honker_sweep_expired ----------


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_sweep_expired_moves_to_dead(ext_db_path):
    """`honker_sweep_expired(queue)` must produce the same result as
    Python's `Queue.sweep_expired()`: delete pending rows whose
    `expires_at <= unixepoch()`, insert them into `_honker_dead`
    with `last_error='expired'`.
    """
    # Seed via Python honker so we know the enqueue path is honest.
    db = honker.open(ext_db_path)
    q = db.queue("exp-ext")
    q.enqueue({"i": 1}, expires=-1)
    q.enqueue({"i": 2}, expires=-1)
    q.enqueue({"i": 3}, expires=3600)  # live

    # Sweep via extension.
    conn = _open_ext(ext_db_path)
    moved = conn.execute(
        "SELECT honker_sweep_expired('exp-ext')"
    ).fetchone()[0]
    conn.commit()
    conn.close()

    assert moved == 2
    # Python can read the same state.
    live = db.query(
        "SELECT COUNT(*) AS c FROM _honker_live WHERE queue='exp-ext'"
    )[0]["c"]
    assert live == 1
    dead = db.query(
        "SELECT payload, last_error FROM _honker_dead "
        "WHERE queue='exp-ext' ORDER BY id"
    )
    assert len(dead) == 2
    assert all(r["last_error"] == "expired" for r in dead)


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_sweep_expired_is_idempotent(ext_db_path):
    db = honker.open(ext_db_path)
    q = db.queue("exp-idem")
    q.enqueue({"i": 1}, expires=-1)

    conn = _open_ext(ext_db_path)
    assert conn.execute("SELECT honker_sweep_expired('exp-idem')").fetchone()[0] == 1
    assert conn.execute("SELECT honker_sweep_expired('exp-idem')").fetchone()[0] == 0
    conn.commit()
    conn.close()


# ---------- honker_lock_acquire / honker_lock_release ----------


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_lock_acquire_release(ext_db_path):
    """`honker_lock_acquire` returns 1 on first acquire, 0 if held by
    another owner, 1 again after release. Mirrors Python's `_Lock`.
    """
    conn = _open_ext(ext_db_path)
    assert conn.execute(
        "SELECT honker_lock_acquire('backup', 'alice', 60)"
    ).fetchone()[0] == 1

    # Second acquire from a different owner must fail.
    assert conn.execute(
        "SELECT honker_lock_acquire('backup', 'bob', 60)"
    ).fetchone()[0] == 0

    # Release — only alice can.
    assert conn.execute(
        "SELECT honker_lock_release('backup', 'bob')"
    ).fetchone()[0] == 0
    assert conn.execute(
        "SELECT honker_lock_release('backup', 'alice')"
    ).fetchone()[0] == 1
    conn.commit()

    # After release, bob can acquire.
    assert conn.execute(
        "SELECT honker_lock_acquire('backup', 'bob', 60)"
    ).fetchone()[0] == 1
    conn.commit()
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_lock_prunes_stale(ext_db_path):
    """If a holder's TTL has elapsed, a new acquirer gets the lock."""
    conn = _open_ext(ext_db_path)
    # Insert a row that expired an hour ago — simulates a crashed holder.
    conn.execute(
        "INSERT INTO _honker_locks (name, owner, expires_at) "
        "VALUES ('stale', 'crashed', unixepoch() - 3600)"
    )
    conn.commit()

    assert conn.execute(
        "SELECT honker_lock_acquire('stale', 'fresh', 60)"
    ).fetchone()[0] == 1
    conn.commit()
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_lock_interops_with_python(ext_db_path):
    """Python's `db.lock()` and extension `honker_lock_acquire` use the
    same table. Acquiring from one side blocks the other."""
    db = honker.open(ext_db_path)
    conn = _open_ext(ext_db_path)

    # Python holds the lock...
    with db.lock("shared", ttl=60):
        # ...extension fails to acquire.
        assert conn.execute(
            "SELECT honker_lock_acquire('shared', 'ext-side', 60)"
        ).fetchone()[0] == 0

    conn.commit()
    # Python released. Extension can now grab it.
    assert conn.execute(
        "SELECT honker_lock_acquire('shared', 'ext-side', 60)"
    ).fetchone()[0] == 1
    conn.commit()
    conn.close()


# ---------- honker_rate_limit_try / honker_rate_limit_sweep ----------


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_rate_limit_try(ext_db_path):
    conn = _open_ext(ext_db_path)
    for _ in range(3):
        assert conn.execute(
            "SELECT honker_rate_limit_try('api', 3, 60)"
        ).fetchone()[0] == 1
    # Over limit.
    for _ in range(5):
        assert conn.execute(
            "SELECT honker_rate_limit_try('api', 3, 60)"
        ).fetchone()[0] == 0
    # Count capped at 3 (rejected calls not counted).
    conn.commit()
    count = conn.execute(
        "SELECT count FROM _honker_rate_limits WHERE name='api'"
    ).fetchone()[0]
    assert count == 3
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_rate_limit_interops_with_python(ext_db_path):
    """Extension-side `honker_rate_limit_try` and Python's
    `db.try_rate_limit` share the same table and window."""
    db = honker.open(ext_db_path)
    conn = _open_ext(ext_db_path)

    # Use up 2 of 3 on the Python side.
    assert db.try_rate_limit("cross", limit=3, per=60) is True
    assert db.try_rate_limit("cross", limit=3, per=60) is True

    # Extension sees 1 slot left.
    assert conn.execute(
        "SELECT honker_rate_limit_try('cross', 3, 60)"
    ).fetchone()[0] == 1
    conn.commit()

    # Fourth call from either side is rejected.
    assert db.try_rate_limit("cross", limit=3, per=60) is False
    assert conn.execute(
        "SELECT honker_rate_limit_try('cross', 3, 60)"
    ).fetchone()[0] == 0
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_rate_limit_sweep(ext_db_path):
    conn = _open_ext(ext_db_path)
    conn.execute(
        "INSERT INTO _honker_rate_limits (name, window_start, count) "
        "VALUES ('old', unixepoch() - 100000, 10)"
    )
    conn.execute("SELECT honker_rate_limit_try('fresh', 10, 60)")
    conn.commit()

    deleted = conn.execute("SELECT honker_rate_limit_sweep(3600)").fetchone()[0]
    conn.commit()
    assert deleted == 1
    remaining = conn.execute(
        "SELECT name FROM _honker_rate_limits"
    ).fetchall()
    assert [r[0] for r in remaining] == ["fresh"]
    conn.close()


# ---------- honker_scheduler_register / honker_scheduler_tick / _unregister ----------


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_scheduler_register_and_tick(ext_db_path):
    """`honker_scheduler_register` inserts a task row; `honker_scheduler_tick`
    fires due boundaries and advances `next_fire_at`."""
    conn = _open_ext(ext_db_path)
    # Install the schema (bootstrap_honker runs on first register
    # through bootstrap — but the ext path bootstraps lazily when
    # honker.open() is called; for a pure-ext session we call the
    # bootstrap scalar explicitly).
    conn.execute("SELECT honker_bootstrap()")
    conn.execute(
        "SELECT honker_scheduler_register('nightly', 'backups', '0 3 * * *', "
        "'\"go\"', 0, NULL)"
    )
    conn.commit()
    row = conn.execute(
        "SELECT queue, cron_expr, payload, next_fire_at "
        "FROM _honker_scheduler_tasks WHERE name='nightly'"
    ).fetchone()
    assert row[0] == "backups"
    assert row[1] == "0 3 * * *"
    assert row[2] == '"go"'
    boundary = int(row[3])

    # Tick one second past the boundary — fires once, advances 24h.
    fires_json = conn.execute(
        "SELECT honker_scheduler_tick(?)", (boundary + 1,)
    ).fetchone()[0]
    conn.commit()
    import json as _json
    fires = _json.loads(fires_json)
    assert len(fires) == 1
    assert fires[0]["name"] == "nightly"
    assert fires[0]["queue"] == "backups"
    assert fires[0]["fire_at"] == boundary
    assert conn.execute(
        "SELECT COUNT(*) FROM _honker_live WHERE queue='backups'"
    ).fetchone()[0] == 1
    new_next = int(conn.execute(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='nightly'"
    ).fetchone()[0])
    assert new_next == boundary + 86400

    # Unregister: row drops.
    deleted = conn.execute(
        "SELECT honker_scheduler_unregister('nightly')"
    ).fetchone()[0]
    conn.commit()
    assert deleted == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM _honker_scheduler_tasks"
    ).fetchone()[0] == 0
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_scheduler_interops_with_python(ext_db_path):
    """Python `Scheduler.add` and the extension's
    `_honker_scheduler_tasks` write to the same table — both sides
    agree on the cron expression and next_fire_at."""
    db = honker.open(ext_db_path)
    from honker import Scheduler, crontab

    sched = Scheduler(db)
    sched.add(name="t", queue="q", schedule=crontab("0 3 * * *"))

    conn = _open_ext(ext_db_path)
    row = conn.execute(
        "SELECT queue, cron_expr FROM _honker_scheduler_tasks WHERE name='t'"
    ).fetchone()
    assert row == ("q", "0 3 * * *")
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_scheduler_interval_expression(ext_db_path):
    conn = _open_ext(ext_db_path)
    conn.execute("SELECT honker_bootstrap()")
    conn.execute(
        "SELECT honker_scheduler_register('fast', 'backups', '@every 1s', "
        "'\"go\"', 0, NULL)"
    )
    conn.commit()

    row = conn.execute(
        "SELECT queue, cron_expr, next_fire_at "
        "FROM _honker_scheduler_tasks WHERE name='fast'"
    ).fetchone()
    assert row[0] == "backups"
    assert row[1] == "@every 1s"
    boundary = int(row[2])

    fires_json = conn.execute(
        "SELECT honker_scheduler_tick(?)", (boundary,)
    ).fetchone()[0]
    conn.commit()
    import json as _json
    fires = _json.loads(fires_json)
    assert len(fires) == 1
    assert fires[0]["fire_at"] == boundary

    new_next = int(conn.execute(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='fast'"
    ).fetchone()[0])
    assert new_next == boundary + 1
    conn.close()


# ---------- honker_result_save / honker_result_get / honker_result_sweep ----------


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_result_save_and_get(ext_db_path):
    conn = _open_ext(ext_db_path)
    # ttl=0 means no expiration.
    conn.execute("SELECT honker_result_save(1, '{\"ok\":true}', 0)")
    conn.commit()

    row = conn.execute("SELECT honker_result_get(1)").fetchone()[0]
    assert row == '{"ok":true}'

    # Missing id returns NULL.
    row = conn.execute("SELECT honker_result_get(999)").fetchone()[0]
    assert row is None
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_result_save_upserts(ext_db_path):
    """Second save for the same id replaces the first."""
    conn = _open_ext(ext_db_path)
    conn.execute("SELECT honker_result_save(42, '\"first\"', 0)")
    conn.execute("SELECT honker_result_save(42, '\"second\"', 0)")
    conn.commit()

    row = conn.execute("SELECT honker_result_get(42)").fetchone()[0]
    assert row == '"second"'
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_result_get_filters_expired(ext_db_path):
    """Extension get() returns NULL for a row whose expires_at has
    passed (same filter semantics as Python's `get_result`)."""
    conn = _open_ext(ext_db_path)
    conn.execute(
        "INSERT INTO _honker_results (job_id, value, expires_at) "
        "VALUES (7, '\"stale\"', unixepoch() - 10)"
    )
    conn.commit()

    assert conn.execute("SELECT honker_result_get(7)").fetchone()[0] is None
    # Row still present until sweep.
    assert conn.execute(
        "SELECT COUNT(*) FROM _honker_results"
    ).fetchone()[0] == 1
    assert conn.execute("SELECT honker_result_sweep()").fetchone()[0] == 1
    conn.commit()
    assert conn.execute(
        "SELECT COUNT(*) FROM _honker_results"
    ).fetchone()[0] == 0
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_result_ttl_absolute(ext_db_path):
    """ttl_s>0 is interpreted as seconds-from-now; the extension
    stores `unixepoch() + ttl_s` as expires_at."""
    conn = _open_ext(ext_db_path)
    conn.execute("SELECT honker_result_save(1, '\"x\"', 3600)")
    conn.commit()

    exp = conn.execute(
        "SELECT expires_at FROM _honker_results WHERE job_id=1"
    ).fetchone()[0]
    now = conn.execute("SELECT unixepoch()").fetchone()[0]
    assert 3598 <= exp - now <= 3602
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_result_interops_with_python(ext_db_path):
    """Extension-side save is readable from Python and vice versa —
    one `_honker_results` table."""
    db = honker.open(ext_db_path)
    q = db.queue("interop-results")

    conn = _open_ext(ext_db_path)
    conn.execute("SELECT honker_result_save(100, '{\"from\":\"ext\"}', 0)")
    conn.commit()

    # Python reads extension's write.
    found, value = q.get_result(100)
    assert found and value == {"from": "ext"}

    # Python writes, extension reads.
    q.save_result(200, {"from": "py"})
    row = conn.execute("SELECT honker_result_get(200)").fetchone()[0]
    assert row == '{"from": "py"}'
    conn.close()


# ---------- honker_enqueue / honker_ack / honker_retry / honker_fail / honker_heartbeat ----------


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_enqueue_returns_id_without_wake_row(ext_db_path):
    """`honker_enqueue` INSERTs a live row and returns its id.

    It must NOT write a synthetic `_honker_notifications` wake row —
    the live INSERT advances data_version on commit, which is what
    update watchers observe. Per-enqueue wake rows used to grow the
    notifications table without bound.
    """
    conn = _open_ext(ext_db_path)
    # Seven args: queue, payload, run_at_or_null, delay_or_null,
    # priority, max_attempts, expires_or_null.
    rid = conn.execute(
        "SELECT honker_enqueue('q', '{\"x\":1}', NULL, NULL, 0, 3, NULL)"
    ).fetchone()[0]
    conn.commit()
    assert isinstance(rid, int) and rid > 0

    # Row landed.
    row = conn.execute(
        "SELECT id, queue, payload, state FROM _honker_live"
    ).fetchone()
    assert row == (rid, "q", '{"x":1}', "pending")

    # No synthetic wake row on honker:<queue>.
    notif = conn.execute(
        "SELECT COUNT(*) FROM _honker_notifications WHERE channel='honker:q'"
    ).fetchone()[0]
    assert notif == 0
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_enqueue_delay_overrides_run_at(ext_db_path):
    """Delay wins over run_at per the documented precedence."""
    conn = _open_ext(ext_db_path)
    # delay=60 should produce run_at = now+60, ignoring the literal
    # run_at=1000.
    conn.execute(
        "SELECT honker_enqueue('q', '{}', 1000, 60, 0, 3, NULL)"
    )
    conn.commit()
    ra = conn.execute("SELECT run_at FROM _honker_live").fetchone()[0]
    now = conn.execute("SELECT unixepoch()").fetchone()[0]
    assert 58 <= ra - now <= 62
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_enqueue_expires_sets_absolute(ext_db_path):
    """expires=60 → expires_at = now + 60."""
    conn = _open_ext(ext_db_path)
    conn.execute(
        "SELECT honker_enqueue('q', '{}', NULL, NULL, 0, 3, 60)"
    )
    conn.commit()
    exp = conn.execute("SELECT expires_at FROM _honker_live").fetchone()[0]
    now = conn.execute("SELECT unixepoch()").fetchone()[0]
    assert 58 <= exp - now <= 62
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_ack_singular(ext_db_path):
    """honker_ack(job_id, worker_id) DELETEs and returns 1 on success,
    0 if the claim isn't ours."""
    conn = _open_ext(ext_db_path)
    conn.execute("SELECT honker_enqueue('q', '{}', NULL, NULL, 0, 3, NULL)")
    conn.commit()
    claimed = conn.execute(
        "SELECT honker_claim_batch('q', 'w1', 1, 300)"
    ).fetchone()[0]
    conn.commit()
    rid = json.loads(claimed)[0]["id"]

    # Wrong worker — 0.
    assert conn.execute(
        "SELECT honker_ack(?, 'w2')", [rid]
    ).fetchone()[0] == 0
    # Right worker — 1.
    assert conn.execute(
        "SELECT honker_ack(?, 'w1')", [rid]
    ).fetchone()[0] == 1
    conn.commit()
    # Row gone.
    assert conn.execute(
        "SELECT COUNT(*) FROM _honker_live"
    ).fetchone()[0] == 0
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_retry_flips_back_without_wake_row(ext_db_path):
    """honker_retry with a delay puts the claim back as 'scheduled' with
    run_at pushed (a delay of 0 would write 'pending'). No synthetic
    notification row — wake is data_version from the UPDATE.
    """
    conn = _open_ext(ext_db_path)
    conn.execute("SELECT honker_enqueue('rq', '{}', NULL, NULL, 0, 5, NULL)")
    conn.commit()
    claimed = conn.execute(
        "SELECT honker_claim_batch('rq', 'w1', 1, 300)"
    ).fetchone()[0]
    conn.commit()
    rid = json.loads(claimed)[0]["id"]

    conn.execute("DELETE FROM _honker_notifications")
    conn.commit()

    result = conn.execute(
        "SELECT honker_retry(?, 'w1', 60, 'transient')", [rid]
    ).fetchone()[0]
    conn.commit()
    assert result == 1

    row = conn.execute(
        "SELECT state, run_at, worker_id, attempts FROM _honker_live"
    ).fetchone()
    state, ra, wid, attempts = row
    assert state == "scheduled"
    assert wid is None
    assert attempts == 1  # incremented during claim; not decremented
    now = conn.execute("SELECT unixepoch()").fetchone()[0]
    assert 58 <= ra - now <= 62

    notif = conn.execute(
        "SELECT COUNT(*) FROM _honker_notifications"
    ).fetchone()[0]
    assert notif == 0
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_retry_exhausted_moves_to_dead(ext_db_path):
    """If attempts >= max_attempts at retry time, honker_retry moves the
    row to _honker_dead with last_error set, instead of flipping
    back to pending."""
    conn = _open_ext(ext_db_path)
    # max_attempts=1 so first attempt exhausts.
    conn.execute("SELECT honker_enqueue('fq', '{}', NULL, NULL, 0, 1, NULL)")
    conn.commit()
    claimed = conn.execute(
        "SELECT honker_claim_batch('fq', 'w1', 1, 300)"
    ).fetchone()[0]
    conn.commit()
    rid = json.loads(claimed)[0]["id"]

    assert conn.execute(
        "SELECT honker_retry(?, 'w1', 60, 'gave up')", [rid]
    ).fetchone()[0] == 1
    conn.commit()

    # Moved to dead.
    dead = conn.execute(
        "SELECT id, last_error FROM _honker_dead"
    ).fetchone()
    assert dead == (rid, "gave up")
    assert conn.execute(
        "SELECT COUNT(*) FROM _honker_live"
    ).fetchone()[0] == 0
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_fail_unconditional(ext_db_path):
    """honker_fail always moves the claim to dead regardless of
    attempts vs max_attempts."""
    conn = _open_ext(ext_db_path)
    conn.execute("SELECT honker_enqueue('x', '{}', NULL, NULL, 0, 99, NULL)")
    conn.commit()
    claimed = conn.execute(
        "SELECT honker_claim_batch('x', 'w', 1, 300)"
    ).fetchone()[0]
    conn.commit()
    rid = json.loads(claimed)[0]["id"]

    assert conn.execute(
        "SELECT honker_fail(?, 'w', 'explicit')", [rid]
    ).fetchone()[0] == 1
    conn.commit()
    dead = conn.execute(
        "SELECT last_error FROM _honker_dead"
    ).fetchone()
    assert dead == ("explicit",)
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_heartbeat_extends_claim(ext_db_path):
    """honker_heartbeat pushes claim_expires_at forward by extend_s."""
    conn = _open_ext(ext_db_path)
    conn.execute("SELECT honker_enqueue('hb', '{}', NULL, NULL, 0, 3, NULL)")
    conn.commit()
    claimed = conn.execute(
        "SELECT honker_claim_batch('hb', 'w', 1, 60)"
    ).fetchone()[0]
    conn.commit()
    rid = json.loads(claimed)[0]["id"]
    orig_exp = conn.execute(
        "SELECT claim_expires_at FROM _honker_live WHERE id=?", [rid]
    ).fetchone()[0]

    # extend_s=300 pushes claim_expires_at to unixepoch() + 300.
    assert conn.execute(
        "SELECT honker_heartbeat(?, 'w', 300)", [rid]
    ).fetchone()[0] == 1
    conn.commit()
    new_exp = conn.execute(
        "SELECT claim_expires_at FROM _honker_live WHERE id=?", [rid]
    ).fetchone()[0]
    assert new_exp > orig_exp
    now = conn.execute("SELECT unixepoch()").fetchone()[0]
    assert 298 <= new_exp - now <= 302

    # Wrong worker — 0.
    assert conn.execute(
        "SELECT honker_heartbeat(?, 'other', 300)", [rid]
    ).fetchone()[0] == 0
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_enqueue_interops_with_python(ext_db_path):
    """Extension honker_enqueue and Python Queue.enqueue hit the same
    table. IDs are the same PRIMARY KEY sequence. Each side can
    claim the other's rows."""
    db = honker.open(ext_db_path)
    q = db.queue("mixed")

    # Python enqueues.
    py_id = q.enqueue({"from": "py"})

    # Extension enqueues.
    conn = _open_ext(ext_db_path)
    ext_id = conn.execute(
        "SELECT honker_enqueue('mixed', '{\"from\":\"ext\"}', NULL, NULL, 0, 3, NULL)"
    ).fetchone()[0]
    conn.commit()
    conn.close()

    assert ext_id > py_id
    # Python can claim both (they're on the same queue/table).
    jobs = q.claim_batch("py-w", 10)
    assert sorted(j.id for j in jobs) == [py_id, ext_id]


def test_load_extension_names_the_real_problem_when_unsupported():
    """A Python without SQLITE_ENABLE_LOAD_EXTENSION must say so.

    Before this, honker.load_extension fell through to
    `SELECT load_extension(...)`, which SQLite rejects with
    "not authorized" — an error that reads like a permissions problem
    and sends the reader nowhere useful. Found when the SQLAlchemy and
    SQLModel proofs hit it on macOS CI.
    """

    class NoExtensionSupport:
        # A CPython built without the feature exposes neither hook.
        def execute(self, *args, **kwargs):
            raise AssertionError("must not reach the SQL fallback")

    with pytest.raises(honker.ExtensionLoadingUnsupported) as excinfo:
        honker.load_extension(NoExtensionSupport())

    message = str(excinfo.value)
    assert "SQLITE_ENABLE_LOAD_EXTENSION" in message
    assert "honker.open()" in message


# --- queue-scoped cancel (issue #134) -------------------------------
#
# `honker_cancel` carries a global 1-arg form and a queue-scoped 2-arg
# form. honker-core proves the semantics against rusqlite directly;
# these two prove the same functions are really there over the
# `.load libhonker_ext` path, which is how Go, Ruby, .NET, C++, Elixir
# and Bun get them.


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_cancel_carries_both_arities(ext_db_path):
    """Through the loaded extension: the 2-arg form refuses a job in
    another queue and leaves the row alone, cancels one in its own
    queue, and the 1-arg form still reaches any queue."""
    conn = _open_ext(ext_db_path)
    sms_id = conn.execute(
        "SELECT honker_enqueue('sms', '{}', NULL, NULL, 0, 3, NULL)"
    ).fetchone()[0]
    email_id = conn.execute(
        "SELECT honker_enqueue('emails', '{}', NULL, NULL, 0, 3, NULL)"
    ).fetchone()[0]
    conn.commit()

    # Wrong queue: no-op, and the row is still live afterwards.
    assert conn.execute(
        "SELECT honker_cancel('emails', ?)", [sms_id]
    ).fetchone()[0] == 0
    assert conn.execute(
        "SELECT queue FROM _honker_live WHERE id=?", [sms_id]
    ).fetchone() == ("sms",)

    # Own queue: cancels, and the row is gone.
    assert conn.execute(
        "SELECT honker_cancel('sms', ?)", [sms_id]
    ).fetchone()[0] == 1
    assert conn.execute(
        "SELECT queue FROM _honker_live WHERE id=?", [sms_id]
    ).fetchone() is None

    # The 1-arg form is unchanged: global, no queue argument.
    assert conn.execute(
        "SELECT honker_cancel(?)", [email_id]
    ).fetchone()[0] == 1
    assert conn.execute(
        "SELECT queue FROM _honker_live WHERE id=?", [email_id]
    ).fetchone() is None
    conn.commit()
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_capability_probe_detects_the_two_arg_cancel(ext_db_path):
    """The connect-time probe a binding runs before trusting the 2-arg
    form. Calling `honker_cancel` at an arity the loaded extension does
    not have is a hard error, so a binding vendoring an older
    libhonker_ext needs to find out at connect time, not at cancel time.

    `pragma_function_list` reports each arity as its own row. The
    negative case registers a 1-arg `honker_cancel` and nothing else,
    standing in for that older extension: same name, missing arity."""
    probe = (
        "SELECT EXISTS(SELECT 1 FROM pragma_function_list "
        "WHERE name = 'honker_cancel' AND narg = 2)"
    )

    conn = _open_ext(ext_db_path)
    assert conn.execute(probe).fetchone()[0] == 1
    conn.close()

    old = sqlite3.connect(":memory:")
    old.create_function("honker_cancel", 1, lambda job_id: 0)
    assert old.execute(
        "SELECT EXISTS(SELECT 1 FROM pragma_function_list "
        "WHERE name = 'honker_cancel' AND narg = 1)"
    ).fetchone()[0] == 1, "precondition: the 1-arg form is registered"
    assert old.execute(probe).fetchone()[0] == 0
    # And the arity mismatch it protects against is a hard error.
    with pytest.raises(sqlite3.OperationalError, match="wrong number of arguments"):
        old.execute("SELECT honker_cancel('emails', 1)")
    old.close()


# --- fencing token (issue #176) --------------------------------------
#
# The claim's `attempts` is a fencing token. The fenced forms
# honker_ack(id, worker, attempt), honker_retry(id, worker, delay,
# error, attempt), honker_fail(id, worker, error, attempt) and
# honker_heartbeat(id, worker, extend, attempt) only act on the claim
# with that token. The stale handler and the restarted worker run in
# separate OS processes here, sharing one worker id. The lease lapse is
# forced by writing claim_expires_at into the past, so nothing sleeps.

_PEER = r"""
import json, sqlite3, sys
ext, db, stmts = sys.argv[1], sys.argv[2], json.loads(sys.argv[3])
conn = sqlite3.connect(db, isolation_level=None, timeout=10)
conn.enable_load_extension(True)
conn.load_extension(ext)
conn.execute("PRAGMA busy_timeout = 10000")
print(json.dumps([conn.execute(sql, args).fetchone()[0] for sql, args in stmts]))
"""


def _in_other_process(db_path, *stmts):
    """Run each (sql, args) in a fresh OS process; return the scalars."""
    out = subprocess.run(
        [sys.executable, "-c", _PEER, _EXT_PATH, db_path, json.dumps(stmts)],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return json.loads(out.stdout)


def _lapse(conn, job_id):
    conn.execute(
        "UPDATE _honker_live SET claim_expires_at = unixepoch() - 5 WHERE id = ?",
        [job_id],
    )


def _row(conn, job_id):
    return conn.execute(
        "SELECT state, worker_id, attempts, claim_expires_at, claimed_at, run_at "
        "FROM _honker_live WHERE id = ?",
        [job_id],
    ).fetchone()


_STALE_CALLS = {
    "ack": "SELECT honker_ack(?, 'w', 1)",
    "retry": "SELECT honker_retry(?, 'w', 0, 'stale', 1)",
    "fail": "SELECT honker_fail(?, 'w', 'stale', 1)",
    "heartbeat": "SELECT honker_heartbeat(?, 'w', 300, 1)",
}


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
@pytest.mark.parametrize("op", sorted(_STALE_CALLS))
def test_extension_fenced_stale_same_worker_is_refused(ext_db_path, op):
    """Worker 'w' claims attempt 1 and stalls past its lease. Its
    restarted self, also 'w', in another process reclaims: attempt 2.
    The stale handler's fenced call returns 0 and the new owner's row is
    untouched; the new owner then finishes with its own token."""
    conn = _open_ext(ext_db_path)
    conn.isolation_level = None
    jid = conn.execute(
        "SELECT honker_enqueue('fence', '{}', NULL, NULL, 0, 5, NULL)"
    ).fetchone()[0]
    first = json.loads(
        conn.execute("SELECT honker_claim_batch('fence', 'w', 1, 60)").fetchone()[0]
    )
    assert [(j["id"], j["attempts"]) for j in first] == [(jid, 1)]
    _lapse(conn, jid)

    (reclaimed,) = _in_other_process(
        ext_db_path, ("SELECT honker_claim_batch('fence', 'w', 1, 60)", [])
    )
    assert [(j["id"], j["attempts"]) for j in json.loads(reclaimed)] == [(jid, 2)]
    owner_row = _row(conn, jid)
    assert owner_row[:3] == ("processing", "w", 2)

    assert conn.execute(_STALE_CALLS[op], [jid]).fetchone()[0] == 0
    assert _row(conn, jid) == owner_row
    assert conn.execute(
        "SELECT count(*) FROM _honker_dead WHERE id = ?", [jid]
    ).fetchone()[0] == 0

    assert _in_other_process(
        ext_db_path, ("SELECT honker_ack(?, 'w', 2)", [jid])
    ) == [1]
    assert _row(conn, jid) is None
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_unfenced_stale_same_worker_ack_still_succeeds(ext_db_path):
    """The legacy 2-arg ack is unchanged and unfenced: it checks
    worker_id and the lease, which the reclaim refreshed, so the stale
    handler's ack deletes the new attempt. This is why the fenced forms
    exist; bindings move to them by passing job.attempts."""
    conn = _open_ext(ext_db_path)
    conn.isolation_level = None
    jid = conn.execute(
        "SELECT honker_enqueue('fence', '{}', NULL, NULL, 0, 5, NULL)"
    ).fetchone()[0]
    conn.execute("SELECT honker_claim_batch('fence', 'w', 1, 60)")
    _lapse(conn, jid)
    _in_other_process(ext_db_path, ("SELECT honker_claim_batch('fence', 'w', 1, 60)", []))
    assert conn.execute("SELECT honker_ack(?, 'w')", [jid]).fetchone()[0] == 1
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_fenced_late_but_unreclaimed_ack_succeeds(ext_db_path):
    """A handler that overran its lease while nobody reclaimed the job
    still completes with its token, and the job does not run again.
    The fenced form has no lease check: the token alone decides."""
    conn = _open_ext(ext_db_path)
    conn.isolation_level = None
    jid = conn.execute(
        "SELECT honker_enqueue('fence', '{}', NULL, NULL, 0, 5, NULL)"
    ).fetchone()[0]
    conn.execute("SELECT honker_claim_batch('fence', 'w', 1, 60)")
    _lapse(conn, jid)

    # The unfenced form needs a live lease and refuses.
    assert conn.execute("SELECT honker_ack(?, 'w')", [jid]).fetchone()[0] == 0
    assert _in_other_process(
        ext_db_path, ("SELECT honker_ack(?, 'w', 1)", [jid])
    ) == [1]
    assert _row(conn, jid) is None
    assert conn.execute("SELECT count(*) FROM _honker_dead").fetchone()[0] == 0
    (again,) = _in_other_process(
        ext_db_path, ("SELECT honker_claim_batch('fence', 'w2', 10, 60)", [])
    )
    assert json.loads(again) == []
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_fenced_ack_batch_takes_id_attempt_pairs(ext_db_path):
    """honker_ack_batch: [id, attempt] pairs are fenced, plain ids keep
    the legacy guard."""
    conn = _open_ext(ext_db_path)
    conn.isolation_level = None
    ids = [
        conn.execute(
            "SELECT honker_enqueue('fence', '{}', NULL, NULL, 0, 5, NULL)"
        ).fetchone()[0]
        for _ in range(3)
    ]
    jobs = json.loads(
        conn.execute("SELECT honker_claim_batch('fence', 'w', 3, 60)").fetchone()[0]
    )
    assert sorted(j["attempts"] for j in jobs) == [1, 1, 1]
    a, b, c = ids
    pairs = json.dumps([[a, 2], [b, 1], c])
    assert conn.execute("SELECT honker_ack_batch(?, 'w')", [pairs]).fetchone()[0] == 2
    assert _row(conn, a)[:3] == ("processing", "w", 1)
    assert _row(conn, b) is None and _row(conn, c) is None


# ---------- claim v2: scheduled state, eager expiry, ordering ----------


def _ext_conn(path):
    conn = _open_ext(path)
    conn.isolation_level = None
    return conn


def _now(conn):
    return conn.execute("SELECT unixepoch()").fetchone()[0]


def _wait_until(conn, t):
    """Sleep until the database clock reads at least `t`."""
    while _now(conn) < t:
        time.sleep(0.05)


def _dead(conn, job_id):
    row = conn.execute(
        "SELECT last_error FROM _honker_dead WHERE id = ?", [job_id]
    ).fetchone()
    return row and row[0]


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_issue_177_expired_in_flight_job_goes_to_dead(ext_db_path):
    """Issue #177's repro, with the worker in its own process. It claims a
    job that expires in 1 s with a 1 s lease, then dies. After both pass,
    another worker's claim must not leave the job `processing` forever:
    it moves it to `_honker_dead` as 'expired'."""
    conn = _ext_conn(ext_db_path)
    jid = conn.execute(
        """SELECT honker_enqueue('q177', '{"n":1}', NULL, NULL, 0, 3, 1)"""
    ).fetchone()[0]
    (claimed,) = _in_other_process(
        ext_db_path, ("SELECT honker_claim_batch('q177', 'w', 1, 1)", [])
    )
    (job,) = json.loads(claimed)
    assert job["id"] == jid
    _wait_until(conn, job["claim_expires_at"] + 1)
    assert _row(conn, jid)[0] == "processing", "precondition: lapsed, expired, still live"

    (again,) = _in_other_process(
        ext_db_path, ("SELECT honker_claim_batch('q177', 'w2', 1, 1)", [])
    )
    assert json.loads(again) == []
    assert _row(conn, jid) is None
    assert _dead(conn, jid) == "expired"
    assert conn.execute("SELECT honker_sweep_expired('q177')").fetchone()[0] == 0
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_sweep_expired_takes_a_lapsed_in_flight_job(ext_db_path):
    """sweep_expired uses the same rule as the claim: an expired row whose
    lease lapsed goes to dead; one with a valid lease stays with its owner,
    whose fenced ack still works."""
    conn = _ext_conn(ext_db_path)
    gone = conn.execute(
        "SELECT honker_enqueue('qsw', '{}', NULL, NULL, 0, 3, 3600)"
    ).fetchone()[0]
    held = conn.execute(
        "SELECT honker_enqueue('qsw', '{}', NULL, NULL, 0, 3, 3600)"
    ).fetchone()[0]
    (claimed,) = _in_other_process(
        ext_db_path, ("SELECT honker_claim_batch('qsw', 'w', 2, 300)", [])
    )
    assert sorted(j["id"] for j in json.loads(claimed)) == [gone, held]
    conn.execute("UPDATE _honker_live SET expires_at = unixepoch() - 1")
    _lapse(conn, gone)

    assert _in_other_process(ext_db_path, ("SELECT honker_sweep_expired('qsw')", [])) == [1]
    assert _dead(conn, gone) == "expired"
    assert _row(conn, held)[0] == "processing"
    assert _in_other_process(ext_db_path, ("SELECT honker_ack(?, 'w', 1)", [held])) == [1]
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_exhausted_lapsed_lease_goes_to_dead(ext_db_path):
    """A worker dies on the job's last allowed attempt. The next claim, in
    another process, dead-letters it and hands out the next job."""
    conn = _ext_conn(ext_db_path)
    last = conn.execute(
        "SELECT honker_enqueue('qex', '{}', NULL, NULL, 0, 1, NULL)"
    ).fetchone()[0]
    _in_other_process(ext_db_path, ("SELECT honker_claim_batch('qex', 'w', 1, 60)", []))
    _lapse(conn, last)
    nxt = conn.execute(
        "SELECT honker_enqueue('qex', '{}', NULL, NULL, 0, 3, NULL)"
    ).fetchone()[0]
    (claimed,) = _in_other_process(
        ext_db_path, ("SELECT honker_claim_batch('qex', 'w2', 5, 60)", [])
    )
    assert [j["id"] for j in json.loads(claimed)] == [nxt]
    assert _row(conn, last) is None
    assert _dead(conn, last) == "max attempts exceeded"
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_claim_order_across_promotion_and_reclaim(ext_db_path):
    """priority DESC, run_at, id over due rows, a promoted scheduled row
    and a reclaimed lapsed lease, written and claimed by different
    processes on the real clock."""
    conn = _ext_conn(ext_db_path)
    now = _now(conn)
    enq = "SELECT honker_enqueue('qord', '{}', ?, NULL, ?, 3, NULL)"
    (a,) = _in_other_process(ext_db_path, (enq, [now, 0]))
    (b,) = _in_other_process(ext_db_path, (enq, [now - 1, 0]))
    (c,) = _in_other_process(ext_db_path, (enq, [now + 2, 5]))
    (d,) = _in_other_process(ext_db_path, (enq, [now, 5]))
    assert conn.execute(
        "SELECT state FROM _honker_live WHERE id = ?", [c]
    ).fetchone()[0] == "scheduled"
    (first,) = _in_other_process(
        ext_db_path, ("SELECT honker_claim_batch('qord', 'w0', 1, 60)", [])
    )
    assert [j["id"] for j in json.loads(first)] == [d]
    _lapse(conn, d)
    (e,) = _in_other_process(ext_db_path, (enq, [now, 5]))
    _wait_until(conn, now + 2)

    claim = ("SELECT honker_claim_batch('qord', 'w1', 1, 60)", [])
    order = [json.loads(r)[0]["id"] for r in _in_other_process(ext_db_path, *[claim] * 5)]
    assert order == [d, e, c, b, a]
    assert _in_other_process(ext_db_path, claim) == ["[]"]
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_scheduled_state_is_visible_and_cancellable(ext_db_path):
    conn = _ext_conn(ext_db_path)
    a = conn.execute(
        "SELECT honker_enqueue('qs', '{}', NULL, 60, 0, 3, NULL)"
    ).fetchone()[0]
    b = conn.execute(
        "SELECT honker_enqueue('qs', '{}', NULL, 60, 0, 3, NULL)"
    ).fetchone()[0]
    snap = json.loads(conn.execute("SELECT honker_get_job(?)", [a]).fetchone()[0])
    assert snap["state"] == "scheduled"
    assert conn.execute("SELECT honker_queue_next_claim_at('qs')").fetchone()[0] == snap["run_at"]
    assert _in_other_process(ext_db_path, ("SELECT honker_cancel(?)", [a])) == [1]
    assert _in_other_process(ext_db_path, ("SELECT honker_cancel('other', ?)", [b])) == [0]
    assert _in_other_process(ext_db_path, ("SELECT honker_cancel('qs', ?)", [b])) == [1]
    assert conn.execute("SELECT count(*) FROM _honker_live").fetchone()[0] == 0
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT honker_enqueue('qm', '{}', NULL, NULL, 0, ?, NULL)",
        "SELECT honker_scheduler_register('t', 'qm', '@every 1m', '{}', 0, NULL, ?)",
    ],
)
@pytest.mark.parametrize("value", [0, -1])
def test_extension_max_attempts_below_one_is_rejected(ext_db_path, sql, value):
    conn = _ext_conn(ext_db_path)
    with pytest.raises(sqlite3.OperationalError, match="max_attempts must be at least 1"):
        conn.execute(sql, [value]).fetchone()
    assert conn.execute("SELECT count(*) FROM _honker_live").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM _honker_scheduler_tasks").fetchone()[0] == 0
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_tick_dead_letters_a_legacy_payload_without_blocking(ext_db_path):
    """A schedule row whose payload is not JSON (raw SQL, or a build before
    the JSON payload contract) must not stop the tick. The good schedule
    fires every boundary exactly once; the bad one gets one `_honker_dead`
    row per boundary naming it, and is never enqueued."""
    conn = _open_ext(ext_db_path)
    conn.execute(
        "SELECT honker_scheduler_register('good', 'backups', '@every 1s', "
        "'{\"ok\":true}', 0, NULL)"
    )
    conn.commit()
    start = int(conn.execute(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='good'"
    ).fetchone()[0])
    # 'aaa' sorts before 'good', so the tick reaches the bad row first.
    conn.execute(
        "INSERT INTO _honker_scheduler_tasks "
        "(name, queue, cron_expr, payload, priority, next_fire_at, enabled, "
        " max_attempts) "
        "VALUES ('aaa-legacy', 'backups', '@every 1s', 'not json', 0, ?, 1, 3)",
        (start,),
    )
    conn.commit()

    good_fires = []
    # One tick per boundary, then one tick that catches up three at once.
    for at in [start, start + 1, start + 2, start + 5]:
        fires = json.loads(
            conn.execute("SELECT honker_scheduler_tick(?)", (at,)).fetchone()[0]
        )
        conn.commit()
        assert {f["name"] for f in fires} == {"good"}, fires
        good_fires += [f["fire_at"] for f in fires]
        # Same instant again: nothing repeats.
        again = conn.execute("SELECT honker_scheduler_tick(?)", (at,)).fetchone()[0]
        conn.commit()
        assert json.loads(again) == []

    boundaries = list(range(start, start + 6))
    assert good_fires == boundaries
    live = conn.execute(
        "SELECT payload FROM _honker_live WHERE queue='backups'"
    ).fetchall()
    assert live == [('{"ok":true}',)] * len(boundaries)

    dead = conn.execute(
        "SELECT run_at, payload, attempts, last_error FROM _honker_dead "
        "ORDER BY run_at"
    ).fetchall()
    assert [d[0] for d in dead] == boundaries
    for run_at, payload, attempts, last_error in dead:
        assert payload == "not json"
        assert attempts == 0
        assert "honker: payload must be valid JSON" in last_error
        assert '"aaa-legacy"' in last_error
    nexts = conn.execute(
        "SELECT name, next_fire_at FROM _honker_scheduler_tasks ORDER BY name"
    ).fetchall()
    assert nexts == [("aaa-legacy", start + 6), ("good", start + 6)]
    conn.close()
