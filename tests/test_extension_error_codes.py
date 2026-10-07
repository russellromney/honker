"""SQLite result codes from honker functions, through the raw extension.

A lock conflict inside a ``honker_*`` function must reach the caller as
SQLITE_BUSY (5) or SQLITE_LOCKED (6), the codes every SQLite client and
retry loop already understands. It used to arrive as SQLITE_ERROR (1)
with the text "database is locked", so a binding could not tell a
transient conflict from a real error without matching on the message.

Every other error keeps SQLITE_ERROR (1) and its message, including
#167's "requires a separate SELECT" error, which SQLite raises as
SQLITE_BUSY but which no retry will fix.

The lock holder is a separate OS process that holds ``BEGIN EXCLUSIVE``
on the same file. The caller sets a 50 ms busy timeout.

``HONKER_EXTENSION_PATH`` selects the extension. These tests skip when
the extension is missing or this interpreter's sqlite3 cannot load
extensions; ``HONKER_REQUIRE_EXTENSION=1`` turns those skips into
failures (CI sets it on the step that runs this file with uv's managed
CPython).
"""

import json
import os
import sqlite3
import subprocess
import sys
import textwrap

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lifecycle_torture import find_extension  # noqa: E402

SQLITE_ERROR = 1
SQLITE_BUSY = 5
SQLITE_LOCKED = 6

_EXT = find_extension()
_HAS_LOAD_EXT = hasattr(sqlite3.connect(":memory:"), "enable_load_extension")
_REQUIRE = os.environ.get("HONKER_REQUIRE_EXTENSION") == "1"


def _missing_reason():
    if _EXT is None:
        return "honker extension not found (set HONKER_EXTENSION_PATH)"
    if not _HAS_LOAD_EXT:
        return f"{sys.executable}'s sqlite3 cannot load extensions"
    return None


@pytest.fixture(autouse=True)
def _need_extension():
    reason = _missing_reason()
    if reason is None:
        return
    if _REQUIRE:
        pytest.fail(reason + " and HONKER_REQUIRE_EXTENSION=1", pytrace=False)
    pytest.skip(reason)


def _connect(path, timeout=10.0, uri=False):
    conn = sqlite3.connect(path, isolation_level=None, timeout=timeout, uri=uri)
    conn.enable_load_extension(True)
    try:
        conn.load_extension(_EXT, entrypoint="sqlite3_honkerext_init")
    except TypeError:  # Python < 3.12 has no entrypoint argument
        conn.load_extension(_EXT)
    conn.enable_load_extension(False)
    return conn


# Each case: (name, SQL that writes under the lock). Setup gives every
# one of them a row to act on, so none of them can finish without the
# write lock.
CALLS = [
    ("honker_claim_batch", "SELECT honker_claim_batch('q', 'w2', 1, 30)"),
    ("honker_ack", "SELECT honker_ack(?, 'w')"),
    ("honker_ack_fenced", "SELECT honker_ack(?, 'w', 1)"),
    ("honker_scheduler_tick", "SELECT honker_scheduler_tick(unixepoch() + 3600)"),
    (
        "honker_enqueue",
        "SELECT honker_enqueue('q', '{}', NULL, NULL, 0, 3, NULL)",
    ),
    ("notify", "SELECT notify('ch', '{}')"),
]


@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "codes.db")
    conn = _connect(path)
    conn.execute("PRAGMA journal_mode=WAL").fetchone()
    conn.execute("SELECT honker_bootstrap()")
    conn.execute("SELECT honker_enqueue('q', '{}', NULL, NULL, 0, 3, NULL)")
    conn.execute("SELECT honker_enqueue('q', '{}', NULL, NULL, 0, 3, NULL)")
    claimed = conn.execute("SELECT honker_claim_batch('q', 'w', 1, 300)").fetchone()[0]
    job_id = json.loads(claimed)[0]["id"]
    conn.execute(
        "SELECT honker_scheduler_register('t', 'q', '@every 1s', '{}', 0, NULL)"
    )
    conn.close()
    return path, job_id


class _Holder:
    """A separate process holding ``BEGIN EXCLUSIVE`` until told to stop."""

    def __init__(self, path):
        code = textwrap.dedent(
            """
            import sqlite3, sys
            c = sqlite3.connect(sys.argv[1], isolation_level=None)
            c.execute("BEGIN EXCLUSIVE")
            print("locked", flush=True)
            sys.stdin.readline()
            c.execute("ROLLBACK")
            c.close()
            """
        )
        self.proc = subprocess.Popen(
            [sys.executable, "-c", code, path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        line = self.proc.stdout.readline().strip()
        assert line == "locked", f"lock holder did not start: {line!r}"

    def release(self):
        self.proc.stdin.write("\n")
        self.proc.stdin.flush()
        assert self.proc.wait(timeout=30) == 0


def _error_of(conn, sql, args=()):
    with pytest.raises(sqlite3.Error) as info:
        conn.execute(sql, args).fetchall()
    return info.value


@pytest.mark.parametrize("name,sql", CALLS, ids=[c[0] for c in CALLS])
def test_lock_conflict_is_sqlite_busy(db, name, sql):
    path, job_id = db
    holder = _Holder(path)
    try:
        conn = _connect(path, timeout=0.05)
        args = (job_id,) if "?" in sql else ()
        err = _error_of(conn, sql, args)
        print(f"{name}: sqlite_errorcode={err.sqlite_errorcode} "
              f"({err.sqlite_errorname}) message={err}")
        assert err.sqlite_errorcode & 0xFF == SQLITE_BUSY, (
            f"{name} under another process's EXCLUSIVE lock must fail with "
            f"SQLITE_BUSY, got {err.sqlite_errorcode} ({err.sqlite_errorname}): {err}"
        )
        assert isinstance(err, sqlite3.OperationalError)
        assert conn.in_transaction is False, "the failed call must not leave a transaction open"
    finally:
        holder.release()
    # Transient means it: once the lock is gone the same call succeeds.
    conn.execute(sql, args).fetchall()
    conn.close()


def test_shared_cache_table_lock_is_sqlite_locked(tmp_path):
    """SQLITE_LOCKED: a shared-cache sibling connection holds a write
    transaction on the table the honker function writes to."""
    uri = (tmp_path / "locked.db").as_uri() + "?cache=shared"
    a = _connect(uri, uri=True)
    a.execute("SELECT honker_bootstrap()")
    b = _connect(uri, timeout=0.05, uri=True)
    a.execute("BEGIN IMMEDIATE")
    a.execute("SELECT honker_enqueue('q', '{}', NULL, NULL, 0, 3, NULL)")
    try:
        err = _error_of(b, "SELECT honker_enqueue('q', '{}', NULL, NULL, 0, 3, NULL)")
        print(f"shared cache: sqlite_errorcode={err.sqlite_errorcode} "
              f"({err.sqlite_errorname}) message={err}")
        assert err.sqlite_errorcode & 0xFF == SQLITE_LOCKED, (
            f"got {err.sqlite_errorcode} ({err.sqlite_errorname}): {err}"
        )
    finally:
        a.execute("ROLLBACK")
        a.close()
        b.close()


def test_non_transient_error_keeps_sqlite_error_and_message(db):
    path, _ = db
    conn = _connect(path)
    err = _error_of(conn, "SELECT honker_enqueue('q', '{}', NULL, NULL, 0, 0, NULL)")
    assert err.sqlite_errorcode == SQLITE_ERROR, (err.sqlite_errorcode, str(err))
    assert "max_attempts must be at least 1" in str(err), str(err)
    conn.close()


def test_write_context_error_keeps_sqlite_error_and_message(db):
    """#167: SQLite raises "cannot open savepoint - SQL statements in
    progress" as SQLITE_BUSY, but retrying cannot fix it. It stays
    SQLITE_ERROR with honker's hint."""
    path, _ = db
    conn = _connect(path)
    conn.execute("CREATE TABLE app(x)")
    err = _error_of(conn, "INSERT INTO app SELECT honker_claim_batch('q', 'w2', 1, 30)")
    assert err.sqlite_errorcode == SQLITE_ERROR, (err.sqlite_errorcode, str(err))
    assert "requires a separate SELECT" in str(err), str(err)
    conn.close()
