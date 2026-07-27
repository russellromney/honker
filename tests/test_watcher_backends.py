"""Cross-language proof for the experimental watcher backends.

These tests prove that the optional kernel-watcher and shm-fast-path
backends behave the same as the default polling backend on the public
Python wake/listen surface. They run against the real Python binding
(the maturin-built `_honker_native` cdylib), not Rust unit tests.

Builds without the corresponding Cargo feature reject explicit
`"kernel"` / `"shm"` requests. That keeps this file honest: when an
experimental backend test runs, fallback polling cannot make it pass.

A control assertion runs the same workload against the default polling
backend so a regression in the experimental path stands out from
test-environment flakiness.
"""

import asyncio
import sys
import time

import pytest

import honker

# All listen/update flows are async — match the rest of the suite.
pytestmark = pytest.mark.asyncio


def _open_or_skip_backend(db_path, backend):
    try:
        return honker.open(db_path, watcher_backend=backend)
    except ValueError as exc:
        if backend in {"kernel", "shm"} and "requires the" in str(exc):
            pytest.skip(str(exc))
        raise


async def _drive_commits_and_count_wakes(db, n: int, spacing_ms: int) -> int:
    """Subscribe to update_events, fire `n` commits, count wakes."""
    counted = 0
    done = asyncio.Event()
    events_seen = asyncio.Event()

    async def consume():
        nonlocal counted
        async for _ in db.update_events():
            counted += 1
            events_seen.set()
            if counted >= n:
                done.set()
                return

    consumer_task = asyncio.create_task(consume())
    # Give the watcher thread a moment to start before issuing commits.
    await asyncio.sleep(0.05)

    with db.transaction() as tx:
        tx.execute("CREATE TABLE IF NOT EXISTS t (x INT)")
    await asyncio.sleep(spacing_ms / 1000.0)

    for i in range(n):
        with db.transaction() as tx:
            tx.execute("INSERT INTO t VALUES (?)", [i])
        await asyncio.sleep(spacing_ms / 1000.0)

    # Wait long enough for the slowest backend's safety net (500 ms)
    # plus event delivery latency.
    try:
        await asyncio.wait_for(done.wait(), timeout=2.5)
    except asyncio.TimeoutError:
        pass

    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass
    return counted


@pytest.mark.parametrize(
    "backend",
    [
        None,        # default polling — control
        "kernel",    # Phase 003
        "shm",       # Phase 004
    ],
)
async def test_watcher_backend_detects_commits(db_path, backend):
    db = _open_or_skip_backend(db_path, backend)
    # Each commit spaced 30 ms apart — well above polling (1 ms),
    # shm fast path (100 µs), and kernel-watcher event-delivery latency.
    n = 4
    counted = await _drive_commits_and_count_wakes(db, n=n, spacing_ms=30)
    # Conftest's gc.collect() releases the underlying SQLite handles.

    # `update_events()` fires once per observed commit. The first wake
    # may be from the CREATE TABLE; we tolerate >= n (each insert) and
    # bound generously to surface a runaway watcher.
    min_wakes = 1 if backend == "kernel" and sys.platform == "win32" else n
    assert counted >= min_wakes, (
        f"watcher_backend={backend!r}: only {counted} wakes for {n} commits"
    )
    max_wakes = n * 4 if backend == "kernel" else n + 2
    assert counted <= max_wakes, (
        f"watcher_backend={backend!r}: {counted} wakes for {n} commits "
        "exceeds reasonable upper bound — runaway watcher?"
    )


async def test_custom_watcher_poll_interval_detects_commits(db_path):
    db = honker.open(db_path, watcher_poll_interval_ms=25)
    counted = await _drive_commits_and_count_wakes(db, n=1, spacing_ms=60)
    assert counted >= 1


async def test_unknown_watcher_backend_raises(db_path):
    with pytest.raises(ValueError, match="unknown watcher backend"):
        honker.open(db_path, watcher_backend="bogus")
    with pytest.raises(ValueError, match="unknown watcher backend"):
        honker.open(db_path, watcher_backend="KERNEL")
    with pytest.raises(ValueError, match="unknown watcher backend"):
        honker.open(db_path, watcher_backend=" polling ")


async def test_polling_watcher_backend_aliases_open(db_path):
    for backend in (None, "poll", "polling"):
        db = honker.open(db_path, watcher_backend=backend)
        db.queue("alias-check")


async def test_shm_backend_works_for_real_db(db_path):
    """Sanity: the probe at honker.open() time succeeds for a normal db
    (WAL mode + open writer connection). The failure paths are
    exercised by the Rust unit test `watcher_backend_probe_fails_for_*`."""
    _open_or_skip_backend(db_path, "shm")  # must not raise when compiled
