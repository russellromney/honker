"""Time-trigger scheduler for honker.

A scheduler process holds a set of named schedules (cron expressions
→ queue + payload). On each cron boundary, it enqueues the payload
into the named queue. Regular workers claim and execute it. The
scheduler itself doesn't run handlers — it just dispatches.

Registration + fire-due logic live in Rust via `honker_scheduler_register`
and `honker_scheduler_tick`; this module is a thin asyncio wrapper. Tasks
persist in `_honker_scheduler_tasks`, so any process (Python, a `sqlite3
.load` session, a future Node/Go binding) sees the same registrations.

Leader election via `db.lock('honker-scheduler', ttl=60)` ensures at
most one scheduler fires across all scheduler processes. A periodic
heartbeat refreshes the lock's TTL during long sleeps between fires.
If the leader crashes, the TTL elapses and a standby can take over.

Boundaries that were missed while the leader was down are caught up
on the next tick — `honker_scheduler_tick` advances `next_fire_at`
minute-by-minute until it's past `now`. For noisy schedules that
shouldn't backfill, set `expires=` so stale jobs get swept instead
of executed.

Usage:

    import asyncio
    import honker
    from honker import Scheduler, crontab, every_s

    db = honker.open("app.db")
    scheduler = Scheduler(db)
    scheduler.add(
        name="nightly-backup",
        queue="backups",
        schedule=crontab("0 3 * * *"),
        payload={"target": "s3"},
        expires=3600,  # fired job drops out of claim after 1 hour
    )
    scheduler.add(
        name="every-five",
        queue="health",
        schedule=every_s(5),
    )
    asyncio.run(scheduler.run())
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime
from typing import Any, Optional

from honker import _honker_native
from honker._honker import LeadershipLost

_UNSET = object()


class CronSchedule:
    """Thin marker around a scheduler expression.

    All parsing and next-boundary computation lives in Rust
    (`honker.cron_next_after` / `honker_cron_next_after`) so every
    language binding shares one implementation.

    Valid expression shapes:
      - 5-field cron: `minute hour dom month dow`
      - 6-field cron: `second minute hour dom month dow`
      - interval expression: `@every <n><unit>` (e.g. `@every 1s`)
    """

    __slots__ = ("expr",)

    def __init__(self, expr: str):
        # Validate eagerly by asking Rust to compute one boundary from
        # a known timestamp. Raises ValueError on malformed input.
        _honker_native.cron_next_after(expr, 0)
        self.expr = expr

    def __repr__(self) -> str:
        return f"crontab({self.expr!r})"

    def next_after(self, dt: datetime) -> datetime:
        """Return the next datetime strictly after `dt` matching this
        schedule expression. Pure function — no db needed.
        """
        return datetime.fromtimestamp(
            _honker_native.cron_next_after(self.expr, int(dt.timestamp()))
        )


def crontab(expr: str) -> CronSchedule:
    """Parse a 5-field or 6-field cron expression into a
    `CronSchedule`.
    """
    if expr.strip().startswith("@every"):
        raise ValueError("use every_s(...) for interval schedules")
    return CronSchedule(expr)


def every_s(seconds: int) -> CronSchedule:
    """Build a fixed-interval schedule expression persisted as
    `@every <n>s`.
    """
    seconds = int(seconds)
    if seconds <= 0:
        raise ValueError("every_s(seconds) requires a positive integer")
    return CronSchedule(f"@every {seconds}s")


class Scheduler:
    """Periodic-task dispatcher. Run one process worth of it per app;
    multiple scheduler processes compete for the leader lock and only
    one fires.

    Task registration + fire-due logic live in Rust
    (`honker_scheduler_register` / `honker_scheduler_tick`); this class is
    ~40 lines of asyncio glue around lock + tick + sleep + heartbeat.

    Leader-lock caveat: the scheduler lock is TTL-based, not true
    mutual exclusion across freezes longer than `LOCK_TTL`. Heartbeats
    refresh `expires_at` only for *this* leader's owner token. If the
    TTL elapses and another process acquires the lock, the next
    heartbeat sees 0 rows updated and sets `stop_event` so this
    leader exits before another tick — no dual-fire after lock steal.

    For work that must run exactly once per fire even under a brief
    dual-leader race (the window between steal and the next heartbeat),
    wrap the task body in a second `db.lock('task-name', ttl=...)` and
    exit early on `LockHeld`.
    """

    LOCK_NAME = "honker-scheduler"
    LOCK_TTL = 60
    HEARTBEAT_INTERVAL = 30

    def __init__(self, db, lock_name: Optional[str] = None):
        self.db = db
        self.lock_name = lock_name or self.LOCK_NAME
        # Instance-overridable so tests can use short TTL/heartbeat
        # without monkeypatching the class constants.
        self.lock_ttl = self.LOCK_TTL
        self.heartbeat_interval = self.HEARTBEAT_INTERVAL
        # Names registered via this Scheduler instance. The
        # authoritative registration lives in
        # `_honker_scheduler_tasks` — this set only exists so a
        # process with no tasks to add doesn't acquire the lock in
        # `run()`.
        self._registered: set[str] = set()

    def add(
        self,
        *,
        name: str,
        queue: str,
        schedule: CronSchedule,
        payload: Any = None,
        priority: int = 0,
        expires: Optional[float] = None,
        max_attempts: int = 3,
    ) -> None:
        """Register a periodic task in `_honker_scheduler_tasks`.

        - `name`: unique per-scheduler identifier. A second `add`
          with the same name replaces the first registration
          entirely (including cron expr, queue, payload).
        - `queue`: the queue to enqueue into on each boundary.
        - `schedule`: a `CronSchedule` from `crontab(expr)` or
          `every_s(n)`.
        - `payload`: the payload for enqueued jobs. Default None.
        - `priority`: enqueue priority for fired jobs.
        - `expires`: how many seconds a fired job stays claimable.
          `queue.sweep_expired()` moves expired rows into
          `_honker_dead`.
        - `max_attempts`: attempt budget for each fired job. Default 3.
        """
        with self.db.transaction() as tx:
            tx.query(
                "SELECT honker_scheduler_register(?, ?, ?, ?, ?, ?, ?)",
                [
                    name,
                    queue,
                    schedule.expr,
                    json.dumps(payload),
                    int(priority),
                    int(expires) if expires is not None else None,
                    int(max_attempts),
                ],
            )
        self._registered.add(name)

    def remove(self, name: str) -> bool:
        """Unregister a task. Returns True iff a row was removed."""
        with self.db.transaction() as tx:
            rows = tx.query(
                "SELECT honker_scheduler_unregister(?) AS n", [name]
            )
        self._registered.discard(name)
        return rows[0]["n"] > 0

    # --- lifecycle methods (Phase Mantle) ---------------------------
    #
    # Operate on the persisted row directly via SQL functions, so they
    # work for tasks registered by another process (CLI tool, MCP
    # wrapper, admin script). Names: pause / resume / list / update.

    def pause(self, name: str) -> bool:
        """Pause a registered schedule. Sets `enabled=0` so the
        scheduler skips emitting from this row. Returns True iff a row
        was paused (False if missing or already paused)."""
        with self.db.transaction() as tx:
            rows = tx.query(
                "SELECT honker_scheduler_pause(?) AS n", [name]
            )
        return rows[0]["n"] > 0

    def resume(self, name: str) -> bool:
        """Resume a paused schedule. Returns True iff a row was resumed
        (False if missing or already enabled)."""
        with self.db.transaction() as tx:
            rows = tx.query(
                "SELECT honker_scheduler_resume(?) AS n", [name]
            )
        return rows[0]["n"] > 0

    def list(self) -> list[dict]:
        """Return every registered schedule with its current state
        (cron_expr, payload, priority, expires_s, next_fire_at,
        enabled, max_attempts). Useful for admin UIs and
        'what's scheduled?' MCP tools."""
        with self.db.transaction() as tx:
            rows = tx.query("SELECT honker_scheduler_list() AS j")
        raw = rows[0]["j"] if rows else "[]"
        return json.loads(raw)

    def update(
        self,
        name: str,
        *,
        schedule: Optional[CronSchedule] = None,
        payload: Any = _UNSET,
        priority: Optional[int] = None,
        expires: Any = _UNSET,
        max_attempts: Any = _UNSET,
    ) -> bool:
        """Mutate fields of an existing schedule in place. Pass only
        the fields you want changed; others stay as-is. If `schedule`
        is provided, `next_fire_at` is recomputed from now. Returns
        True iff a row was updated (False if name doesn't exist).

        `payload=None` means "set to JSON null". To leave payload
        unchanged, omit the kwarg. `expires=None` clears expiry;
        `max_attempts=None` resets the attempt budget to the scheduler
        default (3)."""
        cron_expr = schedule.expr if schedule is not None else None
        payload_arg = json.dumps(payload) if payload is not _UNSET else None
        expires_arg = (
            int(expires) if expires is not _UNSET and expires is not None else None
        )
        touch_expires = 1 if expires is not _UNSET else 0
        priority_arg = int(priority) if priority is not None else None
        max_attempts_arg = (
            int(max_attempts)
            if max_attempts is not _UNSET and max_attempts is not None
            else None
        )
        touch_max_attempts = 1 if max_attempts is not _UNSET else 0
        with self.db.transaction() as tx:
            rows = tx.query(
                "SELECT honker_scheduler_update(?, ?, ?, ?, ?, ?, ?, ?) AS n",
                [
                    name,
                    cron_expr,
                    payload_arg,
                    priority_arg,
                    expires_arg,
                    touch_expires,
                    max_attempts_arg,
                    touch_max_attempts,
                ],
            )
        return rows[0]["n"] > 0

    # --- scheduler main loop -----------------------------------------

    async def run(
        self,
        stop_event: Optional[asyncio.Event] = None,
    ) -> None:
        """Acquire the leader lock and run the scheduler loop until
        `stop_event` is set or the enclosing task is cancelled.

        Raises `honker.LockHeld` if another scheduler already holds
        the lock. Callers that want hot-standby semantics should wrap
        in a retry loop.
        """
        stop_event = stop_event or asyncio.Event()

        if not self._registered:
            # This process didn't call .add(), but tasks may have been
            # registered by another process (separate registrar, CLI,
            # migration, prior run persisted to disk). Check the
            # authoritative table before bailing. Raising is friendlier
            # than silent no-op — a dedicated runner process that
            # loads tasks from elsewhere would otherwise log nothing
            # and fire nothing.
            rows = self.db.query(
                "SELECT COUNT(*) AS n FROM _honker_scheduler_tasks"
            )
            if not rows or rows[0]["n"] == 0:
                raise RuntimeError(
                    "Scheduler.run() called with no registered tasks. "
                    "Call scheduler.add(...) first, or ensure another "
                    "process has populated _honker_scheduler_tasks."
                )

        # Hold the lock handle so we know our owner token. Heartbeats
        # must renew by (name, owner) — name-only refresh would extend
        # a *successor* leader's row after our TTL elapsed, and we'd
        # keep ticking as a dual leader.
        lock = self.db.lock(self.lock_name, ttl=self.lock_ttl)
        lost = asyncio.Event()
        with lock:
            hb = asyncio.create_task(
                self._heartbeat_loop(stop_event, lost, lock.owner)
            )
            try:
                await self._main_loop(stop_event, lost, lock.owner)
            finally:
                hb.cancel()
                try:
                    await hb
                except asyncio.CancelledError:
                    pass
            if lost.is_set():
                # Leadership stolen; distinct from clean stop_event exit
                # (caller-set stop_event alone never sets `lost`).
                raise LeadershipLost(
                    f"scheduler lock {self.lock_name!r} lost "
                    f"(owner={lock.owner!r}); another leader took over"
                )

    def _renew_leadership(self, owner: str) -> bool:
        """Extend the leader lock for `owner`. Returns False if stolen."""
        with self.db.transaction() as tx:
            rows = tx.query(
                "SELECT honker_lock_renew(?, ?, ?) AS r",
                [self.lock_name, owner, self.lock_ttl],
            )
        return bool(rows and rows[0]["r"])

    async def _heartbeat_loop(
        self,
        stop_event: asyncio.Event,
        lost: asyncio.Event,
        owner: str,
    ) -> None:
        """Refresh the leader lock's `expires_at` every
        `heartbeat_interval` seconds so the TTL doesn't elapse during
        long sleeps between cron boundaries.

        Scoped to `(name, owner)`. If renew fails we lost the lock —
        set `lost` + `stop_event` so `_main_loop` exits before another
        tick. Ownership is also checked every tick (see `_main_loop`)
        so dual-fire is bounded by tick cadence, not only heartbeat.
        """
        while not stop_event.is_set() and not lost.is_set():
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=self.heartbeat_interval
                )
                return  # stop_event set
            except asyncio.TimeoutError:
                pass
            if not self._renew_leadership(owner):
                lost.set()
                stop_event.set()
                return

    async def _main_loop(
        self,
        stop_event: asyncio.Event,
        lost: asyncio.Event,
        owner: str,
    ) -> None:
        # Subscribe to WAL eagerly so a register/unregister landing
        # during the tick transaction is buffered. Schedule mutations
        # advance data_version on commit so we re-evaluate soonest.
        updates = self.db.update_events()
        while not stop_event.is_set() and not lost.is_set():
            # Prove we still own the lock before every fire. Shrinks
            # the dual-leader window after a steal to one loop
            # iteration instead of up to heartbeat_interval.
            if not self._renew_leadership(owner):
                lost.set()
                stop_event.set()
                return
            now = int(time.time())
            # tick + soonest share a writer transaction: honker_* scalars
            # are registered on the writer slot only (one copy, lowest
            # memory), so reads through reader connections wouldn't
            # find them. The soonest query also serializes behind the
            # tick, so we can't miss a freshly registered task.
            with self.db.transaction() as tx:
                tx.query("SELECT honker_scheduler_tick(?)", [now])
                rows = tx.query(
                    "SELECT honker_scheduler_soonest() AS t"
                )
            soonest = int(rows[0]["t"])
            if soonest == 0:
                return
            sleep_s = max(0.1, soonest - time.time())
            # Race three wake sources against the timer:
            #   - stop_event   → caller asked us to shut down / lost lock
            #   - update tick  → a register/unregister (or any other
            #                    commit) happened; re-evaluate soonest
            #   - timeout      → the originally-computed soonest fired
            # Any of the three just falls through to the top of the loop.
            stop_task = asyncio.ensure_future(stop_event.wait())
            update_task = asyncio.ensure_future(updates.__anext__())
            try:
                await asyncio.wait(
                    {stop_task, update_task},
                    timeout=sleep_s,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for t in (stop_task, update_task):
                    if not t.done():
                        t.cancel()
                for t in (stop_task, update_task):
                    try:
                        await t
                    except (asyncio.CancelledError, StopAsyncIteration):
                        pass
