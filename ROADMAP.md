# ROADMAP

Roadmap items are future work. Completed phases move to
`CHANGELOG.md`; this file should not carry shipped history.

## Phase naming

Phases use the format `Phase <Name>`, not numbers, so we can insert
new work without renumbering. Names are unique. Each phase header
should include adjacency links (`After: ... · Before: ...`) when the
ordering matters.

## Phase Ranger — Delegate Locks To Bouncer

> Later architecture work

Replace Honker's internal named-lock lease implementation with
`bouncer-core` while preserving Honker's public lock APIs.

### Scope

- Add a versioned `bouncer-core` dependency to `honker-core`.
- Bootstrap `bouncer_resources` as part of Honker bootstrap.
- Reimplement `honker_lock_acquire(name, owner, ttl_s)` using
  `bouncer_core::claim` / `claim_in_tx`.
- Reimplement `honker_lock_release(name, owner)` using
  `bouncer_core::release` / `release_in_tx`.
- Add a Honker renew path for scheduler and binding heartbeats,
  backed by `bouncer_core::renew` / `renew_in_tx`.
- Preserve Python `db.lock(...)`, Rust `db.try_lock(...)`, and SQL
  `honker_lock_*` return shapes.
- Keep Bouncer fencing tokens internal for now; Honker's lock API can
  stay boolean.

### Non-goals

- Do not expose Bouncer's full public API through Honker.
- Do not rename `honker_lock_acquire` / `honker_lock_release`.
- Do not migrate live `_honker_locks` rows across upgrade; these are
  ephemeral TTL rows and can expire naturally.
- Do not move queue job visibility claims to Bouncer. Job claims are
  queue semantics, not named resource ownership.
- Do not extract rate limiting here.

### Verification

- Existing Python lock tests pass unchanged except assertions that
  reached into `_honker_locks` internals.
- Existing Rust `honker-rs` advisory-lock tests pass.
- Existing scheduler leader tests pass.
- New SQL interop test proves Honker lock acquire and Bouncer
  inspection observe the same resource row.
- New regression test proves losing the Bouncer lease causes the
  scheduler leader loop to exit before firing again.

## Phase Test Depth And Interop

> Near-term hardening

The maintained bindings now live in-tree, and root CI owns the default
checks. The remaining test-regime gaps are about depth, not basic
"does this binding build?" coverage.

Keep this split into reviewable slices instead of growing one giant CI
change.

### Cross-binding interop

- Add at least one more pair beyond Python <-> Node and Ruby <->
  Python. Go <-> Python is the likely next pair because both can share
  a plain `.db` file through the extension.
- Add a tiny matrix that proves a job enqueued in one binding can be
  claimed and acked in another.
- Keep slow or expensive combinations out of default CI unless they
  catch real bugs.

### Stress and soak

- Add higher-pressure multi-writer / multi-reader tests.
- Add many-subscriber listener churn coverage beyond the focused
  regression tests.
- Add a manual or scheduled soak workflow that watches FD, thread, and
  memory growth over time.

### Compatibility surface

- Add a SQLite version matrix where it matters. Default CI mostly proves
  the versions on GitHub runners.
- Add coverage reporting if it starts guiding useful decisions.
- Add follow-up watcher timing tests if Windows still shows
  platform-specific drift.

## Completed — Time-trigger scheduler and wake parity

Shipped in PR #29, with follow-up release prep in PR #33.

- `run_at` jobs now wake workers at their deadline instead of waiting
  for a later fallback poll.
- Reclaim deadlines now wake sleeping workers on time too.
- Scheduler expressions now support 5-field cron, 6-field cron, and
  `@every <n><unit>`.
- Maintained bindings converged on the same basic time-trigger shape:
  update wake or next deadline, with fallback polling only as backup.
- Canonical recurring name is now `schedule`, with legacy `cron` kept
  as a compatibility alias where needed.
- Ruby and Elixir expose extension-backed `notify` and table APIs but do
  not yet expose async listen/update-watcher APIs.

### Scope

- Add binding docs that name whether each package uses update events or
  timer polling.
- Add Bun `updateEvents()` / listen bridge, or explicitly defer it with
  tests proving current poll behavior.
- Add Ruby and Elixir listener APIs only if their runtime integrations
  can support a clean cancellation story.
- Decide whether Go and C++ should keep local watcher implementations or
  grow a shared C ABI around the core watcher.
- Add parity tests that exercise a cross-process notification wake in
  every binding with a listener API.

### Non-goals

- Do not make the SQLite loadable extension itself push events. Plain SQL
  clients can write/read the shared tables, but need a host-language
  watcher to sleep efficiently.
- Do not block 1.0 on bindings that are explicitly marked poll-based or
  partial, as long as the docs and tests say so.


## Phase Echo — Experimental Watcher Backends Across All Bindings

> After: Phase Wake Parity · Before: 1.0 release prep

> **Status:** core + Python + Node shipped in PR #30 (universal polling
> SQLITE_BUSY fix, opt-in `kernel`/`shm` backends behind Cargo features,
> sync baseline handshake, watcher-death propagation). Rust wrapper
> parity is wired via `OpenOptions::watcher_backend`. Go, Bun, C++,
> .NET, Ruby, and Elixir are core-backed too: extension consumers route
> blocking wake waits through the shared extension watcher ABI (or the
> SQL watcher-handle bridge for Elixir). Polling soaked 600 s under
> sustained writes — every commit observed, integrity ok.

The experimental `kernel` and `shm` watcher backends ship in
`honker-core` and every maintained binding routes waits through that
same implementation. Direct Rust-style bindings call
`SharedUpdateWatcher` in-process. Extension-backed bindings load the
watcher ABI from the same `libhonker_ext` they already require; Elixir
uses SQL watcher-handle functions registered by that extension.

### Scope

For each supported binding:

- Add Cargo features `kernel-watcher` and `shm-fast-path` that forward
  to `honker-core/<feature>` (where the binding has a Cargo.toml).
- Accept a `watcher_backend` (or language-idiomatic equivalent) string
  parameter on the binding's `open()`.
- Parse via `honker_core::WatcherBackend::parse` so the accepted
  aliases (`"polling"` / `"poll"`, `"kernel"` / `"kernel-watcher"`,
  `"shm"` / `"shm-fast-path"`) stay in lockstep with Python/Node.
- Call `WatcherBackend::probe(&db_path)` at `open()` time and surface
  failures as the language's idiomatic error type. **No silent
  fallback.**
- Pass the `WatcherConfig` through to `SharedUpdateWatcher::new_with_config`
  directly or through the extension ABI. No binding owns a separate
  data-version poller for blocking waits.

### Tests per binding

- Direct API test: `open(backend=...)` for each backend; one returns
  the polling default, kernel/shm raise iff the feature isn't
  compiled into that binding's build.
- Cross-process listener: writer subprocess + parent listener under
  each backend. Every commit must surface to the listener.
- Cross-process queue worker: 1×1, 1×N (workers), N×1 (writers)
  topologies × each backend. Mirrors the existing
  `tests/test_watcher_backends_queue_e2e.py` and
  `packages/honker-node/test/watcher_backends_queue_e2e.js`.
- Backend-isolation test: disable high-level fallback polling / idle
  waits, or inspect backend wake telemetry, so the selected experimental
  backend must carry the wake.

### Non-goals

- Don't add new wire formats or per-binding watcher logic. All
  backends live in `honker-core`; bindings only thread the param.
- Don't auto-detect / silently substitute backends — the experimental
  contract is "opt in, fail loud, restart".
- Don't add per-language watcher backends. Extension consumers can have
  language-specific fanout/iterator wrappers, but the blocking wake
  primitive comes from `honker-core`.

### Verification

- Per binding: tests above pass on Linux and macOS in CI.
- Windows is opt-in; document expected behavior per binding.
- All tests in `tests/test_watcher_backends.py`,
  `tests/test_watcher_backends_e2e.py`, and
  `tests/test_watcher_backends_queue_e2e.py` continue to pass for
  Python; equivalents continue to pass for Node.

### Proof-Parity Plan

1. Treat Python and Node as the reference proof shape for watcher
   backends: direct option contract, cross-process listener, and
   cross-process queue worker tests for 1×1, 1×N, and N×1 topologies.
2. Every binding keeps a native-language proof, but helpers must be
   separate OS processes. Threads, tasks, futures, and BEAM processes
   are useful local stress tests, not watcher-backend parity proof.
3. Queue e2e tests must use a no-fallback wait path: workers block on
   the selected core watcher until real queue writes or explicit stop
   jobs wake them. A polling timeout may only bound failure, not make
   success possible.
4. CI must run every binding with an extension/build that can load
   `libhonker_ext` and must fail when a binding's watcher e2e suite is
   skipped unexpectedly. Local skips are allowed only for missing
   optional toolchains or experimental backend features.
5. The release gate for this phase is the binding matrix plus one CI
   job per binding showing: polling aliases accepted exactly, unknown
   names rejected exactly, unsupported experimental backends fail loud,
   and supported `kernel` / `shm` modes pass the full cross-process
   listener + queue topology suite.

Hardening decisions made while converting the remaining bindings to
process helpers:

- The kernel watcher now wakes conservatively for every filesystem
  event and retries direct watches when SQLite creates WAL-side files
  after watcher startup. This removes the .NET many-writer missed-wake
  failure without adding a high-level polling fallback.
- The `shm` fast path now tolerates WAL shared-memory churn by reopening
  the current `-shm` file and rebasing `iChange`. C++ and Elixir `shm`
  process proofs also pin a long-lived SQLite connection during the
  topology so the proof matches the backend's "WAL + live shm" contract.
- Ruby now depends on `sqlite3 >= 2.0.4`, the modern line that exposes
  loadable-extension APIs, and CI sets
  `HONKER_REQUIRE_RUBY_EXTENSION_LOADING=1` so extension-loading skips
  are hard failures.

### Full API-Parity Follow-Up

Watcher-backend parity does not imply full binding-surface parity.
Transactional outbox helpers now exist in every core binding and have
binding-local rollback/commit/deliver proofs. Python remains the
richest binding for task decorators and worker shortcuts, so the next
feature-parity phase should either port those task APIs to every core
binding or define explicit API tiers so "core binding" has a precise
meaning before 1.0.

## Phase Atlas — Map Experimental Backend Edge-Case Behavior

> After: Phase Echo · Before: 1.0 release prep

Experimental backends ship with "spurious wakes possible, missed
wakes possible" plus a dead-man's switch. That covers the headline
contract but leaves a long tail where the three backends behave
differently. Users opting in deserve tests that pin down exactly
what they get. This phase characterizes — does not fix.

### Acceptance: a Rust test per (backend, scenario), behavior matrix in README + module docs

- [ ] Rollback (`BEGIN IMMEDIATE; INSERT; ROLLBACK`)
- [ ] `wal_checkpoint(TRUNCATE)` wake count
- [ ] External non-SQLite writes (`dd`, `truncate`)
- [ ] Multiple databases in same directory (kernel cross-pollination)
- [ ] VACUUM (all three should panic via dead-man's switch with the
      same message)
- [ ] Mid-flight `journal_mode` change (shm should panic; others keep working)
- [ ] System suspend/hibernate (document; CI can't test)
- [ ] Symlinked db path
- [ ] `fork()` without exec (document non-support)
- [ ] NFS / SMB / FUSE — `probe()` should refuse the shm fast path
- [ ] Crash recovery: SIGKILL the writer, reopen, prove wakes resume
- [ ] Litestream-style restore — `update_events()` should `Err`
      (Disconnected) within ~200 ms of replacement; document the
      "recreate after restore" pattern in `docs/litestream.md`

### Non-goals

- Don't *fix* the differences. The contract says experimental.
- Don't add backend-specific de-duplication to mask rollback wakes.
- Don't suppress the VACUUM panic.

## Phase Boundary — Wake-Source Isolation And Independence

> After: Phase Atlas · Before: 1.0 release prep

Honker has multiple wake sources (DB-update via `SharedUpdateWatcher`,
deadline wake from #29, fallback poll, push signals on `notify`
rows). Each consumer listens on a union. Today they appear independent
by inspection but nothing proves it. This phase pins down isolation:
disabling any one source must leave the others carrying the load,
and no commit + deadline collision should double-fire.

### Acceptance: per-isolation tests, plus `docs/wake-topology.md`

- [ ] DB-update only (no deadlines, no fallback firing)
- [ ] Deadline only (`SharedUpdateWatcher` mocked to never fire)
- [ ] Fallback-poll only (both above mocked off)
- [ ] Push-signal `db.listen(channel)` independent of deadlines
- [ ] No double-fire when a commit lands exactly at a deadline
- [ ] No starvation: flood of DB-update wakes still lets deadline
      wakes fire and vice versa
- [ ] Cross-process repro: writer + worker subprocess, worker with
      each source disabled in turn

### Non-goals

- Don't unify sources. Independence is the feature.
- Don't add source priority. Every source fires; consumers re-query
  on wake; everything is idempotent.

## Phase Lighthouse — Ship Experimental Wake Backends In Published Wheels

> After: Phase Atlas · Before: 1.0 release prep

PR #30 shipped the experimental backends in source, gated by Cargo
features. Published wheels still build polling-only. This phase
decides when to enable the features in wheel builds. Polling stays
the default either way; "available in wheels" is the only flip.

### Prerequisites (all must hold before flipping)

- [ ] CI builds and tests `--features kernel-watcher,shm-fast-path`
      on Linux + macOS + Windows (Rust + Python + Node e2e)
- [ ] Phase Atlas characterization tests green everywhere — at
      minimum: rollback, multi-db-same-dir, VACUUM, NFS refusal
- [ ] Real-world dogfooding (days, not minutes) with each backend
      selected — catches OS quirks under load and resource leaks

### Scope when prereqs met

- [ ] Wheel CI passes the features on platforms that earned it
- [ ] README drops the "source-only" notice
- [ ] Optional `*-rc` channel for early adopters before flipping

### Non-goals

- Don't change the default backend. Polling stays.
- Don't ship a feature that fails any platform's CI. If one OS
  earns it and another doesn't, ship neither.

## Phase Mantle — Schedule Lifecycle + Cancel

> After: Phase Echo · Before: 1.0 release prep

Mickey Mantle was a switch-hitter — pause / resume from one side to
the other. This phase makes schedules and pending jobs *manageable* at
runtime: pause, resume, modify, unschedule, list, cancel. Today an
operator (or CLI tool, or MCP wrapper) has to UPDATE / DELETE
`_honker_scheduler_tasks` and `_honker_live` directly. After Mantle
they call methods.

This is the answer to issue #25 / #26. `max_runs` is deliberately NOT
in scope — see Non-goals — but the rest of sahuguet's wishlist
(pause, resume, modify, list, unschedule, cancel) is. Closest prior
art: pg-boss (`schedule`, `unschedule`, `getSchedules`, `cancel`).
pg-boss also lacks `max_runs`; honker matches its shape.

### Scope

Schedule lifecycle:
- [ ] Add `enabled BOOLEAN NOT NULL DEFAULT 1` to `_honker_scheduler_tasks`.
- [ ] `db.unschedule(name)` — DELETE the row. Idempotent on missing.
- [ ] `db.pause_schedule(name)` / `db.resume_schedule(name)` — toggle
      `enabled`. Scheduler skips emitting from disabled rows.
- [ ] `db.list_schedules()` — return all rows with current state +
      `next_fire_at`. Useful for admin UIs and "what's scheduled?"
      MCP tools.
- [ ] `db.update_schedule(name, *, cron_expr=None, payload=None,
      priority=None, expires_s=None)` — mutate in place. Recompute
      `next_fire_at` if `cron_expr` changed.

Job lifecycle:
- [ ] `db.queue("emails").cancel(job_id)` — DELETE a pending or
      processing row. Returns true if a row was removed. Idempotent.
- [ ] `db.queue("emails").get_job(job_id)` — return the row (or
      None). Pure read.

Bindings:
- [ ] Wire all methods through Python + Node + Rust. Other bindings
      tracked under Phase Echo.

### Acceptance

- [ ] One round-trip test per method per binding (Python + Node + Rust).
- [ ] Cross-process: `pause` from process A is observed by scheduler
      in process B within ≤ 1 s; `resume` re-emits within ≤ 1 s.
- [ ] `list_schedules` round-trips all fields including the new `enabled`.
- [ ] `cancel()` of a pending job removes it before any worker claims.
- [ ] `cancel()` of a claimed-but-not-ack'd job removes it; the
      worker's subsequent `ack()` returns 0 (no-op, same as expired claim).
- [ ] Documented "deterministic-wrapper for max_runs" pattern in
      `docs/recipes/bounded-schedules.mdx` — uses `unschedule()` from
      inside the worker after a counter check.

### Non-goals

- **No `max_runs` column.** It looks like a scheduler primitive but the
  semantic ambiguity (fires vs claims vs successful completions) is
  business logic. Apps that need bounded recurrences either insert N
  concrete `run_at` jobs at registration time, or maintain their own
  counter and call `unschedule()` when the bound is hit. pg-boss
  reached the same conclusion. Issue #25 thread covers the reasoning.
- No `end_at` (time-based bound) yet — different semantics from
  `max_runs`, separate phase if demand surfaces. The clean version of
  the same idea (measured at the emit boundary, no downstream-state
  dependency).
- No "cancel by predicate" / "cancel all in queue" — too easy to shoot
  a foot. Write the SQL if you need it.
- No "modify all running jobs spawned from this schedule" — schedules
  emit jobs; jobs are independent rows. Mutating the schedule affects
  future emits only.

## Phase Mays — Pg-Boss Parity (Singleton, State Events, Queue Stats)

> After: Phase Mantle · Before: 1.0 release prep

Willie Mays was a five-tool player — hit, hit for power, run, field,
throw. This phase is five-ish small features that together close most
of honker's remaining gap with pg-boss for production users:

- **Singleton / dedup keys** — "this enqueue can't fire twice while a
  prior one is still pending."
- **Job-state event channels** — every state transition (created,
  claimed, completed, failed, retried, dead) emits a notify on a
  reserved `_honker:job:*` channel. UI dashboards, downstream
  triggers, and oncall scripts subscribe without running a worker.
- **Queue stats / size** — `size()`, `stats()` for "what's in this
  queue right now?" Surface the counts that already exist in queries.
- **Convenience event handlers** — `db.on_completed(queue, handler)`
  etc., a thin filter over `db.listen()` on the new channels.

Each one is small (one-table-touch + one method per binding). They're
bundled because they're all "the queue is now observable and
controllable from outside the worker."

### Scope

Singleton:
- [ ] Add `singleton_key TEXT NULL` to `_honker_live`.
- [ ] Partial unique index: `(queue, singleton_key) WHERE singleton_key
      IS NOT NULL AND state IN ('pending','processing')`.
- [ ] `enqueue(payload, singleton_key=...)` accepts an optional key.
      On constraint violation, return the existing job's id (or `None`
      — decide ergonomic shape) rather than raising.
- [ ] Same on `enqueue_tx`. Atomicity preserved with the tx.

State events:
- [ ] Define channel names: `_honker:job:{queue}:created`,
      `:claimed`, `:completed`, `:failed`, `:retried`, `:dead`.
- [ ] Worker code `tx.notify(channel, {job_id, ...})` on each
      transition, atomic with the state change.
- [ ] Reserve the `_honker:` prefix in user docs.
- [ ] Convenience: `db.on_completed(queue, handler)` etc. Internally
      a `db.listen()` filtered by channel.

Queue stats:
- [ ] `db.queue("emails").size()` — `{pending, processing, dead}` counts.
- [ ] `db.queue("emails").stats()` — extends `size()` with
      `oldest_pending_at`, `oldest_processing_at`, `next_run_at`.

Bindings:
- [ ] Wire through Python + Node + Rust.

### Acceptance

Singleton:
- [ ] Two concurrent processes both call `enqueue(payload,
      singleton_key="daily-stripe")`. Exactly one row lands. Second
      call returns existing job's id (no raise).
- [ ] Once ack'd, subsequent enqueue with same key succeeds.
- [ ] Per-queue isolation: same key in different queues is allowed.

State events:
- [ ] Listener attached to `_honker:job:emails:completed` receives
      notification within ~1 ms of `job.ack()`.
- [ ] All six transitions fire and are tested.
- [ ] Cross-process / cross-binding: Python writer → Node listener.
- [ ] No measurable throughput regression on enqueue/claim/ack path.

Queue stats:
- [ ] `size()` returns correct counts after a known enqueue/claim/ack
      sequence, in every binding.
- [ ] `stats()` `oldest_pending_at` is correct after a 1 s delay.

### Non-goals

- No time-windowed dedup (`singletonMinutes`-style). Separate phase
  if demand surfaces.
- Dedup applies to enqueue, not to schedule emit — schedules with a
  `singleton_key` in their payload would still emit one job per tick.
- State events are ephemeral. No replay / catchup. If you need that,
  query `_honker_live` / `_honker_dead` directly.
- No real-time stats stream — `stats()` is a point-in-time read.
- No purge / cancel-by-predicate (write the SQL).

## Phase Gehrig — Per-Queue Config Defaults

> After: Phase Mantle · Phase Mays · Before: 1.0 release prep

Today every `enqueue()` accepts (and may require) `max_attempts`,
`expires_s`, `priority`. For production deployments this is repetitive
and error-prone — the same queue should have the same retry policy
across every callsite.

pg-boss attaches policy to the queue: `boss.createQueue(name, {
retryLimit, retryDelay, expireInHours, deadLetter })`. Per-enqueue
overrides win when present.

This is the biggest schema change of the new phases — adds a
`_honker_queues` table and merge-at-enqueue logic — so it's
intentionally last in the sequence after the easier wins.

### Scope

- [ ] `_honker_queues` table: `name PRIMARY KEY`, `default_max_attempts`,
      `default_retry_delay_s`, `default_expires_s`, `default_priority`,
      `dead_letter_queue TEXT NULL`, `created_at`.
- [ ] `db.queue("emails", config={...})` upserts a row when called
      with a config dict; called without config it just returns the
      handle (current behavior). Backwards-compatible.
- [ ] `enqueue()` merges per-call args over per-queue defaults over
      hard-coded defaults. Per-call wins.
- [ ] `db.list_queues()` — return all configured queues with their
      defaults + sizes (composes with Phase Ledger).
- [ ] Migration: existing databases work unchanged; queues without a
      `_honker_queues` row use hard-coded defaults exactly as today.

### Acceptance

- [ ] A queue configured with `default_max_attempts=5`, called with
      `enqueue(payload)` (no per-call attempts), produces a job with
      `max_attempts=5`.
- [ ] Same queue, `enqueue(payload, max_attempts=2)` produces a job
      with `max_attempts=2`. Per-call wins.
- [ ] An older database (no `_honker_queues` table) opens, bootstrap
      adds the table, existing queues without rows continue to use
      hard-coded defaults. No data loss.
- [ ] `dead_letter_queue` set to `"emails-dlq"`: a job that exhausts
      retries lands in `_honker_dead` *and* an enqueue fires into
      `emails-dlq`. Atomic with the dead transition.

### Non-goals

- No per-queue concurrency limit (worker-side concern; honker doesn't
  own the worker pool).
- No per-queue rate limit. `_honker_locks` already exists for
  rate-limit-ish patterns; revisit if it's not enough.
- No quota / capacity limits per queue. SQLite is the limit.

## Phase DiMaggio — Adaptive Polling Backoff

> After: Phase Echo · Before: 1.0 release prep

DiMaggio's hitting streak was 56 games of consistent contact — pace
matching context. The default polling backend currently runs at a
fixed 1 ms cadence regardless of activity: idle databases burn the
same CPU as busy ones. Recurring HN ask: "wind back the polling to
once a second when nothing is happening."

This is a change to the **default** backend (not the experimental
ones). Affects everyone, no opt-in. Addresses both the polling-
overhead critique and the mobile / battery concern in one move.

### Scope

- [ ] Track `time_since_last_wake` in `run_poll_loop`.
- [ ] If > `IDLE_THRESHOLD_S` (default 5 s), scale poll interval up
      geometrically (1 ms → 2 → 4 → ... cap at `MAX_POLL_INTERVAL_MS`,
      default 1000 ms).
- [ ] Reset to 1 ms on any wake (commit observed via PRAGMA, or any
      conservative-wake path).
- [ ] Configurable via `WatcherConfig::idle_threshold_s` and
      `max_poll_interval_ms` (defaults sensible; users don't need
      to think about it).
- [ ] Wire through Python + Node bindings (other bindings track
      under Phase Echo).

### Acceptance

- [ ] Idle database: after `IDLE_THRESHOLD_S` of no commits, poll
      interval has scaled up to MAX. Verified via test that times
      tick cadence.
- [ ] First commit after idle: wake latency stays bounded by
      MAX_POLL_INTERVAL_MS (worst case 1 s on default config).
      Tested for both polling and shm backends (kernel doesn't poll).
- [ ] CPU benchmark: idle 1-database process drops from ~3.5 ms/sec
      busy work to ~3.5 µs/sec at the cap.
- [ ] No effect on a sustained-load workload — the fast path stays
      at 1 ms while wakes are arriving.

### Non-goals

- No per-database tuning of the curve. One geometric backoff serves
  every workload; users who need different latency reach for the
  experimental kernel/shm backends.
- No "predictive" wake (e.g. learn the workload's commit cadence and
  pre-empt). Linear backoff is enough.
- Doesn't replace the experimental backends. Polling-with-backoff
  still has a worst-case latency; users who can't tolerate that opt
  in to kernel/shm.

## Phase Berra — HN-Feedback Docs Sweep

> After: Phase Mantle · Before: 1.0 release prep

Yogi Berra called pitches; this phase calls out the docs that need
fixing now that the second HN front-page hit + 970 stars + the JVM
binding shipped. Pure docs work, but high-leverage at this stage of
adoption.

### Scope

- [ ] **README prior-art line**: Oban supports SQLite directly now
      (raised by @arlobish on HN). Update to acknowledge it as a
      real comparison, not a pure Postgres-only system. Add Graphile
      Worker (raised by @odie5533).
- [ ] **README benchmark table**: publish the numbers from
      `bench/wal_index_methods` and any cross-process latency we've
      measured. The "no benchmarks" criticism (raised by
      @andrewstuart on HN) is fair — we have data, we just haven't
      surfaced it.
- [ ] **"When NOT to use" section** in README and docs index. At
      minimum: mobile / battery context (raised by @ncruces on HN),
      and a clear statement of the multi-process WAL story to
      defang the "SQLite isn't concurrent" critique.
- [ ] **JVM binding mention** in README + docs site nav. Currently
      shipped but not visible. Update the "Bindings:" line and add
      to honker.dev nav.
- [ ] **Cross-link to docs.honker.dev/reference/watcher-backends**
      from any README mention of polling, so the polling-overhead
      thread on HN has a concrete answer in our README.

### Non-goals

- No README rewrite. The existing pitch is working (970 stars).
- No marketing/positioning change. Just close the specific gaps the
  HN audience pointed at.

## Phase Robinson — Native Turso Engine Backend

> After: 1.0 release prep · gated on upstream Turso

Jackie Robinson crossed into a league that wasn't open to him. This
phase gets honker running on **Turso** (the Rust rewrite of SQLite, the
engine [pgmicro](https://github.com/glommer/pgmicro) is built on), which
exposes none of the primitives honker relies on. Origin: a thread with
@glcst (Turso CEO) asking to integrate honker natively and expose
`LISTEN`/`NOTIFY`/`pg_notify()` through pgmicro —
https://x.com/LeMikaelF/status/2059589419447750957.

Tracking issue: #59. Upstream Turso issue:
[tursodatabase/turso#7397](https://github.com/tursodatabase/turso/issues/7397).

### State (do not lose this thread)

- **Upstream is the gate and will take time.** Turso has no
  `PRAGMA data_version`, no `.so` loadable extensions, and no
  `update_hook`/`commit_hook`. honker's three SQLite wake paths
  (data_version polling, extension load, shm fast-path) are all dead on
  the rewrite — Turso's WAL index is a Turso-specific `-tshm` format, not
  SQLite's `-shm`.
- **Replacement found + proven.** The WAL's shared `max_frame` counter is
  the `data_version` analog. The public `Connection::wal_state().max_frame`
  reads the **wrong** counter (a per-connection local mirror that never
  refreshes cross-process for an idle reader). A cross-process spike
  confirmed it: 0 wakes via `wal_state()`, 20/20 wakes via the
  shared-authority value (`Wal::get_max_frame_in_wal()`).
- **~26-line additive patch** exposing `Connection::wal_max_frame_in_wal()`
  is written and working on a Turso fork; PR is held pending maintainer
  confirmation in #7397 (also asked whether they'd prefer a real push
  commit-hook instead of poll).
- **Multi-process is opt-in on Turso**:
  `DatabaseOpts::new().with_multiprocess_wal(true)` — default open takes an
  exclusive file lock. `TursoEngine` must set this.

### Scope

- [ ] **`Engine` seam** — abstract honker-core off the concrete rusqlite
  `Connection`. `honker_ops.rs` takes `&dyn Engine`. The whole
  engine difference collapses into `Engine::commit_version()`:
  SQLite reads `PRAGMA data_version`, Turso reads `wal_max_frame_in_wal()`.
  This is the main internal refactor and is **independent of upstream** —
  can start now.
- [ ] `SqliteEngine` — today's rusqlite path behind the new trait; serves
  SQLite **and libSQL** (a free third target — real `.so` + data_version).
- [ ] `TursoEngine` — native `turso` crate. Opens with
  `with_multiprocess_wal(true)`; wake via `wal_max_frame_in_wal()`. Async
  is contained: it owns its IoWorker thread and `block_on`s internally,
  exposing the same blocking `Engine` API. honker-core and every binding
  stay sync.
- [ ] `run_poll_loop` reads `engine.commit_version()` instead of issuing
  `PRAGMA data_version` directly.
- [ ] Binding `open()` gains an `engine=` selector (mirrors
  `watcher_backend=`); default `sqlite`.
- [ ] *(later, separate)* Turso-native extension surface via
  `register_extension!` for the "any Turso client / ORM-owned connection"
  story. `notify()` can't be a stateless scalar there (no DB handle) — it
  becomes a virtual table (vtabs get `Arc<Connection>`). Not needed for the
  honker-core-driven first cut.

### Acceptance

- [ ] Cross-process wake suite passes under `TursoEngine` (writer process
  commits, reader process wakes) — the same proof shape honker requires of
  every backend.
- [ ] Queue claim/ack and notify/listen round-trip on Turso in WAL mode.
- [ ] SQLite path is byte-for-byte unchanged behavior behind the new trait
  (existing tests pass without modification).
- [ ] Release-build wake latency on Turso measured and documented vs the
  ~1–2 ms SQLite baseline (spike saw ~9–10 ms debug; needs real numbers).

### Non-goals

- **MVCC / concurrent-writes mode.** honker-on-Turso starts in WAL
  (non-MVCC). `max_frame`/CDC live in WAL mode; the single-writer claim/ack
  model is unchanged. Concurrent-write support is a separate phase that
  would need a different wake primitive (a commit broadcast).
- **CDC as the wake source.** `turso_cdc` is poll-only, per-connection
  opt-in, and off under MVCC — can't passively observe arbitrary app
  commits. Possibly a typed change-feed feature later; not the wake path.
- **Forking Turso.** The only upstream need is the small additive accessor
  (and ideally, later, a push commit-hook). Both are public-API-shaped and
  upstreamable; honker does not vendor a patched engine.

## Release Automation


This is not 1.0 prep. The goal is simpler: make normal releases boring.

- One tag should drive extension artifacts and package publishes.
- Build and attach loadable extension binaries for supported platforms.
- Publish Python wheels with maturin for the supported Python/platform
  matrix.
- Publish npm/Bun packages with native artifacts where needed.
- Publish crates for `honker-core`, `honker-extension`, and
  `honker-rs`.
- Publish NuGet, Ruby, and other maintained binding packages from the
  in-tree `packages/` directories.
- Keep release notes tied to `CHANGELOG.md`.

## Later 1.0 Prep

- Maturin wheels: Python 3.11 / 3.12 / 3.13 across Linux, macOS, and
  Windows where supported.
- npm publish with napi-rs prebuilds.
- Crate publish flow for `honker-core`, `honker-extension`, and
  `honker-rs`.
- Health / observability primitives: claim depth, DLQ rate, update
  watcher firing rate, and optional OpenTelemetry integration.
- Crash-recovery tests beyond writer kills: listener-process kills,
  bridge-thread panics, mid-checkpoint disk-full.

## Perf

- Shave PyO3 / mutex / GIL overhead off single-tx paths only if new
  measurements show it matters. Current batched workloads already
  clear the target throughput.
- Consider cached `PRAGMA data_version` polling if the current
  update watcher ever shows up in profiles.
- Keep the `mmap` wal-index reader as a research path, not a default
  runtime path, until cross-platform correctness is proven.
- Stream consumer groups: competing consumers within a named group
  with a shared advancing offset. No schema change expected.

## Docs

- Publish benchmark baselines for reference hardware beyond the
  M-series release-build numbers in `bench/README.md`.
- Keep the docs site aligned with the package README wake-path text:
  update watcher, `PRAGMA data_version`, WAL recommended but not a
  correctness requirement.
- Add cookbook recipes for FastAPI / Django / Flask / Express /
  Rails instead of reviving framework packages by default.
