# Adversarial Review Playbook (honker)

Use this when you want a serious bug hunt of honker, especially after
large LLM-written or LLM-assisted Rust changes to `honker-core` /
`honker-extension`, or to the SQL contracts shared by every binding.

This is not product documentation. This is an agent prompt/playbook.
It is adapted from the Tina-rs adversarial-review playbook to honker's
risk boundaries: SQLite as the substrate, a single-writer queue/stream,
a 1 ms commit watcher, a leader-elected scheduler, and an FFI surface
that every language binding inherits.

## Copy-Paste Invocation

```text
Read .intent/review/adversarial-review-playbook.md, then run the full
honker adversarial review process against the current checkout. Produce
findings using the output contract. Include a track coverage map and a
ranked top-10 fix list.
```

## Goal

Find real bugs.

Do not summarize the repo. Do not bikeshed style. Do not stop after
grepping for `unwrap`, `expect`, `panic`, or `todo`.

Assume some code may be LLM-written: plausible, idiomatic-looking, and
tested on happy paths, but wrong at boundary conditions, lifecycle law,
failure paths, capacity semantics, or cross-process invariants.

The single most important framing for honker: **the happy path is a live
worker that catches its own failure in-process and calls
`ack`/`retry`/`fail`. The dangerous path is the one the design exists
for — the process that is hard-killed, the clock that jumps, the file
that is replaced, the second process that races the first.** Hunt there.

## Lessons Carried Over

Prior Tina reviews found the most important bugs at boundaries, not in
the middle of functions:

- Where a field name promises truth that code never checks.
- Where "bounded" means bounded callers but not bounded work.
- Where a terminal error silently becomes a timeout (or never settles).
- Where one traffic class starves another.
- Where tests prove helper internals but not user-visible behavior.
- What happens with duplicate, malformed, repeated, early, late,
  oversized, zero-sized, or split input.

The second pass mattered. Always run it.

## Review Process

### Phase 1: Find The Invariants

Before hunting individual bugs, write down the invariants honker claims
or implies. Check at least these:

- Every job settles exactly once with a typed terminal state, and a job
  whose worker keeps crashing eventually reaches `_honker_dead` (the
  README crash-recovery contract).
- `pending`, `processing`, dead, cancelled, expired, and reclaimed are
  never silently converted into each other.
- A claim is owned by exactly one worker until its visibility window
  elapses; `attempts`/`max_attempts` actually bound retries.
- "Atomic with your business write" means commit-together /
  rollback-together for queue/stream/notify rows.
- A committed update wakes every relevant subscriber within ~1 ms; a
  missed wake is a silent correctness bug, an extra wake is free.
- The commit watcher tells the truth about file identity: if the db is
  replaced, it fails loudly and every subscriber learns — it never sits
  on stale data, and it never takes the host process down with it.
- Streams are at-least-once: a consumer past offset N never skips an
  offset <= N that later becomes visible.
- Locks grant to one owner; a lease that is "held" is not silently
  expired; rate limits grant at most `limit` per window.
- Cron/`@every` boundaries are deterministic, strictly advancing, and
  match the documented "standard Unix cron" semantics (including the
  day-of-month / day-of-week rule).

### Phase 2: Run Specific Tracks

Review by track. Each track should trace real data/control flow across
module and process boundaries, not just one function.

**Track A — Queue lifecycle & exactly-once.** `claim_batch`, `ack`,
`ack_batch`, `retry`, `fail`, `cancel`, `heartbeat`, `sweep_expired`.
Trace every job to a terminal state for both the live-worker path and
the hard-kill path. Look for reclaim that ignores `max_attempts`,
attempts that grow without bound, dead-letter that only the happy path
reaches, claims that double-settle, and `claim_expires_at` boundary
off-by-ones.

**Track B — Scheduler & cron (the protocol law).** `cron.rs`,
`scheduler_register/tick/soonest/update/pause/resume`. Treat standard
cron as the oracle: day-of-month vs day-of-week semantics, `N/step`,
ranges, DST spring-forward gaps and fall-back ambiguity, strict
advancement (no infinite loop), and catch-up behaviour after an outage.

**Track C — Wake path & subscriber lifecycle.** `run_poll_loop`,
`SharedUpdateWatcher`, `WatcherDeathGuard`, bounded channels. Look for
lost wakes across the SELECT/recv boundary, wakes lost during reconnect
or re-baseline, subscribers that hang forever (especially after the
watcher thread dies), and fan-out under backpressure.

**Track D — Cross-process atomicity.** `notify`, transactional coupling,
`lock_acquire/release`, `rate_limit_try`. The key question: which
multi-statement sequences assume a transaction they don't hold? SQLite
serializes write *transactions*, but autocommit check-then-act across
two statements does not. Hunt TOCTOU on locks and rate limits.

**Track E — Resource ownership & drop paths.** `Writer`, `Readers`,
watcher thread join/close, FFI handle lifecycle in `honker-extension`.
Look for leaked threads/connections, double free across FFI, handles
checked out of a map across a blocking wait, outstanding-count drift,
and Drop that runs in async contexts.

**Track F — Persistence, files, crash, FFI.** WAL/journal modes,
file-replacement detection, `panic=abort` interaction with the
dead-man's switch, `unsafe` in `kernel_watcher`/`shm_watcher`, native
byte order of the wal-index, NUL bytes in paths, and the C ABI in
`honker-extension`. SIGKILL mid-tx must leave a consistent db.

**Track G — Determinism & proof harness.** Wall-clock leakage into
schedule math, unstable JSON ordering, tests that assert helper
internals instead of user-visible behaviour, and contracts asserted in
the README that no test covers (dom/dow, crash-loop dead-lettering).

**Track H — SQL/ABI contracts every binding inherits.** Hand-rolled
JSON building (`json_str`), `RETURNING` column order, AUTOINCREMENT vs
plain rowid (id reuse vs cursor safety), and the `honker_*` scalar
function signatures. A bug here is a bug in ten languages at once.

**Track I — Performance as correctness.** Unbounded `_honker_notifications`
growth, catch-up enqueue storms holding the single writer, a new poll
thread per SQL watcher open, and any hot-path linear scan over history
rather than the working set.

### Phase 3: Second-Pass Truth-Gap Review

After the first findings list, run a narrower second pass. Ask:

- What did the code assume instead of enforce (a transaction, a single
  writer, a same-owner check, a `max_attempts` bound)?
- Where does a README sentence promise behaviour the code never
  implements?
- Where does "bounded" mean bounded callers but not bounded work or
  bounded catch-up?
- Where does a terminal outcome (dead-letter, lock loss) never happen on
  the crash path?
- Where can two processes both win a check-then-act?
- Where does a subscriber/handle block or leak forever instead of
  learning it is dead?

## Output Contract

For each finding, include:

1. Severity: Critical, High, Medium, or Low.
2. Confidence: High, Medium, or Low.
3. File and line reference.
4. Violated invariant or documented contract.
5. Concrete bug.
6. Why it can happen in real use.
7. Minimal reproduction or failing-test idea.
8. Small idiomatic fix.
9. Whether this looks like an LLM-style bug pattern.

Do not include style-only findings unless they hide a real bug.

If a suspected issue is false or already handled, record the proof and
the mechanism that saves it. Do not silently drop it.

## Final Report Shape

Produce:

- Short summary by risk boundary.
- Ranked top 10 fixes.
- Full findings list.
- Invariants violated.
- Areas needing deeper review.
- Suggested fuzz, property, and integration tests.
- Track coverage map: which track found which findings.

Keep the report concrete. The best finding names the exact bad state,
the line that allows it, and the test that would fail.
