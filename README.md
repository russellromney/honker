<h1 align="center">
  <img src="assets/honker-logo.png" width="120" alt="" /><br/>honker
</h1>

`honker` is a SQLite extension + language bindings that add
Postgres-style `NOTIFY`/`LISTEN` semantics to SQLite, with built-in
durable pub/sub, task queue, and event streams, without client polling
or a daemon/broker. Any language that can
`SELECT load_extension('honker')` gets the same features.

`honker` replaces queue-table polling with a single-digit-microsecond
`PRAGMA data_version` read. The default watcher checks every 1 ms,
giving push-like semantics and single-digit-millisecond cross-process
delivery; raise the watcher interval when lower idle CPU matters more
than lowest-latency wakeups.

If SQLite is your primary datastore, the queue should live in the same
file. `INSERT INTO orders` and `queue.enqueue(...)` can commit in the
same transaction. Rollback drops both.

See [honker.dev](https://honker.dev) for guides and API details, and
[Binding support](BINDINGS.md) for what each binding supports.

[Simon Willison highlighted honker](https://simonwillison.net/2026/Apr/24/honker/)
as a SQLite implementation of the transactional outbox pattern.

> Alpha software. Better than experimental, not beta-quality yet.

## Quick Start

```bash
pip install honker
```

```python
import honker

db = honker.open("app.db")
emails = db.queue("emails")

with db.transaction() as tx:
    tx.execute("INSERT INTO orders (user_id) VALUES (?)", [42])
    emails.enqueue({"to": "alice@example.com"}, tx=tx)

async for job in emails.claim("worker-1"):
    send_email(job.payload)
    job.ack()
```

The enqueue is atomic with the order insert. A worker in another process
wakes when the transaction commits.

## What It Does

- Notify/listen across processes on one SQLite `.db` file
- Durable at-least-once queues with retries, delayed jobs, priority,
  visibility timeouts, dead-letter rows, and task result storage
- Durable streams with per-consumer offsets
- Time-trigger scheduling with cron and `@every <duration>` expressions
- Named locks, rate limits, and transactional outbox helpers
- SQL functions through a SQLite loadable extension
- Thin bindings for Python, Node.js, Rust, Go, Ruby, Bun, Elixir, C++,
  .NET / C#, Java/JVM, and Kotlin

Deliberately not included: workflow DAGs, task chains/groups/chords,
multi-writer replication, or distributed locking across machines.

## Why

SQLite is increasingly the database for shipped projects. Those projects
eventually need pub/sub and a task queue. The usual answer is "add Redis
+ Celery." That works, but it introduces a second datastore with its own
backup story, a dual-write problem between your business table and the
queue, and a broker to run.

Honker takes the approach that if SQLite is the primary datastore, the
queue should live in the same file. The queue is just rows in a table
with a partial index. Every binding uses the same schema and extension,
so one language can enqueue work and another can claim it.

## Design

Honker is built around three pieces:

- ephemeral pub/sub with `notify()` / `listen()`
- durable streams with per-consumer offsets
- at-least-once queues with visibility timeouts and retries

All three are INSERTs inside your transaction. Put `queue.enqueue(...)`,
`stream.publish(...)`, or `notify(...)` beside the write that created the
work. Commit lands both rows. Rollback drops both rows.

Payloads are JSON. Queue enqueue, stream publish, notifications, task results,
and scheduler register/update accept any JSON value that decodes: objects,
arrays, strings, numbers, booleans, or `null`. Typed binding APIs serialize
values where documented. Raw JSON APIs and direct SQL calls require serialized
JSON text (for example, `'{"id":42}'`, `'[1,2]'`, or `'"ready"'`); non-JSON text
is rejected.

Validation parses the payload the same way the read side does, so anything that
enqueues will decode. That rules out a few shapes the JSON grammar allows but no
decoder can represent: lone surrogates such as `"\ud800"`, numbers outside f64
range such as `1e999`, and nesting deeper than 128 levels. These are rejected at
the producer rather than surfacing as an undecodable job in another process.

SQLite has no server-side push channel, so honker uses a shared watcher.
The stable backend reads `PRAGMA data_version` every millisecond; when
the counter changes, listeners re-read indexed SQLite state.

If you use your app's existing SQLite file, honker wakes workers on
every commit to that file. Most wakes will not find work for a given
queue or channel. That overtriggering is on purpose: one indexed SELECT
is cheap, while a missed wake is a correctness bug. The stable semantics
are:

- wake on committed updates
- ignore rolled-back work
- re-read SQLite state after every wake
- use file-backed SQLite databases, not `:memory:`

Optional source-build backends also exist for kernel file events and WAL
shared-memory reads. See [Binding support](BINDINGS.md) for which
bindings expose backend options and what CI proves.

Honker is single-machine and file-backed. SQLite's locking model is
designed for one host writing one database file; two servers writing the
same `.db` over NFS is not a Honker deployment strategy.

## Prior Art

[`pg_notify`](https://www.postgresql.org/docs/current/sql-notify.html)
gives Postgres fast triggers, but no retry or visibility timeout.
[pg-boss](https://github.com/timgit/pg-boss) and
[Oban](https://hexdocs.pm/oban/) are the Postgres-side gold standards
we're chasing on SQLite. [Huey](https://github.com/coleifer/huey) is an
excellent SQLite-backed Python task queue. If you already run Postgres,
use the Postgres tools, as they are excellent.

The transactional outbox idea also owes a lot to Brandur Leach's
[Transactionally Staged Job Drains in Postgres](https://brandur.org/job-drain):
write the job row in the same transaction as the business row, then let
a worker deliver it after commit.

## Bindings

| Ecosystem | Package / path | Notes |
| --- | --- | --- |
| Python | `pip install honker` | Batteries-included package; includes the Python API and loadable extension in release wheels |
| Node.js | `npm install @russellthehippo/honker-node` | Native Node binding |
| Ruby | `gem install honker` | Native gem with precompiled platforms where available |
| .NET / C# | `dotnet add package Honker` | NuGet package with bundled runtime assets |
| Rust | `honker`, `honker-core`, `honker-extension` | Core engine and Rust wrapper |
| Elixir | Hex package `honker` | Extension-backed Elixir binding |
| Go, Bun, C++, JVM, Kotlin | in `packages/` | Maintained in-tree bindings |
| SQLite | `honker-extension` | Loadable extension for any SQLite 3.9+ client |

The detailed parity table lives in [BINDINGS.md](BINDINGS.md).
Language-specific install and API notes live in each package README.

## SQL Extension

Any SQLite client that can load extensions can use honker directly:

```sql
.load ./libhonker_ext
SELECT honker_bootstrap();
SELECT honker_enqueue('emails', '{"to":"alice"}', NULL, NULL, 0, 3, NULL);
SELECT honker_claim_batch('emails', 'worker-1', 32, 300);    -- JSON array
SELECT honker_ack_batch('[1,2,3]', 'worker-1');              -- DELETEs; returns count
SELECT honker_sweep_expired('emails');                       -- count moved to dead
SELECT honker_lock_acquire('backup', 'me', 60);              -- 1 = got it, 0 = held
SELECT honker_lock_release('backup', 'me');                  -- 1 = released
SELECT honker_rate_limit_try('api', 10, 60);                 -- 1 = under, 0 = at limit
SELECT honker_rate_limit_sweep(3600);                        -- drop windows >1h old
SELECT honker_cron_next_after('0 3 * * *', unixepoch());     -- 5-field cron
SELECT honker_cron_next_after('*/2 * * * * *', unixepoch()); -- 6-field cron
SELECT honker_cron_next_after('@every 5s', unixepoch());     -- interval schedule
SELECT honker_scheduler_register('nightly', 'backups',
  '0 3 * * *', '"go"', 0, NULL);                             -- periodic task
SELECT honker_scheduler_tick(unixepoch());                   -- JSON: fires due
SELECT honker_scheduler_soonest();                           -- min next_fire_at
SELECT honker_queue_next_claim_at('emails');                 -- next run/reclaim deadline
SELECT honker_stream_publish('orders', 'k', '{"id":42}');    -- returns offset
SELECT honker_stream_read_since('orders', 0, 1000);          -- JSON array
SELECT honker_stream_save_offset('worker', 'orders', 42);    -- monotonic upsert
SELECT honker_stream_get_offset('worker', 'orders');         -- offset or 0
SELECT honker_result_save(42, '{"ok":true}', 3600);          -- save w/ 1h TTL
SELECT honker_result_get(42);                                -- value or NULL
SELECT honker_result_sweep();                                -- prune expired
SELECT notify('orders', '{"id":42}');
```

The extension shares tables with the language bindings, so a Python
worker can claim jobs written by SQL, Node, Ruby, Go, or any other
binding.


### Fenced completion (attempt token)

`honker_claim_batch` returns each job's `attempts`. That value is the
claim's fencing token: pass it as the last argument to finish the job.

```sql
SELECT honker_ack(7, 'worker-1', 3);                        -- 1 = done
SELECT honker_retry(7, 'worker-1', 30, 'timeout', 3);       -- 1 = retried or dead
SELECT honker_fail(7, 'worker-1', 'rejected', 3);           -- 1 = moved to dead
SELECT honker_heartbeat(7, 'worker-1', 300, 3);             -- 1 = lease extended
SELECT honker_ack_batch('[[7,3],[8,1]]', 'worker-1');       -- [id, attempt] pairs
```

A fenced call acts only if the row is still that claim: same id, worker
id and `attempts`, and still `processing`. It does not check the lease.
A reclaim increases `attempts`, and dead-lettering, expiry and cancel
remove the row, so:

- A stale handler gets 0, even when a restarted worker with the same
  worker id has reclaimed the job.
- A handler that overran its lease still completes if nobody reclaimed
  the job. The job does not run again. A fenced heartbeat in that state
  sets `claim_expires_at = now + extend_s` again.

The shorter forms (`honker_ack(id, worker_id)`, `honker_retry(id,
worker_id, delay_s, error)`, `honker_fail(id, worker_id, error)`,
`honker_heartbeat(id, worker_id, extend_s)`, and plain ids in
`honker_ack_batch`) are unchanged and **unfenced**. They check the worker
id and an unexpired lease. A stale handler that shares the new holder's
worker id passes that check and can ack, retry or fail the newer attempt
(issue #176). Use the fenced forms when worker ids can repeat, for
example a worker restarted with a fixed id while its old process still
runs.

### Job states and claim

A row in `_honker_live` is in one of three states:

- `scheduled`: `run_at` is in the future. Enqueue with a delay, or a
  retry with `delay_s > 0`, writes this state.
- `pending`: ready to claim.
- `processing`: claimed. The lease ends at `claim_expires_at`.

Every `honker_claim_batch` reads the clock once, then does three bounded
housekeeping steps before it claims, each capped at 1000 rows per call:

1. Move due `scheduled` rows to `pending`.
2. Move expired jobs (`expires_at` passed) to `_honker_dead` with
   `'expired'`. This includes in-flight jobs whose lease has lapsed. A
   job whose lease is still valid is left to its worker.
3. Move lapsed leases with no attempts left to `_honker_dead` with
   `'max attempts exceeded'`.

It then claims `pending` rows and lapsed leases in `priority DESC,
run_at, id` order. A lapsed lease stays `processing` until it is
reclaimed, so a fenced late ack still works until then. Because the
claim expires jobs itself, `honker_sweep_expired` is optional. Use it to
clear a queue that no worker claims from.

`max_attempts` must be at least 1. `honker_enqueue`,
`honker_scheduler_register` and `honker_scheduler_update` reject 0 and
negative values with an error.

If you INSERT into `_honker_live` directly with a future `run_at`, also
set `state = 'scheduled'`. A `pending` row with a future `run_at` is not
claimed early, but it sits in the ready index and the claim has to step
over it.

### Scheduler tick

`honker_scheduler_tick(now)` enqueues one job for every due boundary of
every enabled schedule and advances its `next_fire_at`. A schedule that
fell more than 64 boundaries behind fires 64 and skips to the next
boundary after `now`.

A tick is atomic on its own. It needs no surrounding transaction, and
any number of processes can tick at once:

- Its first statement takes the write lock, so a second tick waits on
  `busy_timeout` and then sees the advanced `next_fire_at`. Each boundary
  is enqueued once.
- If any enqueue fails, the tick returns the error and changes nothing:
  no job, no advance. The next tick fires those boundaries.
- A schedule whose stored payload is not valid JSON (a row written by raw
  SQL or by a build before the JSON payload contract) is not enqueued and
  does not fail the tick. Each of its due boundaries becomes one
  `_honker_dead` row whose `last_error` names the schedule, and its
  `next_fire_at` advances. Other schedules fire normally. Fix it with
  `honker_scheduler_update`.
- Inside a deferred `BEGIN`, call it before the transaction reads
  anything (or use `BEGIN IMMEDIATE`). Then a commit from another
  connection cannot fail it with "database is locked".

Before this, a tick read the due schedules before it wrote. Two ticks
could both enqueue the same boundary, and a failed tick in autocommit
left jobs behind without advancing, so the next tick enqueued them again
(issue #173).

### SQL call context

`honker_claim_batch`, `honker_fail`, `honker_sweep_expired`,
`honker_scheduler_tick`, and a `honker_retry` that moves the job to
`_honker_dead` (its attempts are used up) must run as a **separate SELECT**, after any write cursors on that
connection are finished. Do not call them inside a trigger, an
INSERT/UPDATE/DELETE, or a RETURNING expression. An unfinished
`INSERT ... RETURNING` cursor also blocks them, even when the call itself
is a separate SELECT.

These operations use a savepoint so that a failure partway through cannot
lose the job. SQLite cannot open a savepoint while a write statement is
active, and the error says so. A larger `busy_timeout` does not help:
finish or close the write cursor. A `honker_retry` that puts the job back
to pending is a single UPDATE with no savepoint, so it works in those
contexts. The branch depends on the job's attempts, so if a call might be
the final attempt, run `honker_retry` as a separate SELECT too.

An explicit application transaction is supported and keeps all work
atomic. Use `BEGIN IMMEDIATE`: it takes the write lock up front, so other
connections' commits cannot fail the transaction partway through.

```sql
BEGIN IMMEDIATE;
UPDATE app_orders SET status = 'failed' WHERE id = 42;
-- If using RETURNING above, finish its cursor before the next statement.
SELECT honker_fail(7, 'worker-1', 'delivery rejected');
COMMIT;
```

The application must roll back the transaction on an error. Versions
before savepoint-protected job transitions accepted these calls inside DML;
keep them as separate statements instead.

### How long has this job been running

`_honker_live.claimed_at` is when the CURRENT claim started. Nothing
else answers that: `created_at` includes queue wait, `run_at` is when
the job became ready, and `claim_expires_at` moves on every heartbeat.
No language binding exposes `claimed_at` yet, so read it in SQL:

```sql
SELECT id, queue, unixepoch() - claimed_at AS running_s
  FROM _honker_live
 WHERE state = 'processing'
   AND claim_expires_at >= unixepoch();   -- required, see below
```

`claimed_at` is only meaningful while `claim_expires_at >= unixepoch()`.
When a claim lapses and nobody reclaims the job yet (no worker on that
queue, or the queue is idle), honker does not touch the row.
`worker_id`, `claim_expires_at` and `claimed_at` all stay put and all go
stale together. The next claim on that queue either reclaims the job and
overwrites all three, or, if the job has no attempts left or has
expired, moves it to `_honker_dead`. Without the
`claim_expires_at` filter, `unixepoch() - claimed_at` keeps counting up
for an attempt nobody is running.

`claimed_at` is NULL until the first claim, and stays NULL for jobs that
were already in flight when an existing database was upgraded: the
migration adds the column without backfilling, because a backfill would
date those rows to upgrade time and read as a claim that never happened.
They pick up a real value on their next claim.


### Upgrading workers for `claimed_at`

Use a stop/upgrade/resume cutover for all Honker processes sharing a database.
Do not run old and new queue workers together if you rely on claim timestamps:
old retry code does not clear `claimed_at`, and old claim code does not set it.
A valid lease can therefore carry a timestamp from an earlier attempt.

1. Stop every old producer, worker, scheduler, and other Honker process using the
   database. Let in-flight handlers finish, or stop them under your normal
   at-least-once recovery procedure.
2. Upgrade all bindings and native extensions that access that database.
3. Open it with the new code and run the normal bootstrap. Existing jobs and
   leases are preserved. Previously in-flight jobs have an unknown (`NULL`)
   claim start; no timestamp is invented for them.
4. Resume only upgraded processes. A new claim/reclaim records a fresh start.
   Until then, display `NULL` as **unknown**, not zero seconds.

If old and new workers already ran together, their affected claim times cannot
be reconstructed from the database. The same applies after a rollback: if old
code ran against the upgraded database at any point, it may have left stale
times even with no mixed fleet. In either case, with **all workers stopped**,
open a maintenance connection and mark the existing times unknown:

```sql
BEGIN IMMEDIATE;
UPDATE _honker_live SET claimed_at = NULL;
COMMIT;
```

Then resume upgraded processes. This deliberately clears even times that might
have been correct, because the database does not identify which version wrote
each row. It does not change job state, worker ownership, attempts, payload, or
lease deadlines. It does not requeue jobs or infer historical timestamps.
Never run this repair automatically on every bootstrap.

### Upgrading to the `scheduled` state (claim v2)

This release adds a job state, `scheduled`, and changes the claim
indexes. Older builds do not know the state: an old worker never claims
a `scheduled` job, and an old producer writes future jobs as `pending`.
Use the same stop/upgrade/resume cutover as above for all Honker
processes sharing a database:

1. Stop every old producer, worker, scheduler, and other Honker process using the
   database. Let in-flight handlers finish, or stop them under your normal
   at-least-once recovery procedure.
2. Upgrade all bindings and native extensions that access that database.
3. Open it with the new code and run the normal bootstrap. The first
   bootstrap migrates the database once, in one transaction: future
   `pending` jobs become `scheduled`, `pending` jobs with no attempts left
   (`max_attempts` 0 or less) move to `_honker_dead` with
   `'max attempts exceeded'`, and the old `_honker_live_claim` and
   `_honker_live_pending_deadline` indexes are dropped. In-flight jobs and
   their leases are not changed. Several processes may bootstrap at the
   same time; one migrates and the others see it done.
4. Resume only upgraded processes.

If an old process bootstraps the database again, it recreates the old
claim index. The next new bootstrap migrates again, which is safe. Code
that reads `_honker_live.state` and assumes only `pending` and
`processing` must also count `scheduled`.

## Architecture

- One `PRAGMA data_version` watcher per `Database`; the default
  Rust-backed watcher cadence is 1 ms and can be raised
- Counter change fans out a wake to each listener/worker/subscriber
- Subscribers re-read SQLite state with indexed SELECTs
- 100 subscribers still share one watcher
- Idle listeners run zero queue/notification SELECTs

Queue claim is one `UPDATE ... RETURNING` that reads the ready index
`(queue, priority DESC, run_at, id) WHERE state = 'pending'` plus lapsed
leases from `(queue, claim_expires_at) WHERE state = 'processing'`.
Scheduled and expiring jobs have their own partial indexes, and the
housekeeping steps before the claim are capped per call. Claim latency
does not grow with delayed, in-flight or expired backlog. Ack is one
`DELETE`. Retry-exhausted jobs move to `_honker_dead`, so claim speed does
not depend on old queue history.

The language bindings default to WAL because it gives concurrent readers
with one writer and efficient fsync batching. Other journal modes still
work. Correctness and cross-process wake do not depend on WAL; the wake
path is SQLite's own `data_version` counter.

## ORMs And Frameworks

Honker does not ship framework plugins. Load the extension on your
framework or ORM connection, run `honker_bootstrap()`, and call SQL
functions inside the ORM's transaction.

That works with SQLAlchemy, SQLModel, Django, Drizzle, Kysely, sqlx,
GORM, ActiveRecord, Ecto, Hibernate, jOOQ, MyBatis, and Exposed. See the
ORM guide at [honker.dev/guides/orm](https://honker.dev/guides/orm/).

## Performance

On a modern laptop, honker handles thousands of messages per second.
Cross-process wake latency is set by the watcher cadence, which defaults
to 1 ms. Measure
on your hardware with:

```bash
python bench/wake_latency_bench.py --samples 500
python bench/real_bench.py --workers 4 --enqueuers 2 --seconds 15
```

## Development

```bash
make test              # Rust + Python + Node fast path
make test-all          # broader suite, including slower tests
make build             # build Python package + loadable extension
cargo build --release -p honker-extension
```

Repo layout:

```text
honker-core/          # shared Rust engine
honker-extension/     # SQLite loadable extension
packages/             # language bindings
tests/                # cross-package integration tests
bench/                # benchmarks
```

## Docs

- [Binding support](BINDINGS.md)
- [Python examples](packages/honker/examples/README.md)
- [Benchmarks](bench/README.md)
- [Roadmap](ROADMAP.md)
- [Changelog](CHANGELOG.md)
- [honker.dev](https://honker.dev)

## License

Apache-2.0 OR MIT. See [LICENSE](LICENSE).
