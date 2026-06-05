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
INSERT INTO _honker_live (queue, payload) VALUES ('emails', '{"to":"alice"}');
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
SELECT honker_enqueue('emails', '{"to":"alice@example.com"}', NULL, NULL, 0, 3, NULL);
```

The extension shares tables with the language bindings, so a Python
worker can claim jobs written by SQL, Node, Ruby, Go, or any other
binding.

## Architecture

- One `PRAGMA data_version` watcher per `Database`; the default
  Rust-backed watcher cadence is 1 ms and can be raised
- Counter change fans out a wake to each listener/worker/subscriber
- Subscribers re-read SQLite state with indexed SELECTs
- 100 subscribers still share one watcher
- Idle listeners run zero queue/notification SELECTs

Queue claim is one `UPDATE ... RETURNING` through a partial index:
`(queue, priority DESC, run_at, id) WHERE state IN ('pending','processing')`.
Ack is one `DELETE`. Retry-exhausted jobs move to `_honker_dead`, so
claim speed depends on pending/processing jobs, not old queue history.

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
