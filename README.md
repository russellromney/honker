<h1 align="center">
  <img src="assets/honker-logo.png" width="120" alt="" /><br/>honker
</h1>

`honker` is a SQLite extension and set of language bindings for durable
queues, streams, pub/sub, and time-trigger scheduling. It gives a local
SQLite database Postgres-style `NOTIFY`/`LISTEN` behavior without Redis,
Celery, a broker, or a daemon.

If SQLite is your primary datastore, honker keeps side effects in the
same file. Business writes and queue/event/notification writes commit
together, and rollback drops both.

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

The usual way to add background work to a SQLite app is to add a second
datastore: Redis, a queue service, or Postgres. That works, but it also
adds backup, deployment, and dual-write failure modes.

Honker stores work in ordinary SQLite tables. Queue sends, stream
publishes, and notifications happen inside your existing transaction.
Every binding uses the same schema and extension, so one language can
enqueue work and another can claim it.

## Wake Behavior

SQLite has no server-side push channel, so honker uses a small shared
watcher. The stable backend reads `PRAGMA data_version` every millisecond;
when the counter changes, listeners re-read indexed SQLite state.

This intentionally wakes more listeners than strictly necessary. A
wasted wake is one cheap indexed query; a missed wake is a correctness
bug. The stable semantics are:

- wake on committed updates
- ignore rolled-back work
- re-read SQLite state after every wake
- use file-backed SQLite databases, not `:memory:`

Experimental source-build backends also exist for kernel file events and
WAL shared-memory reads. See [Binding support](BINDINGS.md) for which
bindings expose backend options and what CI proves.

## Bindings

| Ecosystem | Package / path | Notes |
| --- | --- | --- |
| Python | `pip install honker` | Batteries-included package; includes the Python API and loadable extension in release wheels |
| Node.js | `npm install @russellthehippo/honker-node` | Native Node binding |
| Ruby | `gem install honker` | Native gem with precompiled platforms where available |
| .NET / C# | `dotnet add package Honker` | NuGet package with bundled runtime assets |
| Rust | `honker`, `honker-core`, `honker-extension` | Core engine and Rust wrapper |
| Go, Bun, Elixir, C++, JVM, Kotlin | in `packages/` | Maintained in-tree bindings |
| SQLite | `honker-extension` | Loadable extension for any SQLite 3.9+ client |

The detailed parity table lives in [BINDINGS.md](BINDINGS.md).
Language-specific install and API notes live in each package README.

## SQL Extension

Any SQLite client that can load extensions can use honker directly:

```sql
.load ./libhonker_ext
SELECT honker_bootstrap();
SELECT notify('orders', '{"id":42}');
SELECT honker_enqueue('emails', '{"to":"alice@example.com"}', NULL, NULL, 0, 3, NULL);
SELECT honker_claim_batch('emails', 'worker-1', 32, 300);
```

The extension shares tables with the language bindings, so a Python
worker can claim jobs written by SQL, Node, Ruby, Go, or any other
binding.

## ORMs And Frameworks

Honker does not ship framework plugins. Load the extension on your
framework or ORM connection, run `honker_bootstrap()`, and call SQL
functions inside the ORM's transaction.

That works with SQLAlchemy, SQLModel, Django, Drizzle, Kysely, sqlx,
GORM, ActiveRecord, Ecto, Hibernate, jOOQ, MyBatis, and Exposed. See the
ORM guide at [honker.dev/guides/orm](https://honker.dev/guides/orm/).

## Performance

On a modern laptop, honker handles thousands of messages per second with
cross-process wake latency bounded by the 1 ms watcher cadence. Measure
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
