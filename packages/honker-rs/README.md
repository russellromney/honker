# honker-rs

Rust binding for [Honker](https://github.com/russellromney/honker): durable queues, streams, pub/sub, and time-trigger scheduling on SQLite.

Full docs:

- [Main repo](https://github.com/russellromney/honker)
- [Docs](https://honker.dev)

## Install

Add the crate. `honker-core` is linked in and bootstraps the schema, so
there is no `.dylib` to load at runtime. Use the loadable extension
(`honker-extension`) only when other SQLite clients share the file.

## Quick start

```rust
use honker::{Database, EnqueueOpts, QueueOpts};
use serde_json::json;

let db = Database::open("app.db")?;
let q = db.queue("emails", QueueOpts::default());

q.enqueue(&json!({ "to": "alice@example.com" }), EnqueueOpts::default())?;

if let Some(job) = q.claim_one("worker-1")? {
    let body: serde_json::Value = job.payload_as()?;
    send_email(&body);
    job.ack()?;
}
```

Runnable versions of this live in
[`examples/basic.rs`](examples/basic.rs) and
[`examples/atomic.rs`](examples/atomic.rs), which CI compiles.

Delayed jobs use `run_at` / `RunAt`-style options in the binding API.

## Job details

`Job` (a claimed unit of work) and `JobRow` (the read-only snapshot from
`Queue::get_job`) both carry every field the core claim/lookup returns:
`id`, `queue`, `payload`, `state`, `priority`, `run_at`, `worker_id`,
`claim_expires_at`, `attempts`, `max_attempts`, `created_at`, and
`expires_at`.

`Job` also holds a claim, so it has `ack`, `retry`, `fail`, and
`heartbeat`, and its `worker_id` / `claim_expires_at` are non-optional.
`JobRow` is data only: no claim methods, and `worker_id` /
`claim_expires_at` are `Option` because a pending row has neither.

Payload encoding is unchanged: `Job::payload` is `Vec<u8>` of raw JSON
bytes and `JobRow::payload` is the raw JSON `String`.

## Typed payloads

`Queue<T>` carries the payload type. `db.queue(..)` still hands back a
`Queue<serde_json::Value>`; `db.typed_queue::<T>(..)` (or
`queue.typed::<T>()`) gives you your own type:

```rust
#[derive(serde::Serialize, serde::Deserialize)]
struct Email { to: String }

let q = db.typed_queue::<Email>("emails", QueueOpts::default());
q.enqueue(&Email { to: "alice@example.com".into() }, EnqueueOpts::default())?;

if let Some(job) = q.claim_one("worker-1")? {
    let email: Email = job.payload_typed()?;
    job.ack()?;
}
```

`payload_typed()` decodes into the queue's `T`; `payload_as::<U>()`
decodes into anything you name. Both are plain `serde` deserialization.

There is no `db.queue::<Email>(..)`. Rust cannot put a default on a
function's type parameter, so making `queue` generic would break every
bare `let q = db.queue(..)`; `typed_queue` is a separate constructor
instead. `db.queue::<Email>(..)` therefore fails with `error[E0107]:
method takes 0 generic arguments` and the compiler's "remove the
unnecessary generics" hint points the wrong way — reach for
`db.typed_queue::<Email>(..)` or `q.typed::<Email>()`.

A generic helper that used to take a plain `&Queue` keeps its
signature — and so its call sites — by reinterpreting inside the body:

```rust
fn helper<P: serde::Serialize>(q: &honker::Queue, p: &P) -> honker::Result<i64> {
    q.typed::<P>().enqueue(p, EnqueueOpts::default())
}
```

**honker never checks payload shape.** The type parameter is a
compile-time convenience for your code only. The database stores the
payload as opaque JSON text and nothing on the write path validates it,
so two processes writing the same queue with different types will
produce rows the other cannot decode. The only error you get is a
`serde` failure at decode time. Keeping producers and consumers in
agreement is your job.

Recurring schedules use schedule expressions:

```rust
use honker::ScheduledTask;

let sched = db.scheduler();
sched.add(ScheduledTask {
    name: "fast".into(),
    queue: "emails".into(),
    schedule: "@every 1s".into(),
    payload: json!({ "kind": "tick" }),
    priority: 0,
    expires_s: None,
})?;
```

Supported schedule forms:

- `0 3 * * *`
- `*/2 * * * * *`
- `@every 1s`

For full API details, async wake behavior, streams, and SQL functions, see the main repo and docs site.

## Experimental watcher backends

Polling is the default. Source builds can opt into the experimental
core backends with Cargo features:

```rust
let opts = honker::OpenOptions::default().watcher_backend("kernel")?;
let db = honker::Database::open_with_options("app.db", opts)?;
```

`kernel` requires the `kernel-watcher` feature. `shm` requires
`shm-fast-path` and WAL mode. Explicit requests fail loudly when the
feature is not compiled or the backend cannot probe the database path.
