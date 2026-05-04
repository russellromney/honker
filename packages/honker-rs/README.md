# honker-rs

Rust binding for [Honker](https://github.com/russellromney/honker): durable queues, streams, pub/sub, and time-trigger scheduling on SQLite.

Full docs:

- [Main repo](https://github.com/russellromney/honker)
- [Docs](https://honker.dev)

## Install

Add the crate, and make sure the Honker SQLite extension is available at runtime.

## Quick start

```rust
let db = honker::Database::open("app.db", "./libhonker_ext.dylib")?;
let q = db.queue("emails");

q.enqueue(serde_json::json!({ "to": "alice@example.com" }))?;

if let Some(job) = q.claim_one("worker-1")? {
    send_email(&job.payload);
    job.ack()?;
}
```

Delayed jobs use `run_at` / `RunAt`-style options in the binding API.

Recurring schedules use schedule expressions:

```rust
let sched = db.scheduler();
sched.add("fast", "emails", "@every 1s", serde_json::json!({ "kind": "tick" }))?;
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
