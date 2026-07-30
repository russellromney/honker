# Rust examples

Each example is runnable from `packages/honker-rs` with Cargo's `--example`
flag:

```bash
cargo run --example basic
cargo run --release --example atomic
```

| File | What it shows |
|---|---|
| [`basic.rs`](basic.rs) | `Queue::enqueue` → `claim_one` → `Job::ack` via the typed `db.queue(...)` wrapper |
| [`atomic.rs`](atomic.rs) | `INSERT INTO orders` + `q.enqueue_tx(&tx, ...)` on one `db.transaction()`. Rollback drops both — no dual-write, no outbox table. |

`atomic.rs` runs against a `tempfile::tempdir()` and asserts the committed and
rolled-back counts, so it doubles as a smoke test. `basic.rs` writes `demo.db`
in the working directory, so delete it between runs if you want a clean queue.
