# Elixir examples

Each example is runnable from `packages/honker-ex` once the Honker SQLite
extension is built:

```bash
cargo build --release -p honker-extension    # if you haven't already
HONKER_EXTENSION_PATH=../../target/release/libhonker_ext.so \
    mix run examples/basic.exs
```

Both examples default to `target/release/libhonker_ext.dylib` in the repo root,
so on macOS you can drop the env var.

| File | What it shows |
|---|---|
| [`basic.exs`](basic.exs) | `Honker.Queue.enqueue/3` → `claim_one/3` → `Honker.Job.ack/2`, draining the queue recursively |
| [`atomic.exs`](atomic.exs) | `INSERT INTO orders` + `honker_enqueue(...)` between `BEGIN IMMEDIATE` and `COMMIT`. The `ROLLBACK` path drops both. |

`atomic.exs` works in a temp directory and cleans up after itself. `basic.exs`
writes `demo.db` in the working directory, so delete it between runs if you
want a clean queue.
