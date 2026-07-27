# Bun examples

Each example is runnable from `packages/honker-bun` once the Honker SQLite
extension is built:

```bash
cargo build --release -p honker-extension    # if you haven't already
HONKER_EXTENSION_PATH=../../target/release/libhonker_ext.so \
    bun run examples/basic.ts
```

Both examples default to `target/release/libhonker_ext.dylib` in the repo root,
so on macOS you can drop the env var.

| File | What it shows |
|---|---|
| [`basic.ts`](basic.ts) | enqueue → `claimOne` → `ack` via the typed `db.queue(...)` wrapper |
| [`atomic.ts`](atomic.ts) | `INSERT INTO orders` + `honker_enqueue(...)` committed in one `db.raw.transaction(...)`. Throwing inside the transaction rolls back both. |

`atomic.ts` works in a temp directory and cleans up after itself. `basic.ts`
writes `demo.db` in the working directory, so delete it between runs if you
want a clean queue.
