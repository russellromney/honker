# Ruby examples

Each example is runnable from `packages/honker-ruby`. A released gem ships the
extension already built; from a checkout, build it and point Honker at it:

```bash
cargo build --release -p honker-extension    # if you haven't already
HONKER_EXTENSION_PATH=../../target/release/libhonker_ext.so \
    ruby examples/basic.rb
```

| File | What it shows |
|---|---|
| [`basic.rb`](basic.rb) | `Queue#enqueue` → `claim_one` → `Job#ack` via the typed `db.queue(...)` wrapper |
| [`atomic.rb`](atomic.rb) | `INSERT INTO orders` + `honker_enqueue(...)` inside one `sqlite3` transaction on `db.db`. Raising inside the block rolls back both. |

`Queue#enqueue` runs its own SQLite write, so `atomic.rb` drops to the raw
connection to keep the business write and the job in the same transaction.

`atomic.rb` works in a `Dir.mktmpdir` block and cleans up after itself.
`basic.rb` writes `demo.db` in the working directory, so delete it between runs
if you want a clean queue.
