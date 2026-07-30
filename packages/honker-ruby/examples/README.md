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

`Queue#enqueue` runs its own SQLite write, so it is *not* atomic with a write
you issue yourself. `atomic.rb` drops to the raw connection to show the
extension-level `honker_enqueue(...)` primitive, but you do not have to: the
binding also ships a typed transaction API that does the same thing —

```ruby
db.transaction do |tx|
  tx.execute("INSERT INTO orders (user_id, total) VALUES (?, ?)", [42, 9900])
  q.enqueue_tx(tx, { to: "alice@example.com", order_id: 42 })
end
```

`atomic.rb` works in a `Dir.mktmpdir` block and cleans up after itself.
`basic.rb` writes `demo.db` in the working directory, so delete it between runs
if you want a clean queue.
