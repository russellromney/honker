# Go examples

Each example is a `main` package in its own directory. Build the Honker SQLite
extension first, then run from `packages/honker-go`:

```bash
cargo build --release -p honker-extension    # if you haven't already
go run -tags sqlite_load_extension ./examples/atomic
```

The `sqlite_load_extension` build tag is required — without it the driver
refuses to load the extension.

| File | What it shows |
|---|---|
| [`basic/main.go`](basic/main.go) | `Queue.Enqueue` → `ClaimOne` → `Job.Ack` via the typed `db.Queue(...)` wrapper |
| [`atomic/main.go`](atomic/main.go) | `INSERT INTO orders` + `honker_enqueue(...)` on one `*sql.Tx`. Rollback drops both. |

`atomic/main.go` locates the built extension under `target/release/` on its own
and works in a temp directory that it cleans up. `basic/main.go` expects
`./libhonker_extension.dylib` next to the working directory and writes
`demo.db` there, so copy or symlink the built extension before running it.
