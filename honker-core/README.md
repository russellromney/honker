# honker-core

Shared Rust foundation for [Honker](https://honker.dev) bindings. Not intended for direct use — consumed by `honker-extension` (the SQLite loadable extension), the Python binding, `honker-node`, `honker-rs`, and future bindings.

If you want to *use* Honker:

| Language | Package | Repo |
|---|---|---|
| Any SQLite client | `honker-extension` on crates.io (or prebuilt `.dylib`/`.so`) | [russellromney/honker](https://github.com/russellromney/honker) |
| Rust | `honker` on crates.io | [packages/honker-rs](https://github.com/russellromney/honker/tree/main/packages/honker-rs) |
| Python | `pip install honker` | [packages/honker](https://github.com/russellromney/honker/tree/main/packages/honker) |
| Node | `@honker/node` on npm | [packages/honker-node](https://github.com/russellromney/honker/tree/main/packages/honker-node) |
| Go | `go get github.com/russellromney/honker/packages/honker-go` | [packages/honker-go](https://github.com/russellromney/honker/tree/main/packages/honker-go) |
| Ruby | `gem install honker` | [packages/honker-ruby](https://github.com/russellromney/honker/tree/main/packages/honker-ruby) |
| Elixir | `{:honker, "~> 0.6"}` | [packages/honker-ex](https://github.com/russellromney/honker/tree/main/packages/honker-ex) |
| Bun | `bun add honker-bun` | [packages/honker-bun](https://github.com/russellromney/honker/tree/main/packages/honker-bun) |

## What this crate provides

- `open_conn(path, install_notify)` — open a SQLite connection with Honker's PRAGMA defaults and optional `notify()` install.
- `attach_notify(conn)` — register the `notify()` SQL scalar + `_honker_notifications` table.
- `attach_honker_functions(conn)` — register every `honker_*` SQL scalar (queues, streams, scheduler, rate limits, locks, results).
- `bootstrap_honker_schema(conn)` — idempotent DDL for all `_honker_*` tables.
- `Writer` / `Readers` — single-writer slot + bounded reader pool with blocking and non-blocking acquire.
- `SharedUpdateWatcher` — one PRAGMA-polling thread per database fanning out to N subscribers.
- `cron::next_after_unix(expr, from_unix)` — standard 5-field crontab parser + next-fire calculator with local-TZ + DST handling.

## License

Apache-2.0.
