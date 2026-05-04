# Honker Bindings

This matrix describes wake behavior for maintained bindings. Table /
SQL compatibility is separate, but every maintained binding now routes
blocking wake waits through `honker-core`.

| Binding | Wake implementation | Backend option | `kernel` / `shm` status |
| --- | --- | --- | --- |
| Python | `honker-core::SharedUpdateWatcher` | `honker.open(..., watcher_backend=...)` | Supported in source builds with matching Cargo features; hard error when unavailable |
| Node | `honker-core::SharedUpdateWatcher` | `open(path, maxReaders?, watcherBackend?)` | Supported in source builds with matching Cargo features; hard error when unavailable |
| Rust wrapper | `honker-core::SharedUpdateWatcher` | `OpenOptions::watcher_backend(...)` | Supported when compiled with matching Cargo features; hard error when unavailable |
| Go | Extension C ABI -> `honker-core::SharedUpdateWatcher` | `OpenWithOptions(..., OpenOptions{WatcherBackend: ...})` | Supported when the loaded extension is built with matching features; hard error when unavailable |
| Bun | Extension C ABI -> `honker-core::SharedUpdateWatcher` | `open(..., { watcherBackend })` | Supported when the loaded extension is built with matching features; hard error when unavailable |
| C++ | Extension C ABI -> `honker-core::SharedUpdateWatcher` | `Database(path, ext_path, watcher_backend)` | Supported when the loaded extension is built with matching features; hard error when unavailable |
| .NET | Extension C ABI -> `honker-core::SharedUpdateWatcher` | `OpenOptions.WatcherBackend` | Supported when the loaded extension is built with matching features; hard error when unavailable |
| Ruby | Extension C ABI -> `honker-core::SharedUpdateWatcher` | `watcher_backend:` | Supported when the loaded extension is built with matching features; hard error when unavailable |
| Elixir | Extension SQL watcher handles -> `honker-core::SharedUpdateWatcher` | `watcher_backend:` | Supported when the loaded extension is built with matching features; hard error when unavailable |

Core API parity:

| Binding | Transactional outbox | Proof |
| --- | --- | --- |
| Python | `db.outbox(...)` | `tests/test_parity.py` / watcher e2e suites |
| Node | `db.outbox(...)` | `packages/honker-node/test/parity.test.js` |
| Rust wrapper | `db.outbox(...)` | `packages/honker-rs/tests/surface.rs` |
| Go | `db.Outbox(...)` | `packages/honker-go/honker_test.go` |
| Bun | `db.outbox(...)` | `packages/honker-bun/test/parity.test.ts` |
| C++ | `db.outbox(...)` | `packages/honker-cpp/test/test_parity.cpp` |
| .NET | `db.Outbox(...)` | `packages/honker-dotnet/tests/Honker.Tests/BindingTests.cs` |
| Ruby | `db.outbox(...)` | `packages/honker-ruby/spec/parity_spec.rb` |
| Elixir | `Honker.outbox(...)` | `packages/honker-ex/test/parity_test.exs` |

Contract:

- Omitted backend, `"polling"`, and `"poll"` select polling/default
  behavior. Backend names are exact; case and whitespace are not
  normalized.
- Unknown backend names are errors everywhere.
- Explicit experimental backend requests must never silently fall back
  to polling.
- A binding may claim `kernel` / `shm` support only when it routes wake
  waits through `honker-core` and has backend-isolation tests that
  cannot pass via fallback polling.
