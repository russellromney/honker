# Honker Bindings

This matrix describes wake behavior for maintained bindings. Table /
SQL compatibility is separate, but every maintained binding now routes
blocking wake waits through `honker-core` or the equivalent shared JVM
watcher.

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
| JVM `dev.honker:honker` | Shared JVM watcher | `OpenOptions.watcherBackend(...)` | Stable `AUTO`/`PRAGMA_DATA_VERSION`; explicit experimental `MMAP_SHM` and `KERNEL_EVENTS` |
| Kotlin `dev.honker:honker-kotlin` | Shared JVM watcher wrapper | JVM `OpenOptions` | Wraps JVM watcher semantics |

Core API parity:

| Binding | Package proof | Queue | Streams | Notify/listen | Scheduler | Wake behavior |
|---|---:|---:|---:|---:|---:|---|
| SQLite extension | load smoke | SQL | SQL | notify SQL only | SQL | host language must watch/read |
| Python `honker` | yes | yes | yes | yes | yes | shared Rust `UpdateWatcher` |
| Node `@russellthehippo/honker-node` | yes | yes | yes | yes | yes | shared Rust `UpdateWatcher` |
| .NET `Honker` | yes | yes | yes | yes | yes | shared Rust watcher through extension C ABI |
| Rust `honker` | CI | yes | yes | yes | yes | shared Rust `UpdateWatcher` |
| Go | CI | yes | yes | yes | yes | shared Rust watcher through extension C ABI |
| Bun `@russellthehippo/honker-bun` | CI | yes | yes | yes | yes | shared Rust watcher through extension C ABI |
| C++ | CI | yes | yes | yes | yes | shared Rust watcher through extension C ABI |
| JVM `dev.honker:honker` | local + clean consumer | yes | yes | yes | yes | shared JVM watcher |
| Kotlin `dev.honker:honker-kotlin` | local + clean consumer | wrapper | Flow wrapper | wrapper | wrapper | wraps shared JVM watcher |
| Ruby `honker` | yes | yes | yes | notify yes, listen no | yes | shared Rust watcher through extension C ABI |
| Elixir `honker` | CI | yes | yes | notify yes, listen no | yes | shared Rust watcher through extension SQL handles |

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
| JVM | `db.outbox(...)` | `packages/honker-jvm/src/test/java/dev/honker/HonkerJvmTest.java` |
| Kotlin | JVM wrapper | `packages/honker-kotlin/src/test/kotlin/dev/honker/kotlin/HonkerKotlinTest.kt` |

Contract:

- Omitted backend, `"polling"`, and `"poll"` select polling/default
  behavior. Backend names are exact; case and whitespace are not
  normalized.
- Unknown backend names are errors everywhere.
- Explicit experimental backend requests must never silently fall back
  to polling.
- A binding may claim `kernel` / `shm` support only when it routes wake
  waits through a shared watcher and has backend-isolation tests that
  cannot pass via fallback polling.

## What CI Proves

- PR CI runs Rust core/extension on Linux, macOS, and Windows.
- PR CI runs Python on Linux, macOS, and Windows.
- PR CI runs Node on Linux, macOS, and Windows.
- PR CI runs .NET on Linux, macOS, and Windows.
- The aggregate Linux binding smoke runs Rust wrapper, Go, .NET
  Python interop, C++, Bun, Ruby, Elixir, and Ruby <-> Python interop.
- The packaged-install proof workflow builds and installs Python,
  Node, Ruby, and .NET packages into clean throwaway consumers.
- JVM and Kotlin bindings have local Maven proof plus a clean Maven
  consumer proof that runs from outside the repo using the packaged
  native resource.
- JVM wake proof mirrors the Python race/resource shape: long-fallback
  listener/worker/stream/result waits, cross-process worker/listener/stream
  wake, concurrent claim uniqueness, delayed deadlines, and listener
  churn/resource cleanup.
- JVM watcher proof includes stable `AUTO`/`PRAGMA_DATA_VERSION`,
  explicit experimental `MMAP_SHM` and `KERNEL_EVENTS`, backend selection
  tests, and cross-process proofs. fcntl is intentionally not a backend
  because the repo proof shows it misses idle cross-connection updates
  without an intervening read transaction.
- JVM/Kotlin ergonomics proof includes typed JSON wrappers,
  `CompletableFuture` waits, Kotlin notification/job/event flows, and
  codec-based typed helpers from a clean Maven consumer.

## What Is Not Proven Yet

- Published registry installs after release. The proof workflow uses
  locally-built artifacts, which catches packaging shape but not registry
  permissions or CDN weirdness.
- Every possible cross-language pair. CI proves representative pairs
  and shared table behavior. It does not run N x N interop.
- Long soak on every OS. The scary nightly workflow soaks Linux; PR CI
  stays shorter.
- Ruby and Elixir do not yet have the same async listener API as
  Python/Node/.NET/Rust/Go/Bun/C++.
- JVM/Kotlin packages do not yet have published Maven Central proof.
