# Binding Support

Honker's core is Rust. The extension owns the SQLite schema and SQL
functions. Bindings are thin language wrappers over that same shape.

This table is meant to be boring and honest. "Yes" means the feature
has a typed binding and runs in root CI. "SQL" means the extension
feature is available through raw SQL, but the language wrapper does
not expose the nice API yet.

| Binding | Package proof | Queue | Streams | Notify/listen | Scheduler | Wake behavior |
|---|---:|---:|---:|---:|---:|---|
| SQLite extension | load smoke | SQL | SQL | notify SQL only | SQL | host language must watch/read |
| Python `honker` | yes | yes | yes | yes | yes | shared Rust `UpdateWatcher` |
| Node `@russellthehippo/honker-node` | yes | yes | yes | yes | yes | shared Rust `UpdateWatcher` |
| .NET `Honker` | yes | yes | yes | yes | yes | .NET `PRAGMA data_version` poller |
| Rust `honker` | CI | yes | yes | yes | yes | shared Rust `UpdateWatcher` |
| Go | CI | yes | yes | yes | yes | Go `PRAGMA data_version` poller |
| Bun `@russellthehippo/honker-bun` | CI | yes | yes | yes | yes | Bun `PRAGMA data_version` poller |
| C++ | CI | yes | yes | yes | yes | C++ `PRAGMA data_version` poller |
| JVM `dev.honker:honker` | local + clean consumer | yes | yes | yes | yes | shared JVM watcher (AUTO/PRAGMA stable, mmap-shm/kernel explicit experimental) |
| Kotlin `dev.honker:honker-kotlin` | local + clean consumer | wrapper | Flow wrapper | wrapper | wrapper | wraps shared JVM watcher |
| Ruby `honker` | yes | yes | yes | notify yes, listen no | yes | no async listener API yet |
| Elixir `honker` | CI | yes | yes | notify yes, listen no | yes | local update snapshots + PRAGMA polling |

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
- JVM wake proof now mirrors the Python race/resource shape: long-fallback
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
