# Honker Binding Support

This is the current binding truth table: packaged status, API coverage,
and wake behavior. SQL table compatibility is shared across all bindings
because the schema is defined in Rust and installed by the SQLite
extension.

## API Parity

| Binding | Package proof | Queue | Streams | Notify/listen | Scheduler | Outbox | Wake path |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| SQLite extension | load smoke | SQL | SQL | notify SQL only | SQL | SQL | host language watches/reads |
| Python `honker` | yes | yes | yes | yes | yes | yes | shared Rust watcher |
| Node `@russellthehippo/honker-node` | yes | yes | yes | yes | yes | yes | shared Rust watcher |
| Ruby `honker` | yes | yes | yes | notify yes, listen no | yes | yes | extension C ABI |
| .NET `Honker` | yes | yes | yes | yes | yes | yes | extension C ABI |
| Rust `honker` | CI | yes | yes | yes | yes | yes | shared Rust watcher |
| Go | CI | yes | yes | yes | yes | yes | extension C ABI |
| Bun `@russellthehippo/honker-bun` | CI | yes | yes | yes | yes | yes | extension C ABI |
| Elixir `honker` | CI | yes | yes | notify yes, listen no | yes | yes | extension SQL handles |
| C++ | CI | yes | yes | yes | yes | yes | extension C ABI |
| JVM `dev.honker:honker` | local + clean consumer | yes | yes | yes | yes | yes | shared JVM watcher |
| Kotlin `dev.honker:honker-kotlin` | local + clean consumer | wrapper | Flow wrapper | wrapper | wrapper | wrapper | JVM wrapper |

## Watcher Backends

The stable backend is `PRAGMA data_version`. It is the default across
maintained bindings and is the only backend assumed by published binary
packages unless a package explicitly says otherwise.

Experimental source-build backends:

- `kernel`: filesystem events over the database/WAL/SHM paths
- `shm`: mmap reads of SQLite's WAL shared-memory index

Backend contract:

- omitted backend, `"polling"`, and `"poll"` select the default stable
  behavior
- backend names are exact; case and whitespace are not normalized
- unknown backend names are errors everywhere
- explicit experimental backend requests must fail loudly when support is
  unavailable
- no binding may silently fall back to polling for an explicit
  experimental request

| Binding | Backend option | Experimental status |
| --- | --- | --- |
| Python | `honker.open(..., watcher_backend=...)` | source builds with matching Cargo features |
| Node | `open(path, maxReaders?, watcherBackend?)` | source builds with matching Cargo features |
| Rust wrapper | `OpenOptions::watcher_backend(...)` | matching Cargo features |
| Go | `OpenWithOptions(..., OpenOptions{WatcherBackend: ...})` | depends on loaded extension features |
| Bun | `open(..., { watcherBackend })` | depends on loaded extension features |
| C++ | `Database(path, ext_path, watcher_backend)` | depends on loaded extension features |
| .NET | `OpenOptions.WatcherBackend` | depends on bundled/loaded extension features |
| Ruby | `watcher_backend:` | depends on loaded extension features |
| Elixir | `watcher_backend:` | depends on loaded extension features |
| JVM | `OpenOptions.watcherBackend(...)` | stable `AUTO`/`PRAGMA_DATA_VERSION`; explicit experimental `MMAP_SHM` and `KERNEL_EVENTS` |
| Kotlin | JVM `OpenOptions` | wraps JVM behavior |

## What CI Proves

- Rust core and extension on Linux, macOS, and Windows
- Python, Node, and .NET on Linux, macOS, and Windows
- Linux binding smoke for Rust wrapper, Go, .NET Python interop, C++,
  Bun, Ruby, Elixir, and Ruby/Python interop
- Packaged-install proof for Python, Node, Ruby, and .NET in clean
  throwaway consumers
- JVM and Kotlin local Maven proof plus clean consumer proof outside the
  repo
- Representative cross-language wake and table-behavior proofs
- JVM watcher parity for stable `AUTO`/`PRAGMA_DATA_VERSION` and explicit
  experimental `MMAP_SHM` / `KERNEL_EVENTS`

## Not Proven Yet

- Every possible cross-language pair; CI covers representative pairs
- Long soak on every OS; scary nightly soaks Linux
- Ruby and Elixir async listen parity with Python/Node/.NET/Rust/Go/Bun/C++
- Published Maven Central proof for JVM/Kotlin
