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
| JVM `dev.honker:honker` | CI + local clean consumer | yes | yes | yes | yes | yes | shared JVM watcher |
| Kotlin `dev.honker:honker-kotlin` | local + ORM CI | wrapper | Flow wrapper | wrapper | wrapper | wrapper | JVM wrapper |

### Function Task Helpers

| Binding | Named handler registry | Worker dispatcher | Convenience syntax |
| --- | ---: | ---: | --- |
| Python | yes | CLI and in-process | `@task`, `@periodic_task` |
| JVM | yes | `runTasks` | explicit `TaskRegistry` / `TaskHandle` |
| Kotlin | yes | `runTasks` | Kotlin helpers over the JVM registry |
| Node, Rust, Go, Ruby, Bun, Elixir, .NET, C++ | no | no | queue and result primitives only |

## Extension Reach

Every binding can `open()` a database. A binding must also be able to
say where the loadable extension is, so callers can load Honker onto a
connection they already own. That is what ORM users need: enqueueing
outside the application's transaction loses atomicity.

| Binding | Ships the extension | Path accessor |
| --- | --- | --- |
| Python `honker` | wheel | `extension_info()`, `load_extension(conn)` |
| Node `@russellthehippo/honker-node` | `honker-ext-*` npm packages | `extensionPath()`, `extensionInfo()` |
| Bun `@russellthehippo/honker-bun` | `honker-ext-*` npm packages | `extensionPath()`, `extensionInfo()` |
| Ruby `honker` | platform gems | `Honker.extension_path`, `Honker.load_extension` |
| .NET `Honker` | NuGet native assets | `HonkerExtension.Locate()` |
| JVM `dev.honker:honker` | jar resources | `HonkerExtension.path()` |
| Kotlin `dev.honker:honker-kotlin` | jar resources | inherits the JVM class |
| Go | no — download it | `honker.ExtensionPath()` |
| Elixir `honker` | no — download it | `Honker.Extension.path/0` |
| C++ | links the static lib | n/a |
| Rust `honker` | crate dependency | n/a |

Contract, in every language:

- An explicit path argument wins where the binding has one, then
  `HONKER_EXTENSION_PATH`, then the bundled copy, then an error naming
  every path searched.
- A set-but-missing `HONKER_EXTENSION_PATH` is an error, never a
  fall-through to the bundled copy. A wrong override is a
  configuration mistake and silently loading something else hides it.
- The bundled-copy step differs by ecosystem and is the one part that
  is not identical: Node and Bun resolve the platform package then
  walk up to the first `node_modules`; Python checks `_lib` then walks
  up; Ruby checks the gem's `lib/honker`; .NET and the JVM check their
  packaged native assets; Go checks the executable's directory then
  the working directory; Elixir checks the working directory then
  `priv/`.
- The entry point is always `sqlite3_honkerext_init`.
- The accessor must not require loading the binding's native code. A
  caller asking for a path string already has their own SQLite in the
  process and must not get a second one.

`packages/honker/python/honker/__init__.py` is the reference
implementation.

The file name is load-bearing. When no entry point is given, SQLite
derives one from the file name, and the exact derivation varies between
SQLite versions — `libhonker_ext-v2.dylib` resolves to
`sqlite3_honkerextv2_init`, which does not exist. Ship the library as
`libhonker_ext.{so,dylib}` / `honker_ext.dll`, or pass
`sqlite3_honkerext_init` explicitly. Per-target naming belongs on the
archive, never on the library.

Go, Elixir, and C++ cannot bundle a binary idiomatically. They take it
from a GitHub release, published by `.github/workflows/release-extension.yml`.

## Argument Types

The `honker_*` SQL functions take their integer arguments the way
SQLite's C API does: an `INTEGER` is used as-is, and a `REAL` holding a
whole number is converted. `sqlite3_value_int64` has always behaved
this way, so a C implementation of these functions would too.

This is a deliberate guarantee, not an accident of the Rust wrapper.
`rusqlite`'s `Context::get` type-checks strictly and rejects `REAL`,
which is stricter than SQLite itself; `honker_ops::arg_i64` restores
the documented behavior.

It matters because dynamically typed clients bind what their language
gives them. `better-sqlite3` binds **every** JavaScript number as
`REAL` — whole ones included — because a JS `Number` is an IEEE-754
double, and it offers `BigInt` as the explicit integer signal. So
`honker_enqueue(..., priority, max_attempts, ...)` from Drizzle,
Kysely, or plain better-sqlite3 arrives entirely as `REAL`.

A `REAL` that is not a whole number is an error, and so are infinities,
NaN, and values outside the range of a 64-bit integer. SQLite would
truncate `2.7` to `2`; we do not. Rounding a job id or a retry count
hides a caller's bug, and this is the one place being stricter than
SQLite earns the inconsistency. The error names the value and why.


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

| Binding | Backend option | Watcher interval option | Experimental status |
| --- | --- | --- | --- |
| Python | `honker.open(..., watcher_backend=...)` | `watcher_poll_interval_ms=` | source builds with matching Cargo features |
| Node | `open(path, { watcherBackend })` | `watcherPollIntervalMs` | source builds with matching Cargo features |
| Rust wrapper | `OpenOptions::watcher_backend(...)` | `OpenOptions::watcher_poll_interval(...)` | matching Cargo features |
| Go | `OpenOptions{WatcherBackend: ...}` | `WatcherPollInterval` | depends on loaded extension features |
| Bun | `open(..., { watcherBackend })` | `watcherPollIntervalMs` | depends on loaded extension features |
| C++ | `Database(path, ext_path, watcher_backend)` | fourth constructor arg, milliseconds | depends on loaded extension features |
| .NET | `OpenOptions.WatcherBackend` | `OpenOptions.UpdatePollInterval` | depends on bundled/loaded extension features |
| Ruby | `watcher_backend:` | `watcher_poll_interval_ms:` | depends on loaded extension features |
| Elixir | `watcher_backend:` | `watcher_poll_interval_ms:` | depends on loaded extension features |
| JVM | `OpenOptions.watcherBackend(...)` | `WatcherOptions.pollInterval(...)` | stable `AUTO`/`PRAGMA_DATA_VERSION`; explicit experimental `MMAP_SHM` and `KERNEL_EVENTS` |
| Kotlin | JVM `OpenOptions` | JVM `WatcherOptions` | wraps JVM behavior |

## What CI Proves

- Rust core and extension on Linux, macOS, and Windows
- Python, Node, and .NET on Linux, macOS, and Windows
- Linux binding smoke for Rust wrapper, Go, .NET Python interop, C++,
  Bun, Ruby, Elixir, and Ruby/Python interop
- Packaged-install proof for Python, Node, Ruby, and .NET in clean
  throwaway consumers
- JVM package install/tests and the documented JVM ORM recipes in PR CI
- Kotlin Exposed ORM recipe in PR CI; Kotlin wrapper package tests and clean
  consumer proof remain local
- Representative cross-language wake and table-behavior proofs
- JVM watcher parity for stable `AUTO`/`PRAGMA_DATA_VERSION` and explicit
  experimental `MMAP_SHM` / `KERNEL_EVENTS`

## Not Proven Yet

- Every possible cross-language pair; CI covers representative pairs
- Cross-binding named-consumer checkpoints involving Node. Its current stream
  wrapper reverses topic and consumer at the offset SQL boundary, so its own
  resume path works but another binding will not see the same checkpoint.
- Long soak on every OS; scary nightly soaks Linux
- Ruby and Elixir async listen parity with Python/Node/.NET/Rust/Go/Bun/C++
- Published Maven Central proof for JVM/Kotlin
