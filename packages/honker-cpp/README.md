# honker-cpp

C++17 binding for [Honker](https://github.com/russellromney/honker): durable queues, streams, pub/sub, and time-trigger scheduling on SQLite.

Full docs:

- [Main repo](https://github.com/russellromney/honker)
- [Docs](https://honker.dev)

## Requirements

- Zig 0.15+
- C++17 compiler
- SQLite development headers / library
- nlohmann-json headers
- Honker SQLite extension

The C++ binding loads the Honker SQLite extension itself. That means SQLite must be built with loadable extension support.

## Watcher Backends

`honker::Database` accepts an optional third backend argument. The
default, `"polling"`, and `"poll"` select the polling backend.
Experimental `"kernel"` / `"shm"` requests route through `honker-core`
via the loaded Honker extension and fail loudly if that extension was
not built with the matching feature.

Platform installs:

```bash
# macOS with Homebrew
brew install sqlite nlohmann-json

# macOS with MacPorts
sudo port install sqlite3 nlohmann-json

# Ubuntu / Debian
sudo apt-get install libsqlite3-dev nlohmann-json3-dev

# Fedora
sudo dnf install sqlite-devel json-devel

# Arch
sudo pacman -S sqlite nlohmann-json
```

Apple's system SQLite headers do not expose the load-extension API. On macOS, pass the package-manager prefix:

```bash
zig build test \
  -Dsqlite-prefix="$(brew --prefix sqlite)" \
  -Djson-prefix="$(brew --prefix nlohmann-json)" \
  -Dhonker-ext=/path/to/libhonker_ext.dylib
```

## Quick start

```cpp
#include "honker.hpp"

int main() {
    honker::Database db{"app.db", "./libhonker_ext.dylib"};
    auto q = db.queue("emails");

    q.enqueue(R"({"to":"alice@example.com"})");

    if (auto job = q.claim_one("worker-1")) {
        send_email(job->payload);
        job->ack();
    }
}
```

## Job details

`honker::Job` (a claimed unit of work) and `honker::JobSnapshot` (the
read-only row from `Queue::get_job`) both carry every field the core
claim/lookup returns:

```cpp
auto job = q.claim_one("worker-1");
job->id(); job->queue(); job->payload(); job->state();
job->priority(); job->run_at(); job->worker_id(); job->claim_expires_at();
job->attempts(); job->max_attempts(); job->created_at(); job->expires_at();
```

`Job` also holds a claim, so it has `ack()`, `retry()`, `fail()`, and
`heartbeat()`. `JobSnapshot` is data only.

On `Job`, `worker_id()` and `claim_expires_at()` are plain values
because holding a claim is what makes it a `Job`. On `JobSnapshot` they
are `std::optional` because a pending row has neither. `expires_at()`
is `std::optional<int64_t>` on both.

`payload()` is the raw JSON text exactly as stored — parse it with your
preferred JSON library. honker never checks the payload's shape; every
process writing a queue must agree on it.

```cpp
if (auto row = q.get_job(id)) {
    row->state();                       // "pending" or "processing"
    nlohmann::json::parse(row->payload());
}
```

`Queue::get_job_json(id)` still returns the undecoded JSON blob for
callers that want the bytes.

A claim or lookup whose JSON is malformed, or that is missing a
required field, throws `honker::Error` rather than returning a
default-filled job.

Delayed jobs use `run_at` options on enqueue. Recurring schedules use schedule expressions:

```cpp
auto sched = db.scheduler();
sched.add("fast", "emails", "@every 1s", R"({"kind":"tick"})");
```

Supported schedule forms:

- `0 3 * * *`
- `*/2 * * * * *`
- `@every 1s`

For full API docs, streams, notify/listen, and SQL details, see the main repo and docs site.
