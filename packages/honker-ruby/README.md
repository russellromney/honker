# honker (Ruby)

Ruby binding for [Honker](https://github.com/russellromney/honker): durable queues, streams, pub/sub, and time-trigger scheduling on SQLite.

Full docs:

- [Main repo](https://github.com/russellromney/honker)
- [Docs](https://honker.dev)

## Install

```ruby
gem "honker"
```

How the SQLite extension reaches the gem depends on your platform:

- **Precompiled** (x86_64 Linux, arm64 Linux, and Apple Silicon macOS):
  the extension is pre-built and included in the gem.
- **Every other platform**: the generic gem builds the extension after
  install with `rustc` (from [rustup.rs](https://rustup.rs)).

### Installing from git

`honker` lives in a subdirectory of a polyglot monorepo, so a `github:`
dependency needs Bundler's `glob:` option to point at the gemspec. The
git checkout has no prebuilt extension either, so it builds on install
and needs `rustc`:

```ruby
gem "honker", github: "russellromney/honker",
    glob: "packages/honker-ruby/honker.gemspec"
```

## Watcher backends

`Honker::Database.new(..., watcher_backend: "polling")` accepts the
default polling backend aliases (`"polling"` / `"poll"`). Experimental
`"kernel"` / `"shm"` requests route through `honker-core` via the loaded
Honker extension and fail loudly if that extension was not built with
the matching feature.

## Quick start

```ruby
require "honker"

db = Honker::Database.new("app.db")
q = db.queue("emails")

q.enqueue({to: "alice@example.com"})

if (job = q.claim_one("worker-1"))
  send_email(job.payload)
  job.ack
end
```

Delayed jobs use `run_at:`:

```ruby
q.enqueue({to: "later@example.com"}, run_at: Time.now.to_i + 10)
```

Recurring schedules use `schedule:`:

```ruby
sched = db.scheduler
sched.add(name: "fast", queue: "emails", schedule: "@every 1s", payload: {kind: "tick"})
```

Supported schedule forms:

- `0 3 * * *`
- `*/2 * * * * *`
- `@every 1s`

`schedule:` is the canonical recurring name. `cron:` still works as a compatibility alias.
