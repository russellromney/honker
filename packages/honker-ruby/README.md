# honker (Ruby)

Ruby binding for [Honker](https://github.com/russellromney/honker): durable queues, streams, pub/sub, and time-trigger scheduling on SQLite.

Full docs:

- [Main repo](https://github.com/russellromney/honker)
- [Docs](https://honker.dev)

## Install

Add it to your `Gemfile`:

```ruby
# Gemfile
gem "honker"
```

then `bundle install`. Or install it directly:

```sh
gem install honker
```

Honker is a thin Ruby wrapper around a SQLite loadable extension
written in Rust. How that extension reaches your gem install depends on
your platform, with a fallback to compile the extension for other
platforms.

Most platforms (x86_64 and arm64 for Linux, arm64 for macOS) get a
platform gem with the extension already built and bundled inside it:

| Platform            | Triple          |
| ------------------- | --------------- |
| x86_64 Linux        | `x86_64-linux`  |
| arm64 Linux         | `aarch64-linux` |
| Apple Silicon macOS | `arm64-darwin`  |

```sh
gem install honker
# Fetching honker-0.2.0-arm64-darwin.gem
```

`Honker::Database.new("app.db")` then finds the bundled extension
automatically.

### Where the bundled extension lives

It is shipped inside the gem under `lib/honker/`, with a filename that
follows the host OS's native shared-library convention:

| Host OS | Extension file                   | Why this name                                |
| ------- | -------------------------------- | -------------------------------------------- |
| Linux   | `lib/honker/libhonker_ext.so`    | `lib` prefix, `.so` (shared object)          |
| macOS   | `lib/honker/libhonker_ext.dylib` | `lib` prefix, `.dylib` (dynamic library)     |
| Windows | `lib/honker/honker_ext.dll`      | no `lib` prefix, `.dll` (dynamic-link lib)   |

`Honker::Database.new` checks the host OS at load time and resolves to
the matching filename, so you usually do not need to think about it.
You only see the difference if you build the extension yourself or set
`HONKER_EXTENSION_PATH`, in which case the file you point at must match
the OS the Ruby process is running on.

To find the installed copy:

```sh
gem which honker            # => …/gems/honker-0.2.0-arm64-darwin/lib/honker.rb
ls "$(dirname "$(gem which honker)")/honker"
# libhonker_ext.dylib  ...
```

### Overriding the extension path

To point at a different copy of the extension (a local dev build, a
sidecar mounted into a container, a system path) pass `extension_path:`
or set `HONKER_EXTENSION_PATH`; both override the bundled file.

```ruby
Honker::Database.new("app.db", extension_path: "./libhonker_ext.dylib")
```

```sh
HONKER_EXTENSION_PATH=/opt/honker/libhonker_ext.so ruby app.rb
```

### Source gem (compiles on install, needs Rust)

Every other platform (Intel macOS, Windows, anything else) gets the
generic gem, which ships the Rust crate source and compiles the
extension during `gem install`. Install [`rustc` and `cargo`](https://rustup.rs)
first; otherwise the install fails with a clear message pointing at
rustup.

```sh
gem install honker
# Fetching honker-0.2.0.gem
# Building native extensions. This could take a while...
# honker: building the SQLite extension with cargo
```

The compiled artifact lands in the same `lib/honker/` directory as the
prebuilt gems, so `Honker::Database.new` finds it the same way.

### Installing from git

`honker` lives in a subdirectory of a polyglot monorepo, so a `github:`
dependency needs Bundler's `glob:` option to point at the gemspec. A
git checkout has no prebuilt extension, so it always takes the
compile-on-install path and needs `rustc`:

```ruby
# Gemfile
gem "honker",
    github: "russellromney/honker",
    glob:   "packages/honker-ruby/honker.gemspec"
```

To pin to a branch, tag, or commit, add `branch:`, `tag:`, or `ref:`:

```ruby
# Gemfile
gem "honker",
    github: "russellromney/honker",
    glob:   "packages/honker-ruby/honker.gemspec",
    branch: "main"
```

## Using Honker alongside an ORM

`honker-ruby` opens its own `SQLite3::Database` handle (via the
[`sqlite3`](https://rubygems.org/gems/sqlite3) gem) and loads the
extension into it. To share a single SQLite file (and a single
transaction) with Rails/ActiveRecord, Sequel, ROM, or Hanami::DB, get
the underlying `sqlite3` gem connection your ORM is already using and
load the Honker extension into that handle, so `honker_enqueue(...)`
and friends are callable from inside one of its transactions.

See the per-ORM walkthroughs at
[honker.dev/guides/orm/ruby](https://honker.dev/guides/orm/ruby/).

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
