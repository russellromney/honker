# honker (Elixir)

Elixir binding for [Honker](https://github.com/russellromney/honker): durable queues, streams, pub/sub, and time-trigger scheduling on SQLite.

Full docs:

- [Main repo](https://github.com/russellromney/honker)
- [Docs](https://honker.dev)

## Install

```elixir
def deps do
  [
    {:honker, "~> 0.1"}
  ]
end
```

You also need the Honker SQLite extension from the main repo.

## Watcher backends

`Honker.open(path, extension_path: ext, watcher_backend: "polling")`
accepts the default polling backend aliases (`"polling"` / `"poll"`).
Experimental `"kernel"` / `"shm"` requests route through `honker-core`
via SQL watcher handles registered by the loaded Honker extension and
return `{:error, reason}` if that extension was not built with the
matching feature.

## Quick start

```elixir
{:ok, db} = Honker.open("app.db", extension_path: "./libhonker_ext.dylib")

{:ok, _id} = Honker.Queue.enqueue(db, "emails", %{to: "alice@example.com"})

case Honker.Queue.claim_one(db, "emails", "worker-1") do
  {:ok, nil} -> :empty
  {:ok, job} ->
    send_email(job.payload)
    Honker.Job.ack(db, job)
end
```

Live pub/sub subscriptions are channel-filtered and skip notifications that
already existed when they attached:

```elixir
{:ok, subscription} = Honker.listen(db, "orders")
ref = subscription.ref

receive do
  {:honker_notification, ^ref, notification} ->
    IO.inspect(notification.payload)
end

:ok = Honker.unlisten(subscription)
```

The listener also stops automatically if the subscribing process exits or the
database is closed.

Delayed jobs use `run_at:`:

```elixir
{:ok, _id} = Honker.Queue.enqueue(db, "emails", %{to: "later@example.com"}, run_at: System.os_time(:second) + 10)
```

Recurring schedules use `schedule:`:

```elixir
:ok = Honker.Scheduler.add(db, name: "fast", queue: "emails", schedule: "@every 1s", payload: %{kind: "tick"})
```

Supported schedule forms:

- `0 3 * * *`
- `*/2 * * * * *`
- `@every 1s`

`schedule:` is the canonical recurring name. `cron:` is compatibility-only.

## Job details

A claimed `%Honker.Job{}` carries the whole row as it stood at claim time:

```elixir
{:ok, job} = Honker.Queue.claim_one(db, "emails", "worker-1")

job.id                # row id
job.queue             # queue this job came from
job.payload           # decoded JSON value
job.state             # "processing"
job.priority          # higher runs first within the queue
job.run_at            # unix seconds; when it became claimable
job.worker_id         # "worker-1"
job.claim_expires_at  # unix seconds; heartbeat before this
job.attempts          # already incremented by this claim
job.max_attempts      # dead-letters once attempts reaches this
job.created_at        # unix seconds
job.expires_at        # unix seconds, or nil when enqueued without :expires
```

`Honker.Queue.get_job/2` returns `{:ok, %Honker.JobSnapshot{}}` — the same
twelve fields, read-only, for a job you did not claim. It is data alone: no
ack/retry/fail/heartbeat, because the reader holds no claim. `state` is
`"pending"` or `"processing"`, and `worker_id` / `claim_expires_at` are `nil`
until some worker claims the job. The snapshot's `payload` is the raw JSON
*text* from the row, not a decoded value — call `Jason.decode!/1` on it.
`{:ok, nil}` means the job was ack'd, dead-lettered, cancelled, or never
existed.

`get_job/2` looks a job up by id and takes no queue name, so an id from
another queue still resolves today; the snapshot's `queue` names the real
one. Do not build on that — queue-scoped lookup is planned and would
narrow it. Check `snapshot.queue` yourself if it matters.

**Breaking change.** `get_job/2` used to return `{:ok, map}` — the raw ABI
row keyed by strings. It now returns a struct, so `row["state"]` becomes
`row.state`. A struct has no `Access` behaviour, so the old bracket form
raises `UndefinedFunctionError` instead of quietly returning nil; the
compiler will not catch it for you, so grep your callers.

Honker never inspects a payload. The shape is a contract between the app that
enqueues and the app that claims, and both sides have to agree on it —
including across languages, since another binding may write to the same queue.
Version the payload if it will change:

```elixir
{:ok, _id} = Honker.Queue.enqueue(db, "emails", %{"v" => 2, "to" => "alice@example.com"})

case job.payload do
  %{"v" => 2, "to" => to} -> send_email(to)
  other -> raise "unknown payload version: #{inspect(other)}"
end
```
