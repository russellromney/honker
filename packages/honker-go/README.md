# honker-go

Go binding for [Honker](https://github.com/russellromney/honker): durable queues, streams, pub/sub, and time-trigger scheduling on SQLite.

Full docs:

- [Main repo](https://github.com/russellromney/honker)
- [Docs](https://honker.dev)

## Install

```bash
go get github.com/russellromney/honker-go
```

You also need the Honker SQLite extension from the main repo.

## Watcher backends

`OpenWithOptions` accepts `OpenOptions{WatcherBackend: "polling"}` (or
`"poll"`), which is also the default. `"kernel"` / `"shm"` route
through `honker-core` via the loaded Honker extension and fail loudly
if that extension was not built with the matching feature.

## Quick start

```go
db, err := honker.Open("app.db", "./libhonker_ext.dylib")
if err != nil {
    panic(err)
}
defer db.Close()

q := db.Queue("emails", honker.QueueOptions{})

if _, err := q.Enqueue(map[string]any{"to": "alice@example.com"}, honker.EnqueueOptions{}); err != nil {
    panic(err)
}

job, err := q.ClaimOne("worker-1")
if err != nil {
    panic(err)
}
if job != nil {
    sendEmail(job.Payload)
    _, _ = job.Ack()
}
```

### Job details and typed payloads

A claimed `Job` and a `GetJob` snapshot (`JobRow`) both carry the full job
shape: `ID`, `Queue`, `Payload`, `State`, `Priority`, `RunAt`, `WorkerID`,
`ClaimExpiresAt`, `Attempts`, `MaxAttempts`, `CreatedAt`, and `ExpiresAt`.
A `Job` also has the claim methods (`Ack`, `Retry`, `Fail`, `Heartbeat`);
a `JobRow` is data only.

`ExpiresAt` is `*int64` on both types and is `nil` — never `0` — when the job
was enqueued without a TTL. On a `JobRow`, `WorkerID` and `ClaimExpiresAt` are
`nil` until a worker claims the job. On a claimed `Job` both are always set,
so they are plain values.

Every field on a `Job` is a snapshot taken at claim time. `Heartbeat` extends
the claim in the database but does not refresh `job.ClaimExpiresAt`; re-read
with `GetJob` if you need the new deadline.

Payloads stay raw JSON. `DecodePayload[T]` unmarshals one into your own type:

```go
type Email struct {
    To       string `json:"to"`
    Template string `json:"template"`
}

job, err := q.ClaimOne("worker-1")
if err != nil {
    return err
}
if job != nil {
    email, err := honker.DecodePayload[Email](job.Payload)
    if err != nil {
        _, _ = job.Fail(err.Error())
    } else {
        send(email)
        _, _ = job.Ack()
    }
    fmt.Println(job.State, job.Priority, job.RunAt, job.CreatedAt)
}
```

A `JobRow` decodes through the same helper via `PayloadBytes`. `GetJob`
returns `(nil, nil)` when the job is gone, so check the row before using it:

```go
row, err := q.GetJob(id)
if err != nil {
    return err
}
if row == nil {
    return fmt.Errorf("job %d is gone", id)
}
email, err := honker.DecodePayload[Email](row.PayloadBytes())
```

`GetJob` is not queue-scoped: job ids are globally unique and the lookup does
not filter on the queue, so it can return a row from a different queue whose
payload is not an `Email` at all. Pass only ids you know this queue owns.

The type parameter is a compile-time contract only. Honker never validates
payload shape in the database, so every app writing to a queue has to agree on
the JSON shape itself. A mismatch shows up as an unmarshal error at decode
time, not at enqueue time. An empty payload returns `honker.ErrEmptyPayload`,
which you can match with `errors.Is`, rather than a zero-valued `Email`.

Delayed jobs use `Delay` (seconds from now) or `RunAt` (absolute unix epoch):

```go
delay := int64(10)
_, _ = q.Enqueue(map[string]any{"to": "later@example.com"}, honker.EnqueueOptions{Delay: &delay})

at := time.Now().Unix() + 10
_, _ = q.Enqueue(map[string]any{"to": "later@example.com"}, honker.EnqueueOptions{RunAt: &at})
```

Recurring schedules use `Schedule`:

```go
s := db.Scheduler()
_ = s.Add(honker.ScheduledTask{
    Name:     "fast",
    Queue:    "emails",
    Schedule: "@every 1s",
    Payload:  map[string]any{"kind": "tick"},
})
```

Supported schedule forms:

- `0 3 * * *`
- `*/2 * * * * *`
- `@every 1s`

`Schedule` is the canonical recurring-schedule name. Older `Cron` naming is compatibility-only.
