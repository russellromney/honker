# honker-dotnet

.NET / C# binding for [Honker](https://github.com/russellromney/honker):
durable queues, streams, pub/sub, and time-trigger scheduling on SQLite.

Full docs:

- [Main repo](https://github.com/russellromney/honker)
- [Docs](https://honker.dev)

## Install

```bash
dotnet add package Honker
```

The NuGet package bundles the Honker SQLite extension for:

- `linux-x64`
- `linux-arm64`
- `osx-x64`
- `osx-arm64`
- `win-x64`

## Quick start

```csharp
using Honker;

using var db = Database.Open("app.db");
var queue = db.Queue("emails");

queue.Enqueue("""{"to":"alice@example.com"}""");

var job = queue.ClaimOne("worker-1");
if (job is not null)
{
    SendEmail(job.PayloadRaw);
    job.Ack();
}
```

## Typed queues and job details

`db.Queue<TPayload>(name)` gives the queue a payload contract. Claimed
jobs and `GetJob()` snapshots keep it:

```csharp
record EmailPayload(string To, string Template);

var emails = db.Queue<EmailPayload>("emails");
var id = emails.Enqueue(new EmailPayload("alice@example.com", "welcome"));

// Read-only snapshot: data, no claim methods.
JobSnapshot<EmailPayload>? pending = emails.GetJob(id);
Console.WriteLine($"{pending!.State} {pending.Priority} {pending.RunAt}");

Job<EmailPayload>? job = emails.ClaimOne("worker-1");
if (job is not null)
{
    Send(job.Payload!.To);
    Console.WriteLine($"{job.Attempts}/{job.MaxAttempts} until {job.ClaimExpiresAt}");
    job.Ack();
}
```

The handle `db.Queue<TPayload>(name)` returns is called
`TypedQueue<TPayload>`, not `Queue<TPayload>`. A `Honker.Queue<T>` would
shadow `System.Collections.Generic.Queue<T>` in any file that uses both.
Write `var`, or `TypedQueue<EmailPayload>` where you need the name.

The type parameter is a compile-time contract only. Honker stores
payloads as opaque JSON and never validates their shape — in this
binding or in the database — so every process writing to a queue has to
agree on the payload type. When a producer disagrees, the row still
lands in the queue and the decode is what fails:

```csharp
foreach (var job in emails.ClaimBatch("worker-1", 10))
{
    EmailPayload payload;
    try
    {
        // Claiming never decodes, so the claim itself cannot throw and
        // you always hold a handle. Payload decodes on first read.
        payload = job.Payload!;
    }
    catch (JsonException e)
    {
        job.Fail(e.Message);   // dead-letter it; job.PayloadRaw has the text
        continue;
    }

    Send(payload.To);
    job.Ack();
}
```

`GetJob()` is the one exception: it decodes eagerly and throws on a
payload that does not match, because a read holds no claim and so
strands nothing. `emails.Untyped.GetJob(id)` reads the row whatever
shape it is.

Claimed jobs (`Job`, `Job<TPayload>`) and read-only snapshots
(`JobRow`, `JobSnapshot<TPayload>`) carry every field the core returns:
`Id`, queue name, payload, `State`, `Priority`, `RunAt`, `WorkerId`,
`ClaimExpiresAt`, `Attempts`, `MaxAttempts`, `CreatedAt`, `ExpiresAt`.
A claimed job additionally has `Ack`, `Retry`, `Fail`, and `Heartbeat`;
a snapshot is data only, because reading a row does not claim it.
`Untyped` on `TypedQueue<TPayload>` and `Job<TPayload>` gets you back
to the untyped handle when an API needs one — results (`SaveResult`,
`GetResult`, `WaitResult`) live there, since a result's type has
nothing to do with `TPayload`.

`Payload` means something different on each of these types, so check
which one you hold. On the typed pair (`Job<T>`, `JobSnapshot<T>`) it is
the decoded `T`, with `PayloadRaw` beside it for the JSON text. On `Job`
it is a `JsonElement`, and `Job.GetPayload<T>()` decodes. On `JobRow` it
is the JSON text itself, with no `PayloadRaw` beside it.

`GetJob` is not scoped to the queue you called it on: job ids are
globally unique and the lookup spans every queue, so pass only ids you
know this queue owns.

## Native loading

`Database.Open(...)` loads the Honker extension and runs
`honker_bootstrap()`. Native discovery checks, in order:

1. `OpenOptions.ExtensionPath`
2. `HONKER_EXTENSION_PATH`
3. the bundled NuGet runtime asset under `runtimes/<rid>/native/`

## Watcher backends

`OpenOptions.WatcherBackend = "polling"` (or `"poll"`) selects the
default stable backend. Experimental `"kernel"` / `"shm"` requests route
through `honker-core` via the loaded extension and fail loudly if that
extension was not built with the matching feature.

## Local test

Build the extension first:

```bash
cargo build -p honker-extension
```

Then run the .NET tests:

```bash
dotnet test packages/honker-dotnet/tests/Honker.Tests/Honker.Tests.csproj
```

## Release

The `Release · NuGet` workflow builds native assets for each supported
RID, packs the `.nupkg`, verifies the package contains every runtime
asset, runs a clean consumer smoke test, and publishes on `dotnet-v*`
tags.
