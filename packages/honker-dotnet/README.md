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
