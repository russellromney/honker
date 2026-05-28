# honker-dotnet

Early .NET / C# binding for Honker.

License:

- MIT or Apache-2.0

Install:

```bash
dotnet add package Honker
```

Current shape:

- thin wrapper over the SQLite loadable extension
- `Database.Open(...)` loads `honker-extension` and runs
  `honker_bootstrap()`
- typed `Queue`, `Stream`, `Outbox`, `Scheduler`, `Job`, and lock wrappers
- async claim / listen / subscribe / outbox worker loops with
  `CancellationToken`
- core-backed update wakes through the loaded Honker extension

Watcher backend option:

- `OpenOptions.WatcherBackend = "polling"` (or `"poll"`) selects the
  default polling backend
- experimental `"kernel"` / `"shm"` requests route through
  `honker-core` via the loaded Honker extension and fail loudly if that
  extension was not built with the matching feature

Current status:

- queue enqueue / claim / ack / retry / fail / heartbeat / results are wired
- stream publish / read / subscribe / offset persistence are wired
- notify / listen, advisory locks, transactional outbox, and rate limits are wired
- scheduler add / remove / tick / soonest / run are wired
- delayed-claim wake and `@every` schedule support are implemented in
  the binding, but tests only exercise them when the underlying
  extension build exposes the corresponding SQL functions

Native loading:

- `Database.Open(...)` first honors `OpenOptions.ExtensionPath`
- then `HONKER_EXTENSION_PATH`
- then it looks for the bundled native extension from the NuGet package
  in the app output root or `runtimes/<rid>/native/`

Bundled native RID coverage:

- `linux-x64`
- `linux-arm64`
- `osx-x64`
- `osx-arm64`
- `win-x64`

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

`release-dotnet.yml` builds native assets on matching runners, combines
them into one NuGet package, smoke-tests a consumer app, and publishes
on `dotnet-v*` tags.

Typical local release flow:

```bash
cargo build --release -p honker-extension
mkdir -p packages/honker-dotnet/src/Honker/package-assets/runtimes/<rid>/native
cp target/release/<native-lib-name> packages/honker-dotnet/src/Honker/package-assets/runtimes/<rid>/native/
dotnet pack packages/honker-dotnet/src/Honker/Honker.csproj -c Release -p:PackageVersion=<version> -o artifacts/honker-dotnet
dotnet nuget push artifacts/honker-dotnet/*.nupkg --source https://api.nuget.org/v3/index.json --api-key <key>
```

That keeps NuGet credentials out of GitHub Actions.
