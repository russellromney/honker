using System.Diagnostics;
using Honker;

// Tiny console app the .NET binding tests spawn as a "worker" or
// "writer" subprocess. Replaces an earlier approach of `dotnet test
// --filter ...` which forced the full vstest host to spin up — that
// added ~30s of startup on macOS-14 ARM64 runners and routinely blew
// past the parent's WaitReady budget.
//
// Args: <mode> where mode is "worker" or "writer". Everything else
// (db path, extension path, ready/result file paths, worker id,
// writer counts) comes through env vars so the call sites stay the
// same shape they had before.

var mode = args.Length > 0 ? args[0] : Environment.GetEnvironmentVariable("HONKER_DOTNET_QUEUE_HELPER");
if (string.IsNullOrEmpty(mode))
{
    Console.Error.WriteLine("usage: Honker.QueueHelper <worker|writer>");
    return 2;
}

var path = Environment.GetEnvironmentVariable("HONKER_DOTNET_QUEUE_DB")!;
var ext = Environment.GetEnvironmentVariable("HONKER_DOTNET_QUEUE_EXT")!;

if (mode == "worker")
{
    var backend = Environment.GetEnvironmentVariable("HONKER_DOTNET_QUEUE_BACKEND");
    if (backend == "")
    {
        backend = null;
    }

    var workerId = Environment.GetEnvironmentVariable("HONKER_DOTNET_QUEUE_WORKER")!;
    var readyPath = Environment.GetEnvironmentVariable("HONKER_DOTNET_QUEUE_READY")!;
    var resultPath = Environment.GetEnvironmentVariable("HONKER_DOTNET_QUEUE_RESULT")!;
    var workerStarted = Stopwatch.StartNew();
    var logPath = resultPath + ".log";
    var logLock = new object();
    void log(string msg)
    {
        var line = $"[helper-worker {workerId} +{workerStarted.ElapsedMilliseconds}ms] {msg}";
        Console.Error.WriteLine(line);
        lock (logLock)
        {
            File.AppendAllText(logPath, line + "\n");
        }
    }
    log($"start backend={backend ?? "polling"} db={path}");
    using var db = Database.Open(path, new OpenOptions
    {
        ExtensionPath = ext,
        WatcherBackend = backend,
    });
    log("Database.Open returned");
    var queue = db.Queue("shared");
    await File.WriteAllTextAsync(readyPath, "ready");
    log("ready file written");
    var processed = new List<int>();
    // Safety net only: parent always enqueues an explicit stop job for
    // a deterministic exit. 30s leaves headroom on slow CI runners.
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

    var sawStop = false;
    try
    {
        await foreach (var job in queue.ClaimAsync(workerId, Timeout.InfiniteTimeSpan, cts.Token))
        {
            var stop = job.Payload.TryGetProperty("stop", out var stopValue) && stopValue.GetBoolean();
            if (!job.Ack())
            {
                log($"ack failed for job {job.Id}");
                return 3;
            }
            if (stop)
            {
                sawStop = true;
                log("got stop job, exiting");
                break;
            }

            processed.Add(job.Payload.GetProperty("i").GetInt32());
            cts.CancelAfter(TimeSpan.FromSeconds(2));
        }
    }
    catch (OperationCanceledException) when (cts.IsCancellationRequested)
    {
        log($"cts fired without stop job; processed.Count={processed.Count}");
    }

    if (!sawStop)
    {
        log($"exiting without stop; processed.Count={processed.Count}");
    }
    await File.WriteAllTextAsync(resultPath, string.Join(Environment.NewLine, processed));
    return 0;
}

if (mode == "writer")
{
    var first = int.Parse(Environment.GetEnvironmentVariable("HONKER_DOTNET_QUEUE_FIRST")!);
    var count = int.Parse(Environment.GetEnvironmentVariable("HONKER_DOTNET_QUEUE_COUNT")!);
    var readyPath = Environment.GetEnvironmentVariable("HONKER_DOTNET_QUEUE_READY");
    var goPath = Environment.GetEnvironmentVariable("HONKER_DOTNET_QUEUE_GO");
    using var db = Database.Open(path, new OpenOptions { ExtensionPath = ext });
    var queue = db.Queue("shared");
    if (!string.IsNullOrEmpty(readyPath) && !string.IsNullOrEmpty(goPath))
    {
        await File.WriteAllTextAsync(readyPath, "ready");
        var watch = Stopwatch.StartNew();
        while (watch.Elapsed < TimeSpan.FromSeconds(30))
        {
            if (File.Exists(goPath))
            {
                break;
            }
            await Task.Delay(25);
        }
        if (!File.Exists(goPath))
        {
            Console.Error.WriteLine($"writer go file never appeared: {goPath}");
            return 4;
        }
    }
    for (var i = first; i < first + count; i += 1)
    {
        queue.Enqueue(new { i });
    }
    return 0;
}

Console.Error.WriteLine($"unknown helper mode '{mode}'");
return 5;
