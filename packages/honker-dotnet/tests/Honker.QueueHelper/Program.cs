using Honker;

var mode = RequiredEnv("HONKER_DOTNET_QUEUE_HELPER");
var path = RequiredEnv("HONKER_DOTNET_QUEUE_DB");
var ext = RequiredEnv("HONKER_DOTNET_QUEUE_EXT");

if (mode == "worker")
{
    await RunWorker(path, ext);
    return;
}

if (mode == "writer")
{
    await RunWriter(path, ext);
    return;
}

throw new InvalidOperationException($"unknown queue helper mode '{mode}'");

static async Task RunWorker(string path, string ext)
{
    var backend = Environment.GetEnvironmentVariable("HONKER_DOTNET_QUEUE_BACKEND");
    if (backend == "")
    {
        backend = null;
    }

    var workerId = RequiredEnv("HONKER_DOTNET_QUEUE_WORKER");
    var readyPath = RequiredEnv("HONKER_DOTNET_QUEUE_READY");
    var resultPath = RequiredEnv("HONKER_DOTNET_QUEUE_RESULT");
    using var db = Database.Open(path, new OpenOptions
    {
        ExtensionPath = ext,
        WatcherBackend = backend,
    });
    var queue = db.Queue("shared");
    await File.WriteAllTextAsync(readyPath, "ready");
    var processed = new List<int>();
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

    try
    {
        await foreach (var job in queue.ClaimAsync(workerId, Timeout.InfiniteTimeSpan, cts.Token))
        {
            var stop = job.Payload.TryGetProperty("stop", out var stopValue) && stopValue.GetBoolean();
            if (!job.Ack())
            {
                throw new InvalidOperationException($"failed to ack job {job.Id}");
            }
            if (stop)
            {
                break;
            }

            processed.Add(job.Payload.GetProperty("i").GetInt32());
            cts.CancelAfter(TimeSpan.FromSeconds(2));
        }
    }
    catch (OperationCanceledException) when (cts.IsCancellationRequested)
    {
        // Idle timeout bounds the helper lifetime. Parent tests still assert
        // that the complete expected job set was processed.
    }

    await File.WriteAllTextAsync(resultPath, string.Join(Environment.NewLine, processed));
}

static async Task RunWriter(string path, string ext)
{
    var first = int.Parse(RequiredEnv("HONKER_DOTNET_QUEUE_FIRST"));
    var count = int.Parse(RequiredEnv("HONKER_DOTNET_QUEUE_COUNT"));
    var readyPath = Environment.GetEnvironmentVariable("HONKER_DOTNET_QUEUE_READY");
    var goPath = Environment.GetEnvironmentVariable("HONKER_DOTNET_QUEUE_GO");
    using var db = Database.Open(path, new OpenOptions { ExtensionPath = ext });
    var queue = db.Queue("shared");
    if (!string.IsNullOrEmpty(readyPath) && !string.IsNullOrEmpty(goPath))
    {
        await File.WriteAllTextAsync(readyPath, "ready");
        WaitForFile(goPath);
    }
    for (var i = first; i < first + count; i += 1)
    {
        queue.Enqueue(new { i });
    }
}

static string RequiredEnv(string key)
{
    return Environment.GetEnvironmentVariable(key)
        ?? throw new InvalidOperationException($"missing required env var {key}");
}

static void WaitForFile(string path)
{
    var watch = System.Diagnostics.Stopwatch.StartNew();
    while (watch.Elapsed < TimeSpan.FromSeconds(15))
    {
        if (File.Exists(path))
        {
            return;
        }
        Thread.Sleep(25);
    }
    throw new TimeoutException($"file did not appear: {path}");
}
