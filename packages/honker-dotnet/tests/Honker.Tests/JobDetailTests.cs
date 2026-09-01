using System.Runtime.InteropServices;
using System.Text.Json;

namespace Honker.Tests;

/// <summary>
/// Every field the core returns for a job — claimed or read back —
/// has to arrive with the right value, not merely be present.
/// </summary>
public sealed class JobDetailTests
{
    private sealed record OrderPayload(string Sku, int Quantity);

    [Fact]
    public void ClaimedJobCarriesEveryCoreDetail()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("work", new QueueOptions(VisibilityTimeoutSeconds: 120, MaxAttempts: 3));

        var runAt = Now(db) - 5;
        var enqueuedBefore = Now(db);
        var id = queue.Enqueue(
            new { k = 7 },
            new EnqueueOptions(RunAtUnix: runAt, Priority: 9, MaxAttempts: 5, ExpiresSeconds: 600));
        var enqueuedAfter = Now(db);

        var claimedBefore = Now(db);
        var job = queue.ClaimOne("worker-1");
        var claimedAfter = Now(db);

        Assert.NotNull(job);
        Assert.Equal(id, job!.Id);
        Assert.Equal("work", job.QueueName);
        Assert.Equal(7, job.Payload.GetProperty("k").GetInt32());
        Assert.Equal("processing", job.State);
        Assert.Equal(9, job.Priority);
        Assert.Equal(runAt, job.RunAt);
        Assert.Equal("worker-1", job.WorkerId);
        Assert.InRange(job.ClaimExpiresAt, claimedBefore + 120, claimedAfter + 120);
        Assert.Equal(1, job.Attempts);
        Assert.Equal(5, job.MaxAttempts);
        Assert.InRange(job.CreatedAt, enqueuedBefore, enqueuedAfter);
        Assert.NotNull(job.ExpiresAt);
        Assert.InRange(job.ExpiresAt!.Value, enqueuedBefore + 600, enqueuedAfter + 600);
    }

    [Fact]
    public void PendingSnapshotCarriesEveryCoreDetail()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("work");

        var runAt = Now(db) + 3600;
        var enqueuedBefore = Now(db);
        var id = queue.Enqueue(
            new { k = 3 },
            new EnqueueOptions(RunAtUnix: runAt, Priority: 4, MaxAttempts: 2, ExpiresSeconds: 900));
        var enqueuedAfter = Now(db);

        var row = queue.GetJob(id);

        Assert.NotNull(row);
        Assert.Equal(id, row!.Id);
        Assert.Equal("work", row.Queue);
        Assert.Equal("{\"k\":3}", row.Payload);
        Assert.Equal("pending", row.State);
        Assert.Equal(4, row.Priority);
        Assert.Equal(runAt, row.RunAt);
        Assert.Null(row.WorkerId);
        Assert.Null(row.ClaimExpiresAt);
        Assert.Equal(0, row.Attempts);
        Assert.Equal(2, row.MaxAttempts);
        Assert.InRange(row.CreatedAt, enqueuedBefore, enqueuedAfter);
        Assert.NotNull(row.ExpiresAt);
        Assert.InRange(row.ExpiresAt!.Value, enqueuedBefore + 900, enqueuedAfter + 900);
    }

    [Fact]
    public void ReaderSeesProcessingDetailWhileWorkerHoldsTheClaimAndNothingAfterAck()
    {
        using var harness = TestHarness.Create();
        using var worker = harness.Open();
        using var reader = harness.Open();

        var workerQueue = worker.Queue("work", new QueueOptions(VisibilityTimeoutSeconds: 45));
        var readerQueue = reader.Queue("work", new QueueOptions(VisibilityTimeoutSeconds: 45));

        var id = workerQueue.Enqueue(new { k = 1 });
        var claimedBefore = Now(worker);
        var job = workerQueue.ClaimOne("worker-7");
        var claimedAfter = Now(worker);
        Assert.NotNull(job);

        var processing = readerQueue.GetJob(id);
        Assert.NotNull(processing);
        Assert.Equal("processing", processing!.State);
        Assert.Equal("worker-7", processing.WorkerId);
        Assert.Equal(1, processing.Attempts);
        Assert.NotNull(processing.ClaimExpiresAt);
        Assert.InRange(processing.ClaimExpiresAt!.Value, claimedBefore + 45, claimedAfter + 45);

        Assert.True(job!.Ack());
        Assert.Null(readerQueue.GetJob(id));
    }

    [Fact]
    public void DelayedJobReportsItsRunAt()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("work");

        var before = Now(db);
        var id = queue.Enqueue(new { k = 2 }, new EnqueueOptions(DelaySeconds: 60));
        var after = Now(db);

        var row = queue.GetJob(id);
        Assert.NotNull(row);
        Assert.Equal("pending", row!.State);
        Assert.InRange(row.RunAt, before + 60, after + 60);
        Assert.Null(queue.ClaimOne("early-worker"));
    }

    [Fact]
    public void TypedQueueDecodesClaimedPayloadsAndKeepsEveryDetail()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue<OrderPayload>("orders", new QueueOptions(VisibilityTimeoutSeconds: 90, MaxAttempts: 4));

        var enqueuedBefore = Now(db);
        var id = queue.Enqueue(new OrderPayload("SKU-42", 3), new EnqueueOptions(Priority: 6, ExpiresSeconds: 300));
        var enqueuedAfter = Now(db);

        var claimedBefore = Now(db);
        var job = queue.ClaimOne("typed-worker");
        var claimedAfter = Now(db);

        Assert.NotNull(job);
        Assert.Equal(id, job!.Id);
        Assert.Equal("orders", job.QueueName);
        Assert.Equal(new OrderPayload("SKU-42", 3), job.Payload);
        Assert.Equal("processing", job.State);
        Assert.Equal(6, job.Priority);
        Assert.InRange(job.RunAt, enqueuedBefore, enqueuedAfter);
        Assert.Equal("typed-worker", job.WorkerId);
        Assert.InRange(job.ClaimExpiresAt, claimedBefore + 90, claimedAfter + 90);
        Assert.Equal(1, job.Attempts);
        Assert.Equal(4, job.MaxAttempts);
        Assert.InRange(job.CreatedAt, enqueuedBefore, enqueuedAfter);
        Assert.NotNull(job.ExpiresAt);
        Assert.InRange(job.ExpiresAt!.Value, enqueuedBefore + 300, enqueuedAfter + 300);
        Assert.True(job.Ack());
    }

    [Fact]
    public void TypedSnapshotDecodesPayloadAndCarriesEveryDetail()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue<OrderPayload>("orders", new QueueOptions(VisibilityTimeoutSeconds: 30));

        var enqueuedBefore = Now(db);
        var id = queue.Enqueue(new OrderPayload("SKU-9", 11), new EnqueueOptions(Priority: 2, MaxAttempts: 7, ExpiresSeconds: 120));
        var enqueuedAfter = Now(db);

        var pending = queue.GetJob(id);
        Assert.NotNull(pending);
        Assert.Equal(id, pending!.Id);
        Assert.Equal("orders", pending.QueueName);
        Assert.Equal(new OrderPayload("SKU-9", 11), pending.Payload);
        Assert.Equal("{\"Sku\":\"SKU-9\",\"Quantity\":11}", pending.PayloadRaw);
        Assert.Equal("pending", pending.State);
        Assert.Equal(2, pending.Priority);
        Assert.InRange(pending.RunAt, enqueuedBefore, enqueuedAfter);
        Assert.Null(pending.WorkerId);
        Assert.Null(pending.ClaimExpiresAt);
        Assert.Equal(0, pending.Attempts);
        Assert.Equal(7, pending.MaxAttempts);
        Assert.InRange(pending.CreatedAt, enqueuedBefore, enqueuedAfter);
        Assert.NotNull(pending.ExpiresAt);
        Assert.InRange(pending.ExpiresAt!.Value, enqueuedBefore + 120, enqueuedAfter + 120);

        var claimedBefore = Now(db);
        var job = queue.ClaimOne("typed-worker");
        var claimedAfter = Now(db);
        Assert.NotNull(job);

        var processing = queue.GetJob(id);
        Assert.NotNull(processing);
        Assert.Equal("processing", processing!.State);
        Assert.Equal("typed-worker", processing.WorkerId);
        Assert.Equal(1, processing.Attempts);
        Assert.NotNull(processing.ClaimExpiresAt);
        Assert.InRange(processing.ClaimExpiresAt!.Value, claimedBefore + 30, claimedAfter + 30);

        Assert.True(job!.Ack());
        Assert.Null(queue.GetJob(id));
    }

    [Fact]
    public void TypedAndUntypedHandlesShareOneQueue()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var typed = db.Queue<OrderPayload>("orders");
        var untyped = db.Queue("orders");

        var id = typed.Enqueue(new OrderPayload("SKU-1", 1));
        var job = untyped.ClaimOne("w1");

        Assert.NotNull(job);
        Assert.Equal(id, job!.Id);
        Assert.Equal(new OrderPayload("SKU-1", 1), job.GetPayload<OrderPayload>());
        Assert.Same(untyped, typed.Untyped);
    }

    private static long Now(Database db)
    {
        return Convert.ToInt64(db.Query("SELECT unixepoch() AS v").Single()["v"]);
    }

    private sealed class TestHarness : IDisposable
    {
        private readonly string _dir;

        private TestHarness(string dir, string extensionPath)
        {
            _dir = dir;
            ExtensionPath = extensionPath;
            DatabasePath = Path.Combine(dir, "test.db");
        }

        public string ExtensionPath { get; }
        public string DatabasePath { get; }

        public static TestHarness Create()
        {
            var root = FindRepoRoot();
            var extensionPath = FindExtension(root);
            var dir = Path.Combine(Path.GetTempPath(), $"honker-dotnet-jobdetail-{Guid.NewGuid():N}");
            Directory.CreateDirectory(dir);
            return new TestHarness(dir, extensionPath);
        }

        public Database Open()
        {
            return Database.Open(DatabasePath, new OpenOptions
            {
                ExtensionPath = ExtensionPath,
                UpdatePollInterval = TimeSpan.FromMilliseconds(5),
            });
        }

        public void Dispose()
        {
            try
            {
                Directory.Delete(_dir, recursive: true);
            }
            catch
            {
                // Best-effort cleanup.
            }
        }

        private static string FindRepoRoot()
        {
            var current = AppContext.BaseDirectory;
            while (!string.IsNullOrEmpty(current))
            {
                if (Directory.Exists(Path.Combine(current, "honker-core")) &&
                    File.Exists(Path.Combine(current, "Cargo.toml")))
                {
                    return current;
                }

                current = Path.GetDirectoryName(current) ?? "";
            }

            throw new DirectoryNotFoundException("could not locate honker repo root from test base directory");
        }

        private static string FindExtension(string root)
        {
            var candidates = new[]
            {
                Path.Combine(root, "target", "release", ExtensionFileName()),
                Path.Combine(root, "target", "debug", ExtensionFileName()),
            };

            var found = candidates.FirstOrDefault(File.Exists);
            if (found is null)
            {
                throw new FileNotFoundException($"expected built honker extension at one of: {string.Join(", ", candidates)}");
            }

            return found;
        }

        private static string ExtensionFileName()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) return "honker_ext.dll";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX)) return "libhonker_ext.dylib";
            return "libhonker_ext.so";
        }
    }
}
