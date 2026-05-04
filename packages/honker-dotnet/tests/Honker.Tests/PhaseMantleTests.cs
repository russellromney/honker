using System.Text.Json;

namespace Honker.Tests;

public sealed class PhaseMantleTests
{
    [Fact]
    public void ScheduleListRoundTripsFields()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var sched = db.Scheduler();

        sched.Add(new ScheduledTask("recap", "emails",
            Payload: new { team = "premier-league" }, Cron: "0 9 * * 1", Priority: 3));
        sched.Add(new ScheduledTask("sync", "syncs",
            Payload: null, Cron: "@every 1h"));

        var rows = sched.List();
        Assert.Equal(2, rows.Count);
        var recap = rows.First(r => r.Name == "recap");
        Assert.Equal("emails", recap.Queue);
        Assert.Equal(3, recap.Priority);
        Assert.True(recap.Enabled);
        Assert.True(recap.NextFireAt > 0);
        var payload = JsonDocument.Parse(recap.Payload).RootElement;
        Assert.Equal("premier-league", payload.GetProperty("team").GetString());
    }

    [Fact]
    public void PauseResumeIdempotent()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var sched = db.Scheduler();
        sched.Add(new ScheduledTask("a", "q", Payload: null, Cron: "0 9 * * *"));

        Assert.True(sched.Pause("a"));
        Assert.False(sched.Pause("a")); // already paused
        Assert.False(sched.Pause("missing"));
        Assert.False(sched.List().First(r => r.Name == "a").Enabled);

        Assert.True(sched.Resume("a"));
        Assert.False(sched.Resume("a"));
        Assert.True(sched.List().First(r => r.Name == "a").Enabled);
    }

    [Fact]
    public void UpdateMutatesAndNoopOnEmpty()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var sched = db.Scheduler();
        sched.Add(new ScheduledTask("t", "q",
            Payload: new { v = 1 }, Cron: "0 9 * * *"));

        Assert.True(sched.Update("t", new ScheduleUpdate()
            .WithPayload(new { v = 99 })
            .WithPriority(5)));
        var row = sched.List().First(r => r.Name == "t");
        var payload = JsonDocument.Parse(row.Payload).RootElement;
        Assert.Equal(99, payload.GetProperty("v").GetInt32());
        Assert.Equal(5, row.Priority);

        var before = row.NextFireAt;
        Assert.True(sched.Update("t", new ScheduleUpdate().WithCron("*/5 * * * *")));
        var after = sched.List().First(r => r.Name == "t");
        Assert.Equal("*/5 * * * *", after.CronExpr);
        Assert.NotEqual(before, after.NextFireAt);

        // Empty update is a no-op.
        Assert.False(sched.Update("t", new ScheduleUpdate()));
        // Missing schedule returns false.
        Assert.False(sched.Update("missing", new ScheduleUpdate().WithPayload(new { })));
    }

    [Fact]
    public void QueueCancelAndGetJob()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var q = db.Queue("emails");
        var jobId = q.Enqueue(new { to = "alice@example.com" });

        var row = q.GetJob(jobId);
        Assert.NotNull(row);
        Assert.Equal("emails", row!.Queue);
        Assert.Equal("pending", row.State);
        Assert.Equal(jobId, row.Id);

        Assert.True(q.Cancel(jobId));
        Assert.False(q.Cancel(jobId)); // idempotent
        Assert.Null(q.GetJob(jobId));
        Assert.Null(q.ClaimOne("worker-1"));
    }

    [Fact]
    public void CancelProcessingInvalidatesAck()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var q = db.Queue("emails");
        var jobId = q.Enqueue(new { to = "x" });
        var job = q.ClaimOne("worker-1")!;
        Assert.Equal(jobId, job.Id);

        Assert.True(q.Cancel(jobId));
        Assert.False(job.Ack());
    }
}
