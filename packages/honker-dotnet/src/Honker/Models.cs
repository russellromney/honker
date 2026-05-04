using System.Text.Json;

namespace Honker;

public sealed record QueueOptions(
    int VisibilityTimeoutSeconds = 300,
    int MaxAttempts = 3
);

public sealed record EnqueueOptions(
    long? RunAtUnix = null,
    double? DelaySeconds = null,
    int Priority = 0,
    int? MaxAttempts = null,
    double? ExpiresSeconds = null
);

public sealed record ScheduledTask(
    string Name,
    string Queue,
    object? Payload = null,
    string? Schedule = null,
    string? Cron = null,
    long Priority = 0,
    long? ExpiresSeconds = null
);

public sealed record ScheduledFire(
    string Name,
    string Queue,
    long FireAt,
    long JobId
);

public sealed record ScheduleRow
{
    [System.Text.Json.Serialization.JsonPropertyName("name")]
    public string Name { get; init; } = "";

    [System.Text.Json.Serialization.JsonPropertyName("queue")]
    public string Queue { get; init; } = "";

    [System.Text.Json.Serialization.JsonPropertyName("cron_expr")]
    public string CronExpr { get; init; } = "";

    /// <summary>JSON-serialized payload string.</summary>
    [System.Text.Json.Serialization.JsonPropertyName("payload")]
    public string Payload { get; init; } = "";

    [System.Text.Json.Serialization.JsonPropertyName("priority")]
    public long Priority { get; init; }

    [System.Text.Json.Serialization.JsonPropertyName("expires_s")]
    public long? ExpiresSeconds { get; init; }

    [System.Text.Json.Serialization.JsonPropertyName("next_fire_at")]
    public long NextFireAt { get; init; }

    [System.Text.Json.Serialization.JsonPropertyName("enabled")]
    public bool Enabled { get; init; }
}

/// <summary>
/// Options for <see cref="Scheduler.Update"/>. Set the Has* flags
/// (typically via the With* helpers) to indicate which fields you
/// want to mutate; omitted fields are left alone. Same shape as
/// Python's _UNSET sentinel and Node's hasOwnProperty detection.
/// </summary>
public sealed class ScheduleUpdate
{
    public string? Cron { get; private set; }
    public bool HasCron { get; private set; }
    public object? Payload { get; private set; }
    public bool HasPayload { get; private set; }
    public long? Priority { get; private set; }
    public bool HasPriority { get; private set; }
    public long? ExpiresSeconds { get; private set; }
    public bool HasExpires { get; private set; }

    public ScheduleUpdate WithCron(string? cron) { Cron = cron; HasCron = true; return this; }
    public ScheduleUpdate WithPayload(object? payload) { Payload = payload; HasPayload = true; return this; }
    public ScheduleUpdate WithPriority(long? priority) { Priority = priority; HasPriority = true; return this; }
    public ScheduleUpdate WithExpiresSeconds(long? value) { ExpiresSeconds = value; HasExpires = true; return this; }
}

public sealed record JobRow
{
    [System.Text.Json.Serialization.JsonPropertyName("id")]
    public long Id { get; init; }

    [System.Text.Json.Serialization.JsonPropertyName("queue")]
    public string Queue { get; init; } = "";

    [System.Text.Json.Serialization.JsonPropertyName("payload")]
    public string Payload { get; init; } = "";

    [System.Text.Json.Serialization.JsonPropertyName("state")]
    public string State { get; init; } = "";

    [System.Text.Json.Serialization.JsonPropertyName("priority")]
    public long Priority { get; init; }

    [System.Text.Json.Serialization.JsonPropertyName("run_at")]
    public long RunAt { get; init; }

    [System.Text.Json.Serialization.JsonPropertyName("worker_id")]
    public string? WorkerId { get; init; }

    [System.Text.Json.Serialization.JsonPropertyName("claim_expires_at")]
    public long? ClaimExpiresAt { get; init; }

    [System.Text.Json.Serialization.JsonPropertyName("attempts")]
    public long Attempts { get; init; }

    [System.Text.Json.Serialization.JsonPropertyName("max_attempts")]
    public long MaxAttempts { get; init; }

    [System.Text.Json.Serialization.JsonPropertyName("created_at")]
    public long CreatedAt { get; init; }

    [System.Text.Json.Serialization.JsonPropertyName("expires_at")]
    public long? ExpiresAt { get; init; }
}

public sealed record OutboxOptions(
    int VisibilityTimeoutSeconds = 300,
    int MaxAttempts = 3,
    int BaseBackoffSeconds = 30
);

public static class Schedules
{
    public static string EverySeconds(int seconds)
    {
        if (seconds <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(seconds), "seconds must be positive");
        }

        return $"@every {seconds}s";
    }
}

public sealed class Job
{
    private readonly Queue _queue;
    private JsonDocument? _document;

    internal Job(
        Queue queue,
        long id,
        string queueName,
        string payloadRaw,
        string workerId,
        long attempts,
        long claimExpiresAt,
        string state = "processing")
    {
        _queue = queue;
        Id = id;
        QueueName = queueName;
        PayloadRaw = payloadRaw;
        WorkerId = workerId;
        Attempts = attempts;
        ClaimExpiresAt = claimExpiresAt;
        State = state;
    }

    public long Id { get; }
    public string QueueName { get; }
    public string PayloadRaw { get; }
    public string WorkerId { get; }
    public long Attempts { get; }
    public long ClaimExpiresAt { get; }
    public string State { get; }
    public JsonElement Payload
    {
        get
        {
            _document ??= JsonDocument.Parse(PayloadRaw);
            return _document.RootElement;
        }
    }

    public T? GetPayload<T>()
    {
        return JsonSerializer.Deserialize<T>(PayloadRaw);
    }

    public bool Ack() => _queue.Ack(Id, WorkerId);
    public bool Retry(int delaySeconds = 60, string error = "") => _queue.Retry(Id, WorkerId, delaySeconds, error);
    public bool Fail(string error = "") => _queue.Fail(Id, WorkerId, error);
    public bool Heartbeat(int? extendSeconds = null) => _queue.Heartbeat(Id, WorkerId, extendSeconds);
}
