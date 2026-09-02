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
    long? ExpiresSeconds = null,
    int MaxAttempts = 3
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

    [System.Text.Json.Serialization.JsonPropertyName("max_attempts")]
    public long MaxAttempts { get; init; }
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
    public int? MaxAttempts { get; private set; }
    public bool HasMaxAttempts { get; private set; }

    public ScheduleUpdate WithCron(string? cron) { Cron = cron; HasCron = true; return this; }
    public ScheduleUpdate WithPayload(object? payload) { Payload = payload; HasPayload = true; return this; }
    public ScheduleUpdate WithPriority(long? priority) { Priority = priority; HasPriority = true; return this; }
    public ScheduleUpdate WithExpiresSeconds(long? value) { ExpiresSeconds = value; HasExpires = true; return this; }
    /// <summary>Set the attempt budget for future fired jobs. Passing null resets to default 3.</summary>
    public ScheduleUpdate WithMaxAttempts(int? value) { MaxAttempts = value; HasMaxAttempts = true; return this; }
}

/// <summary>
/// A job row exactly as the core returns it. Decodes the output of both
/// honker_get_job and honker_claim_batch, which emit the same twelve
/// keys.
///
/// Every member is required, so a core that omits one fails the decode
/// with a JsonException naming the field. That matters most for a
/// binding running against an older extension: honker_claim_batch before
/// 0.6 returned six columns and no `state`, and a defaulted <c>""</c>
/// would have travelled all the way to <see cref="Job.State"/> as a
/// silently wrong value. The nullable members still have to be present
/// in the JSON — the core emits them as null, never omits them.
/// </summary>
public sealed record JobRow
{
    [System.Text.Json.Serialization.JsonPropertyName("id")]
    public required long Id { get; init; }

    [System.Text.Json.Serialization.JsonPropertyName("queue")]
    public required string Queue { get; init; }

    /// <summary>The payload as stored: JSON text, not yet decoded.</summary>
    [System.Text.Json.Serialization.JsonPropertyName("payload")]
    public required string Payload { get; init; }

    /// <summary>"pending" or "processing".</summary>
    [System.Text.Json.Serialization.JsonPropertyName("state")]
    public required string State { get; init; }

    [System.Text.Json.Serialization.JsonPropertyName("priority")]
    public required long Priority { get; init; }

    [System.Text.Json.Serialization.JsonPropertyName("run_at")]
    public required long RunAt { get; init; }

    /// <summary>Null while the job is pending.</summary>
    [System.Text.Json.Serialization.JsonPropertyName("worker_id")]
    public required string? WorkerId { get; init; }

    /// <summary>Null while the job is pending.</summary>
    [System.Text.Json.Serialization.JsonPropertyName("claim_expires_at")]
    public required long? ClaimExpiresAt { get; init; }

    [System.Text.Json.Serialization.JsonPropertyName("attempts")]
    public required long Attempts { get; init; }

    [System.Text.Json.Serialization.JsonPropertyName("max_attempts")]
    public required long MaxAttempts { get; init; }

    [System.Text.Json.Serialization.JsonPropertyName("created_at")]
    public required long CreatedAt { get; init; }

    /// <summary>Null when the job never expires.</summary>
    [System.Text.Json.Serialization.JsonPropertyName("expires_at")]
    public required long? ExpiresAt { get; init; }
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

/// <summary>
/// A claimed unit of work. Carries the same job detail the core
/// returns — see <see cref="JobRow"/> for the read-only shape — plus
/// the claim methods Ack, Retry, Fail, and Heartbeat.
/// </summary>
public sealed class Job
{
    private readonly Queue _queue;
    private JsonDocument? _document;

    internal Job(Queue queue, JobRow row)
    {
        _queue = queue;
        Id = row.Id;
        QueueName = row.Queue;
        PayloadRaw = row.Payload;
        State = row.State;
        Priority = row.Priority;
        RunAt = row.RunAt;
        // A claimed row always carries the claim columns. If the core
        // ever hands one back without them, that is a bug worth
        // crashing on, not a silent default.
        WorkerId = row.WorkerId
            ?? throw new InvalidOperationException($"claimed job {row.Id} has no worker_id");
        ClaimExpiresAt = row.ClaimExpiresAt
            ?? throw new InvalidOperationException($"claimed job {row.Id} has no claim_expires_at");
        Attempts = row.Attempts;
        MaxAttempts = row.MaxAttempts;
        CreatedAt = row.CreatedAt;
        ExpiresAt = row.ExpiresAt;
    }

    public long Id { get; }
    public string QueueName { get; }
    public string PayloadRaw { get; }
    /// <summary>Always "processing" for a claimed job.</summary>
    public string State { get; }
    public long Priority { get; }
    public long RunAt { get; }
    public string WorkerId { get; }
    public long ClaimExpiresAt { get; }
    public long Attempts { get; }
    public long MaxAttempts { get; }
    public long CreatedAt { get; }
    public long? ExpiresAt { get; }
    public JsonElement Payload
    {
        get
        {
            _document ??= JsonDocument.Parse(PayloadRaw);
            return _document.RootElement;
        }
    }

    /// <summary>
    /// Decode the payload as T. Compile-time typing only: honker never
    /// checks payload shape in the database, so every writer to this
    /// queue has to agree on the JSON.
    /// </summary>
    public T? GetPayload<T>()
    {
        return JsonSerializer.Deserialize<T>(PayloadRaw);
    }

    public bool Ack() => _queue.Ack(Id, WorkerId);
    public bool Retry(int delaySeconds = 60, string error = "") => _queue.Retry(Id, WorkerId, delaySeconds, error);
    public bool Fail(string error = "") => _queue.Fail(Id, WorkerId, error);
    public bool Heartbeat(int? extendSeconds = null) => _queue.Heartbeat(Id, WorkerId, extendSeconds);
}

/// <summary>
/// A claimed unit of work with a decoded payload of type
/// <typeparamref name="TPayload"/>. Same fields and claim methods as
/// <see cref="Job"/>, plus <see cref="Payload"/> decoded as
/// <typeparamref name="TPayload"/>.
///
/// The type parameter is a compile-time contract only. Honker stores
/// payloads as opaque JSON and never validates their shape, so every
/// producer writing to this queue must agree on it.
/// </summary>
public sealed class Job<TPayload>
{
    private readonly Job _job;
    private TPayload? _payload;
    private bool _decoded;

    internal Job(Job job)
    {
        _job = job;
    }

    /// <summary>The untyped job, for APIs that take one.</summary>
    public Job Untyped => _job;

    public long Id => _job.Id;
    public string QueueName => _job.QueueName;

    /// <summary>
    /// The payload decoded as <typeparamref name="TPayload"/>, decoded
    /// on first read and cached. Throws
    /// <see cref="JsonException"/> when the stored JSON does not match
    /// <typeparamref name="TPayload"/> — honker never checks payload
    /// shape, so a producer that disagrees puts a row here anyway.
    ///
    /// Decoding deliberately does NOT happen during the claim. By the
    /// time the binding sees the payload the row is already held in the
    /// database, so throwing inside ClaimBatch/ClaimOne/ClaimAsync would
    /// strand it: invisible until the visibility timeout, with no handle
    /// to Ack, Retry, or Fail it, poisoning every later claim. Catch the
    /// exception here and <see cref="Fail"/> the job, or read
    /// <see cref="PayloadRaw"/> instead.
    /// </summary>
    public TPayload? Payload
    {
        get
        {
            if (!_decoded)
            {
                _payload = _job.GetPayload<TPayload>();
                _decoded = true;
            }

            return _payload;
        }
    }

    public string PayloadRaw => _job.PayloadRaw;
    /// <summary>Always "processing" for a claimed job.</summary>
    public string State => _job.State;
    public long Priority => _job.Priority;
    public long RunAt => _job.RunAt;
    public string WorkerId => _job.WorkerId;
    public long ClaimExpiresAt => _job.ClaimExpiresAt;
    public long Attempts => _job.Attempts;
    public long MaxAttempts => _job.MaxAttempts;
    public long CreatedAt => _job.CreatedAt;
    public long? ExpiresAt => _job.ExpiresAt;

    public bool Ack() => _job.Ack();
    public bool Retry(int delaySeconds = 60, string error = "") => _job.Retry(delaySeconds, error);
    public bool Fail(string error = "") => _job.Fail(error);
    public bool Heartbeat(int? extendSeconds = null) => _job.Heartbeat(extendSeconds);
}

/// <summary>
/// A read-only job snapshot with a decoded payload of type
/// <typeparamref name="TPayload"/>. Data only — no ack, retry, fail,
/// or heartbeat, because reading a row does not claim it.
///
/// The type parameter is a compile-time contract only; honker never
/// validates payload shape.
///
/// Unlike <see cref="Job{TPayload}"/>, the payload here is decoded
/// eagerly, so <see cref="TypedQueue{TPayload}.GetJob"/> throws
/// <see cref="JsonException"/> on a payload that does not match
/// <typeparamref name="TPayload"/>. A read holds nothing, so nothing is
/// stranded by that throw — use <see cref="TypedQueue{TPayload}.Untyped"/>'s
/// <see cref="Queue.GetJob"/> to read the row regardless of its shape.
/// </summary>
public sealed record JobSnapshot<TPayload>
{
    public required long Id { get; init; }
    public required string QueueName { get; init; }
    /// <summary>
    /// The payload decoded as <typeparamref name="TPayload"/>. Null only
    /// when the stored JSON is literally <c>null</c>.
    /// </summary>
    public required TPayload? Payload { get; init; }
    /// <summary>The payload exactly as stored, before decoding.</summary>
    public required string PayloadRaw { get; init; }
    /// <summary>"pending" or "processing".</summary>
    public required string State { get; init; }
    public required long Priority { get; init; }
    public required long RunAt { get; init; }
    /// <summary>Null while the job is pending.</summary>
    public required string? WorkerId { get; init; }
    /// <summary>Null while the job is pending.</summary>
    public required long? ClaimExpiresAt { get; init; }
    public required long Attempts { get; init; }
    public required long MaxAttempts { get; init; }
    public required long CreatedAt { get; init; }
    public required long? ExpiresAt { get; init; }

    internal static JobSnapshot<TPayload> FromRow(JobRow row) => new()
    {
        Id = row.Id,
        QueueName = row.Queue,
        Payload = JsonSerializer.Deserialize<TPayload>(row.Payload),
        PayloadRaw = row.Payload,
        State = row.State,
        Priority = row.Priority,
        RunAt = row.RunAt,
        WorkerId = row.WorkerId,
        ClaimExpiresAt = row.ClaimExpiresAt,
        Attempts = row.Attempts,
        MaxAttempts = row.MaxAttempts,
        CreatedAt = row.CreatedAt,
        ExpiresAt = row.ExpiresAt,
    };
}
