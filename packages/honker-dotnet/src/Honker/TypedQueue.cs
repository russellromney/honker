using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Honker;

/// <summary>
/// A queue whose payloads are typed as <typeparamref name="TPayload"/>.
/// A thin wrapper over <see cref="Queue"/>: enqueue takes a
/// <typeparamref name="TPayload"/>, claims return
/// <see cref="Job{TPayload}"/>, and reads return
/// <see cref="JobSnapshot{TPayload}"/>.
///
/// The type parameter is a compile-time contract only. Honker stores
/// payloads as opaque JSON and never validates their shape, in this
/// binding or in the database. Every process writing to the queue has
/// to agree on the payload type; nothing enforces it at runtime.
/// </summary>
public sealed class TypedQueue<TPayload>
{
    private readonly Queue _queue;

    internal TypedQueue(Queue queue)
    {
        _queue = queue;
    }

    public string Name => _queue.Name;

    /// <summary>The untyped queue, for APIs this wrapper does not cover.</summary>
    public Queue Untyped => _queue;

    public long Enqueue(TPayload payload, EnqueueOptions? options = null, HonkerTransaction? transaction = null)
    {
        return _queue.Enqueue(payload, options, transaction);
    }

    public IReadOnlyList<Job<TPayload>> ClaimBatch(string workerId, int batchSize)
    {
        return _queue.ClaimBatch(workerId, batchSize)
            .Select(job => new Job<TPayload>(job))
            .ToList();
    }

    public Job<TPayload>? ClaimOne(string workerId)
    {
        var job = _queue.ClaimOne(workerId);
        return job is null ? null : new Job<TPayload>(job);
    }

    public async IAsyncEnumerable<Job<TPayload>> ClaimAsync(
        string workerId,
        TimeSpan? idlePoll = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var job in _queue.ClaimAsync(workerId, idlePoll, cancellationToken)
            .WithCancellation(cancellationToken))
        {
            yield return new Job<TPayload>(job);
        }
    }

    /// <summary>
    /// Read a single job row by id. Returns the snapshot or null on
    /// miss (ack'd, dead'd, or never existed). Pure read.
    /// </summary>
    public JobSnapshot<TPayload>? GetJob(long jobId)
    {
        var row = _queue.GetJob(jobId);
        return row is null ? null : JobSnapshot<TPayload>.FromRow(row);
    }

    public int AckBatch(IEnumerable<long> ids, string workerId) => _queue.AckBatch(ids, workerId);

    public bool Cancel(long jobId) => _queue.Cancel(jobId);

    public long NextClaimAt() => _queue.NextClaimAt();
}
