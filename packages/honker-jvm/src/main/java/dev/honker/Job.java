package dev.honker;

import java.time.Duration;
import java.util.Map;

/**
 * One job this worker holds a claim on.
 *
 * <p>Carries every field the core returns for the row (see
 * {@link #snapshot()}) plus the claim operations: ack, retry, fail and
 * heartbeat.
 */
public final class Job {
    private final Queue queueRef;
    private final JobSnapshot snapshot;

    private Job(Queue queueRef, JobSnapshot snapshot) {
        this.queueRef = queueRef;
        this.snapshot = snapshot;
    }

    static Job from(Queue queue, Map<String, Object> row) {
        return new Job(queue, JobSnapshot.from(row));
    }

    /** The read-only view of this job's row. */
    public JobSnapshot snapshot() {
        return snapshot;
    }

    public long id() {
        return snapshot.id();
    }

    public String queue() {
        return snapshot.queue();
    }

    public String payloadJson() {
        return snapshot.payloadJson();
    }

    /**
     * Decode the payload with {@code codec}. The type is a compile-time
     * contract only; Honker never validates payload shape.
     */
    public <T> T payload(JsonCodec<T> codec) {
        return snapshot.payload(codec);
    }

    /** Always {@code "processing"} for a job this worker just claimed. */
    public String state() {
        return snapshot.state();
    }

    public int priority() {
        return snapshot.priority();
    }

    /** When the job became claimable, unix epoch seconds. */
    public long runAt() {
        return snapshot.runAt();
    }

    public String workerId() {
        return snapshot.workerId();
    }

    /** When this claim lapses, unix epoch seconds. Never null on a claimed job. */
    public long claimExpiresAt() {
        return snapshot.claimExpiresAt();
    }

    public int attempts() {
        return snapshot.attempts();
    }

    public int maxAttempts() {
        return snapshot.maxAttempts();
    }

    /** When the job was enqueued, unix epoch seconds. */
    public long createdAt() {
        return snapshot.createdAt();
    }

    /**
     * When the job stops being claimable, unix epoch seconds, or {@code null}
     * when it was enqueued without {@code expires}.
     */
    public Long expiresAt() {
        return snapshot.expiresAt();
    }

    public boolean ack() {
        return queueRef.ack(id(), workerId());
    }

    public boolean retry(Duration delay, String error) {
        return queueRef.retry(id(), workerId(), delay, error);
    }

    public boolean fail(String error) {
        return queueRef.fail(id(), workerId(), error);
    }

    public boolean heartbeat() {
        return queueRef.heartbeat(id(), workerId());
    }

    public boolean heartbeat(Duration extendBy) {
        return queueRef.heartbeat(id(), workerId(), extendBy);
    }
}
