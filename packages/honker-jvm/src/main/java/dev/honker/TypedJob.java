package dev.honker;

import java.time.Duration;

public final class TypedJob<T> {
    private final Job raw;
    private final JsonCodec<T> codec;

    TypedJob(Job raw, JsonCodec<T> codec) {
        this.raw = raw;
        this.codec = codec;
    }

    public Job raw() {
        return raw;
    }

    /** The read-only view of this job's row. */
    public JobSnapshot snapshot() {
        return raw.snapshot();
    }

    /**
     * The payload decoded with this queue's codec. The type is a
     * compile-time contract only; Honker never validates payload shape.
     */
    public T payload() {
        return codec.decode(raw.payloadJson());
    }

    public String payloadJson() {
        return raw.payloadJson();
    }

    public long id() {
        return raw.id();
    }

    public String queue() {
        return raw.queue();
    }

    /** Always {@code "processing"} for a job this worker just claimed. */
    public String state() {
        return raw.state();
    }

    public int priority() {
        return raw.priority();
    }

    /** When the job became claimable, unix epoch seconds. */
    public long runAt() {
        return raw.runAt();
    }

    public String workerId() {
        return raw.workerId();
    }

    public int attempts() {
        return raw.attempts();
    }

    /** When this claim lapses, unix epoch seconds. */
    public long claimExpiresAt() {
        return raw.claimExpiresAt();
    }

    public int maxAttempts() {
        return raw.maxAttempts();
    }

    /** When the job was enqueued, unix epoch seconds. */
    public long createdAt() {
        return raw.createdAt();
    }

    /**
     * When the job stops being claimable, unix epoch seconds, or
     * {@code null} when it was enqueued without {@code expires}.
     */
    public Long expiresAt() {
        return raw.expiresAt();
    }

    public boolean ack() {
        return raw.ack();
    }

    public boolean retry(Duration delay, String error) {
        return raw.retry(delay, error);
    }

    public boolean fail(String error) {
        return raw.fail(error);
    }

    public boolean heartbeat() {
        return raw.heartbeat();
    }

    public boolean heartbeat(Duration extendBy) {
        return raw.heartbeat(extendBy);
    }
}
