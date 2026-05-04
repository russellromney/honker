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

    public T payload() {
        return codec.decode(raw.payloadJson());
    }

    public long id() {
        return raw.id();
    }

    public String queue() {
        return raw.queue();
    }

    public String workerId() {
        return raw.workerId();
    }

    public int attempts() {
        return raw.attempts();
    }

    public long claimExpiresAt() {
        return raw.claimExpiresAt();
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
