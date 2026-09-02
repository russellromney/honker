package dev.honker;

import org.jspecify.annotations.Nullable;

/**
 * A {@link JobSnapshot} whose payload is already decoded to {@code T}.
 *
 * <p>Like {@link JobSnapshot} this is data only. The payload type is a
 * compile-time contract; Honker does not validate payload shape.
 */
public record TypedJobSnapshot<T>(JobSnapshot raw, T payload) {
    static <T> TypedJobSnapshot<T> of(JobSnapshot raw, JsonCodec<T> codec) {
        return new TypedJobSnapshot<>(raw, codec.decode(raw.payloadJson()));
    }

    public long id() {
        return raw.id();
    }

    public String queue() {
        return raw.queue();
    }

    public String payloadJson() {
        return raw.payloadJson();
    }

    public String state() {
        return raw.state();
    }

    public int priority() {
        return raw.priority();
    }

    public long runAt() {
        return raw.runAt();
    }

    public @Nullable String workerId() {
        return raw.workerId();
    }

    public @Nullable Long claimExpiresAt() {
        return raw.claimExpiresAt();
    }

    public int attempts() {
        return raw.attempts();
    }

    public int maxAttempts() {
        return raw.maxAttempts();
    }

    public long createdAt() {
        return raw.createdAt();
    }

    public @Nullable Long expiresAt() {
        return raw.expiresAt();
    }
}
