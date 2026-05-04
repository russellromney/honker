package dev.honker;

import java.time.Duration;
import java.time.Instant;

public final class EnqueueOptions {
    private final Instant runAt;
    private final Duration delay;
    private final int priority;
    private final Duration expires;

    private EnqueueOptions(Builder builder) {
        this.runAt = builder.runAt;
        this.delay = builder.delay;
        this.priority = builder.priority;
        this.expires = builder.expires;
    }

    public static EnqueueOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public Instant runAt() {
        return runAt;
    }

    public Duration delay() {
        return delay;
    }

    public int priority() {
        return priority;
    }

    public Duration expires() {
        return expires;
    }

    public static final class Builder {
        private Instant runAt;
        private Duration delay;
        private int priority;
        private Duration expires;

        public Builder runAt(Instant runAt) {
            this.runAt = runAt;
            return this;
        }

        public Builder delay(Duration delay) {
            this.delay = delay == null ? null : Durations.nonNegativeSeconds(delay, "delay");
            return this;
        }

        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        public Builder expires(Duration expires) {
            this.expires = expires == null ? null : Durations.positiveSeconds(expires, "expires");
            return this;
        }

        public EnqueueOptions build() {
            return new EnqueueOptions(this);
        }
    }
}
