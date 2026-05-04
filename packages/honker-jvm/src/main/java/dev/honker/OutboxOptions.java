package dev.honker;

import java.time.Duration;

public final class OutboxOptions {
    private final int maxAttempts;
    private final Duration baseBackoff;
    private final Duration visibilityTimeout;

    private OutboxOptions(Builder builder) {
        this.maxAttempts = builder.maxAttempts;
        this.baseBackoff = builder.baseBackoff;
        this.visibilityTimeout = builder.visibilityTimeout;
    }

    public static OutboxOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public Duration baseBackoff() {
        return baseBackoff;
    }

    public Duration visibilityTimeout() {
        return visibilityTimeout;
    }

    public static final class Builder {
        private int maxAttempts = 5;
        private Duration baseBackoff = Duration.ofSeconds(5);
        private Duration visibilityTimeout = Duration.ofSeconds(60);

        public Builder maxAttempts(int maxAttempts) {
            if (maxAttempts <= 0) {
                throw new HonkerInvalidOptionException("maxAttempts must be positive");
            }
            this.maxAttempts = maxAttempts;
            return this;
        }

        public Builder baseBackoff(Duration baseBackoff) {
            this.baseBackoff = Durations.positiveSeconds(baseBackoff, "baseBackoff");
            return this;
        }

        public Builder visibilityTimeout(Duration visibilityTimeout) {
            this.visibilityTimeout = Durations.positiveSeconds(visibilityTimeout, "visibilityTimeout");
            return this;
        }

        public OutboxOptions build() {
            return new OutboxOptions(this);
        }
    }
}
