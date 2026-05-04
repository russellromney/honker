package dev.honker;

import java.time.Duration;

public final class QueueOptions {
    private final Duration visibilityTimeout;
    private final int maxAttempts;

    private QueueOptions(Builder builder) {
        this.visibilityTimeout = builder.visibilityTimeout;
        this.maxAttempts = builder.maxAttempts;
    }

    public static QueueOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public Duration visibilityTimeout() {
        return visibilityTimeout;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public static final class Builder {
        private Duration visibilityTimeout = Duration.ofSeconds(300);
        private int maxAttempts = 3;

        public Builder visibilityTimeout(Duration visibilityTimeout) {
            this.visibilityTimeout = Durations.positiveSeconds(visibilityTimeout, "visibilityTimeout");
            return this;
        }

        public Builder maxAttempts(int maxAttempts) {
            if (maxAttempts <= 0) {
                throw new HonkerInvalidOptionException("maxAttempts must be positive");
            }
            this.maxAttempts = maxAttempts;
            return this;
        }

        public QueueOptions build() {
            return new QueueOptions(this);
        }

    }
}
