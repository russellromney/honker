package dev.honker;

import java.time.Duration;

public final class ScheduleOptions {
    private final int priority;
    private final Duration expires;
    private final int maxAttempts;

    private ScheduleOptions(Builder builder) {
        this.priority = builder.priority;
        this.expires = builder.expires;
        this.maxAttempts = builder.maxAttempts;
    }

    public static ScheduleOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public int priority() {
        return priority;
    }

    public Duration expires() {
        return expires;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public static final class Builder {
        private int priority;
        private Duration expires;
        private int maxAttempts = 3;

        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        public Builder expires(Duration expires) {
            this.expires = expires == null ? null : Durations.positiveSeconds(expires, "schedule expires");
            return this;
        }

        public Builder maxAttempts(int maxAttempts) {
            if (maxAttempts <= 0) {
                throw new HonkerInvalidOptionException("schedule maxAttempts must be positive");
            }
            this.maxAttempts = maxAttempts;
            return this;
        }

        public ScheduleOptions build() {
            return new ScheduleOptions(this);
        }
    }
}
