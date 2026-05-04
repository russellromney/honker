package dev.honker;

import java.time.Duration;

public final class NotificationPruneOptions {
    private final Duration olderThan;
    private final Integer maxKeep;

    private NotificationPruneOptions(Builder builder) {
        this.olderThan = builder.olderThan;
        this.maxKeep = builder.maxKeep;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Duration olderThan() {
        return olderThan;
    }

    public Integer maxKeep() {
        return maxKeep;
    }

    public static final class Builder {
        private Duration olderThan;
        private Integer maxKeep;

        public Builder olderThan(Duration olderThan) {
            this.olderThan = olderThan == null ? null : Durations.nonNegativeSeconds(olderThan, "olderThan");
            return this;
        }

        public Builder maxKeep(Integer maxKeep) {
            if (maxKeep != null && maxKeep < 0) {
                throw new HonkerInvalidOptionException("maxKeep must not be negative");
            }
            this.maxKeep = maxKeep;
            return this;
        }

        public NotificationPruneOptions build() {
            return new NotificationPruneOptions(this);
        }
    }
}
