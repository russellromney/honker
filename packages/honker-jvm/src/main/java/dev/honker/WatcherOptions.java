package dev.honker;

import java.time.Duration;
import java.util.Objects;

public final class WatcherOptions {
    private final WatcherBackend backend;
    private final Duration pollInterval;
    private final Duration identityCheckInterval;
    private final int subscriberBufferSize;

    private WatcherOptions(Builder builder) {
        this.backend = builder.backend;
        this.pollInterval = builder.pollInterval;
        this.identityCheckInterval = builder.identityCheckInterval;
        this.subscriberBufferSize = builder.subscriberBufferSize;
    }

    public static WatcherOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public WatcherBackend backend() {
        return backend;
    }

    public Duration pollInterval() {
        return pollInterval;
    }

    public Duration identityCheckInterval() {
        return identityCheckInterval;
    }

    public int subscriberBufferSize() {
        return subscriberBufferSize;
    }

    public static final class Builder {
        private WatcherBackend backend = WatcherBackend.PRAGMA_DATA_VERSION;
        private Duration pollInterval = Duration.ofMillis(1);
        private Duration identityCheckInterval = Duration.ofMillis(100);
        private int subscriberBufferSize = 1024;

        public Builder backend(WatcherBackend backend) {
            this.backend = Objects.requireNonNull(backend, "backend");
            return this;
        }

        public Builder pollInterval(Duration pollInterval) {
            this.pollInterval = positive(pollInterval, "pollInterval");
            return this;
        }

        public Builder identityCheckInterval(Duration identityCheckInterval) {
            this.identityCheckInterval = positive(identityCheckInterval, "identityCheckInterval");
            return this;
        }

        public Builder subscriberBufferSize(int subscriberBufferSize) {
            if (subscriberBufferSize <= 0) {
                throw new HonkerInvalidOptionException("subscriberBufferSize must be positive");
            }
            this.subscriberBufferSize = subscriberBufferSize;
            return this;
        }

        public WatcherOptions build() {
            return new WatcherOptions(this);
        }

        private static Duration positive(Duration value, String name) {
            Objects.requireNonNull(value, name);
            if (value.isZero() || value.isNegative()) {
                throw new HonkerInvalidOptionException(name + " must be positive");
            }
            return value;
        }
    }
}
