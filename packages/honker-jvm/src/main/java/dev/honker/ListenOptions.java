package dev.honker;

import java.time.Duration;
import java.util.concurrent.Executor;

public final class ListenOptions {
    private final Duration pollTimeout;
    private final Executor executor;

    private ListenOptions(Builder builder) {
        this.pollTimeout = builder.pollTimeout;
        this.executor = builder.executor;
    }

    public static ListenOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public Duration pollTimeout() {
        return pollTimeout;
    }

    public Executor executor() {
        return executor;
    }

    public static final class Builder {
        private Duration pollTimeout = Duration.ofSeconds(15);
        private Executor executor;

        public Builder pollTimeout(Duration pollTimeout) {
            if (pollTimeout == null || pollTimeout.isZero() || pollTimeout.isNegative()) {
                throw new HonkerInvalidOptionException("pollTimeout must be positive");
            }
            this.pollTimeout = pollTimeout;
            return this;
        }

        public Builder executor(Executor executor) {
            this.executor = executor;
            return this;
        }

        public ListenOptions build() {
            return new ListenOptions(this);
        }
    }
}
