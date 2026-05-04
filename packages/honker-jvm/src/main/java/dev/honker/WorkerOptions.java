package dev.honker;

import java.time.Duration;
import java.util.concurrent.Executor;

public final class WorkerOptions {
    private final int concurrency;
    private final Duration idlePollInterval;
    private final Duration defaultRetryDelay;
    private final Executor executor;

    private WorkerOptions(Builder builder) {
        this.concurrency = builder.concurrency;
        this.idlePollInterval = builder.idlePollInterval;
        this.defaultRetryDelay = builder.defaultRetryDelay;
        this.executor = builder.executor;
    }

    public static WorkerOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public int concurrency() {
        return concurrency;
    }

    public Duration idlePollInterval() {
        return idlePollInterval;
    }

    public Duration defaultRetryDelay() {
        return defaultRetryDelay;
    }

    public Executor executor() {
        return executor;
    }

    public static final class Builder {
        private int concurrency = 1;
        private Duration idlePollInterval = Duration.ofSeconds(5);
        private Duration defaultRetryDelay = Duration.ofSeconds(60);
        private Executor executor;

        public Builder concurrency(int concurrency) {
            if (concurrency <= 0) {
                throw new HonkerInvalidOptionException("concurrency must be positive");
            }
            this.concurrency = concurrency;
            return this;
        }

        public Builder idlePollInterval(Duration idlePollInterval) {
            if (idlePollInterval == null || idlePollInterval.isZero() || idlePollInterval.isNegative()) {
                throw new HonkerInvalidOptionException("idlePollInterval must be positive");
            }
            this.idlePollInterval = idlePollInterval;
            return this;
        }

        public Builder defaultRetryDelay(Duration defaultRetryDelay) {
            this.defaultRetryDelay = Durations.nonNegativeSeconds(defaultRetryDelay, "defaultRetryDelay");
            return this;
        }

        public Builder executor(Executor executor) {
            this.executor = executor;
            return this;
        }

        public WorkerOptions build() {
            return new WorkerOptions(this);
        }
    }
}
