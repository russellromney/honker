package dev.honker;

import java.time.Duration;

public final class TaskWorkerOptions {
    private final int concurrency;
    private final Duration idlePollInterval;

    private TaskWorkerOptions(Builder builder) {
        this.concurrency = builder.concurrency;
        this.idlePollInterval = builder.idlePollInterval;
    }

    public static TaskWorkerOptions defaults() {
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

    public static final class Builder {
        private int concurrency = Runtime.getRuntime().availableProcessors();
        private Duration idlePollInterval = Duration.ofSeconds(5);

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

        public TaskWorkerOptions build() {
            return new TaskWorkerOptions(this);
        }
    }
}
