package dev.honker;

import java.time.Duration;
import java.util.concurrent.Executor;

public final class SchedulerOptions {
    private final String lockName;
    private final Duration lockTtl;
    private final Duration heartbeatEvery;
    private final Duration idlePollInterval;
    private final Executor executor;

    private SchedulerOptions(Builder builder) {
        this.lockName = builder.lockName;
        this.lockTtl = builder.lockTtl;
        this.heartbeatEvery = builder.heartbeatEvery;
        this.idlePollInterval = builder.idlePollInterval;
        this.executor = builder.executor;
    }

    public static SchedulerOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public String lockName() {
        return lockName;
    }

    public Duration lockTtl() {
        return lockTtl;
    }

    public Duration heartbeatEvery() {
        return heartbeatEvery;
    }

    public Duration idlePollInterval() {
        return idlePollInterval;
    }

    public Executor executor() {
        return executor;
    }

    public static final class Builder {
        private String lockName = "honker-scheduler";
        private Duration lockTtl = Duration.ofSeconds(60);
        private Duration heartbeatEvery = Duration.ofSeconds(30);
        private Duration idlePollInterval = Duration.ofSeconds(15);
        private Executor executor;

        public Builder lockName(String lockName) {
            this.lockName = lockName;
            return this;
        }

        public Builder lockTtl(Duration lockTtl) {
            this.lockTtl = Durations.positiveSeconds(lockTtl, "lockTtl");
            return this;
        }

        public Builder heartbeatEvery(Duration heartbeatEvery) {
            if (heartbeatEvery == null || heartbeatEvery.isZero() || heartbeatEvery.isNegative()) {
                throw new HonkerInvalidOptionException("heartbeatEvery must be positive");
            }
            this.heartbeatEvery = heartbeatEvery;
            return this;
        }

        public Builder idlePollInterval(Duration idlePollInterval) {
            if (idlePollInterval == null || idlePollInterval.isZero() || idlePollInterval.isNegative()) {
                throw new HonkerInvalidOptionException("idlePollInterval must be positive");
            }
            this.idlePollInterval = idlePollInterval;
            return this;
        }

        public Builder executor(Executor executor) {
            this.executor = executor;
            return this;
        }

        public SchedulerOptions build() {
            return new SchedulerOptions(this);
        }
    }
}
