package dev.honker;

import java.time.Duration;

public final class TaskOptions {
    private final int retries;
    private final Duration retryDelay;
    private final int priority;
    private final Duration expires;
    private final boolean storeResult;
    private final Duration resultTtl;

    private TaskOptions(Builder builder) {
        this.retries = builder.retries;
        this.retryDelay = builder.retryDelay;
        this.priority = builder.priority;
        this.expires = builder.expires;
        this.storeResult = builder.storeResult;
        this.resultTtl = builder.resultTtl;
    }

    public static TaskOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public int retries() {
        return retries;
    }

    public Duration retryDelay() {
        return retryDelay;
    }

    public int priority() {
        return priority;
    }

    public Duration expires() {
        return expires;
    }

    public boolean storeResult() {
        return storeResult;
    }

    public Duration resultTtl() {
        return resultTtl;
    }

    public static final class Builder {
        private int retries = 3;
        private Duration retryDelay = Duration.ofSeconds(60);
        private int priority;
        private Duration expires;
        private boolean storeResult = true;
        private Duration resultTtl = Duration.ofHours(1);

        public Builder retries(int retries) {
            if (retries <= 0) {
                throw new HonkerInvalidOptionException("retries must be positive");
            }
            this.retries = retries;
            return this;
        }

        public Builder retryDelay(Duration retryDelay) {
            this.retryDelay = Durations.nonNegativeSeconds(retryDelay, "retryDelay");
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

        public Builder storeResult(boolean storeResult) {
            this.storeResult = storeResult;
            return this;
        }

        public Builder resultTtl(Duration resultTtl) {
            this.resultTtl = Durations.positiveSeconds(resultTtl, "resultTtl");
            return this;
        }

        public TaskOptions build() {
            return new TaskOptions(this);
        }
    }
}
