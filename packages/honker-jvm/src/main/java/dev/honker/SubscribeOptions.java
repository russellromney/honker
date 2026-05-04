package dev.honker;

import java.time.Duration;
import java.util.concurrent.Executor;

public final class SubscribeOptions {
    private final String consumer;
    private final Long fromOffset;
    private final int saveEveryN;
    private final Duration saveEvery;
    private final Duration pollTimeout;
    private final Executor executor;

    private SubscribeOptions(Builder builder) {
        this.consumer = builder.consumer;
        this.fromOffset = builder.fromOffset;
        this.saveEveryN = builder.saveEveryN;
        this.saveEvery = builder.saveEvery;
        this.pollTimeout = builder.pollTimeout;
        this.executor = builder.executor;
    }

    public static SubscribeOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public String consumer() {
        return consumer;
    }

    public Long fromOffset() {
        return fromOffset;
    }

    public int saveEveryN() {
        return saveEveryN;
    }

    public Duration saveEvery() {
        return saveEvery;
    }

    public Duration pollTimeout() {
        return pollTimeout;
    }

    public Executor executor() {
        return executor;
    }

    public static final class Builder {
        private String consumer;
        private Long fromOffset;
        private int saveEveryN = 1000;
        private Duration saveEvery = Duration.ofSeconds(1);
        private Duration pollTimeout = Duration.ofSeconds(15);
        private Executor executor;

        public Builder consumer(String consumer) {
            this.consumer = consumer;
            return this;
        }

        public Builder fromOffset(Long fromOffset) {
            if (fromOffset != null && fromOffset < 0) {
                throw new HonkerInvalidOptionException("fromOffset must not be negative");
            }
            this.fromOffset = fromOffset;
            return this;
        }

        public Builder saveEveryN(int saveEveryN) {
            if (saveEveryN < 0) {
                throw new HonkerInvalidOptionException("saveEveryN must not be negative");
            }
            this.saveEveryN = saveEveryN;
            return this;
        }

        public Builder saveEvery(Duration saveEvery) {
            if (saveEvery == null || saveEvery.isNegative()) {
                throw new HonkerInvalidOptionException("saveEvery must not be negative");
            }
            this.saveEvery = saveEvery;
            return this;
        }

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

        public SubscribeOptions build() {
            return new SubscribeOptions(this);
        }
    }
}
