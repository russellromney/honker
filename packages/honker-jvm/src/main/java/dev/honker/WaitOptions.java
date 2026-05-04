package dev.honker;

import java.time.Duration;

public final class WaitOptions {
    private final Duration timeout;
    private final Duration fallbackPollInterval;

    private WaitOptions(Builder builder) {
        this.timeout = builder.timeout;
        this.fallbackPollInterval = builder.fallbackPollInterval;
    }

    public static WaitOptions forever() {
        return builder().build();
    }

    public static WaitOptions timeout(Duration timeout) {
        return builder().timeout(timeout).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public Duration timeout() {
        return timeout;
    }

    public Duration fallbackPollInterval() {
        return fallbackPollInterval;
    }

    public static final class Builder {
        private Duration timeout;
        private Duration fallbackPollInterval = Duration.ofSeconds(15);

        public Builder timeout(Duration timeout) {
            if (timeout != null && timeout.isNegative()) {
                throw new HonkerInvalidOptionException("timeout must not be negative");
            }
            this.timeout = timeout;
            return this;
        }

        public Builder fallbackPollInterval(Duration fallbackPollInterval) {
            if (fallbackPollInterval == null || fallbackPollInterval.isZero() || fallbackPollInterval.isNegative()) {
                throw new HonkerInvalidOptionException("fallbackPollInterval must be positive");
            }
            this.fallbackPollInterval = fallbackPollInterval;
            return this;
        }

        public WaitOptions build() {
            return new WaitOptions(this);
        }
    }
}
