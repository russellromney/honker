package dev.honker;

import java.time.Duration;

public final class RetryableException extends HonkerException {
    private final Duration delay;

    public RetryableException(String message, Duration delay) {
        super(message);
        this.delay = Durations.nonNegativeSeconds(delay, "retry delay");
    }

    public Duration delay() {
        return delay;
    }
}
