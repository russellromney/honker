package dev.honker;

import java.time.Duration;

public final class ResultOptions {
    private final Duration ttl;

    private ResultOptions(Duration ttl) {
        this.ttl = ttl;
    }

    public static ResultOptions neverExpires() {
        return new ResultOptions(null);
    }

    public static ResultOptions ttl(Duration ttl) {
        return new ResultOptions(Durations.positiveSeconds(ttl, "result ttl"));
    }

    public Duration ttl() {
        return ttl;
    }
}
