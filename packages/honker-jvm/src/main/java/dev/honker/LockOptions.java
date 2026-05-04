package dev.honker;

import java.time.Duration;

public final class LockOptions {
    private final Duration ttl;
    private final String owner;

    private LockOptions(Builder builder) {
        this.ttl = builder.ttl;
        this.owner = builder.owner;
    }

    public static LockOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public Duration ttl() {
        return ttl;
    }

    public String owner() {
        return owner;
    }

    public static final class Builder {
        private Duration ttl = Duration.ofSeconds(60);
        private String owner;

        public Builder ttl(Duration ttl) {
            this.ttl = Durations.positiveSeconds(ttl, "lock ttl");
            return this;
        }

        public Builder owner(String owner) {
            this.owner = owner;
            return this;
        }

        public LockOptions build() {
            return new LockOptions(this);
        }
    }
}
