package dev.honker;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executor;

public final class OpenOptions {
    private final Path extensionPath;
    private final int maxReaders;
    private final Duration busyTimeout;
    private final String journalMode;
    private final int walAutocheckpoint;
    private final Duration fallbackPollInterval;
    private final WatcherOptions watcherOptions;
    private final Executor executor;

    private OpenOptions(Builder builder) {
        this.extensionPath = builder.extensionPath;
        this.maxReaders = builder.maxReaders;
        this.busyTimeout = builder.busyTimeout;
        this.journalMode = builder.journalMode;
        this.walAutocheckpoint = builder.walAutocheckpoint;
        this.fallbackPollInterval = builder.fallbackPollInterval;
        this.watcherOptions = builder.watcherOptions;
        this.executor = builder.executor;
    }

    public static OpenOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public Path extensionPath() {
        return extensionPath;
    }

    public int maxReaders() {
        return maxReaders;
    }

    public Duration busyTimeout() {
        return busyTimeout;
    }

    public String journalMode() {
        return journalMode;
    }

    public int walAutocheckpoint() {
        return walAutocheckpoint;
    }

    public Duration fallbackPollInterval() {
        return fallbackPollInterval;
    }

    public WatcherOptions watcherOptions() {
        return watcherOptions;
    }

    public Executor executor() {
        return executor;
    }

    public static final class Builder {
        private Path extensionPath;
        private int maxReaders = 8;
        private Duration busyTimeout = Duration.ofSeconds(5);
        private String journalMode = "WAL";
        private int walAutocheckpoint = 10_000;
        private Duration fallbackPollInterval = Duration.ofMillis(10);
        private WatcherOptions watcherOptions = WatcherOptions.defaults();
        private Executor executor;

        public Builder extensionPath(String extensionPath) {
            this.extensionPath = extensionPath == null ? null : Path.of(extensionPath);
            return this;
        }

        public Builder extensionPath(Path extensionPath) {
            this.extensionPath = extensionPath;
            return this;
        }

        public Builder maxReaders(int maxReaders) {
            if (maxReaders < 0) {
                throw new HonkerInvalidOptionException("maxReaders must be non-negative");
            }
            this.maxReaders = maxReaders;
            return this;
        }

        public Builder busyTimeout(Duration busyTimeout) {
            this.busyTimeout = positiveOrZero(busyTimeout, "busyTimeout");
            return this;
        }

        public Builder journalMode(String journalMode) {
            this.journalMode = Objects.requireNonNull(journalMode, "journalMode");
            return this;
        }

        public Builder walAutocheckpoint(int walAutocheckpoint) {
            if (walAutocheckpoint <= 0) {
                throw new HonkerInvalidOptionException("walAutocheckpoint must be positive");
            }
            this.walAutocheckpoint = walAutocheckpoint;
            return this;
        }

        public Builder fallbackPollInterval(Duration fallbackPollInterval) {
            Duration d = positiveOrZero(fallbackPollInterval, "fallbackPollInterval");
            if (d.isZero()) {
                throw new HonkerInvalidOptionException("fallbackPollInterval must be positive");
            }
            this.fallbackPollInterval = d;
            return this;
        }

        public Builder watcherOptions(WatcherOptions watcherOptions) {
            this.watcherOptions = Objects.requireNonNull(watcherOptions, "watcherOptions");
            return this;
        }

        public Builder executor(Executor executor) {
            this.executor = executor;
            return this;
        }

        public OpenOptions build() {
            return new OpenOptions(this);
        }

        private static Duration positiveOrZero(Duration value, String name) {
            Objects.requireNonNull(value, name);
            if (value.isNegative()) {
                throw new HonkerInvalidOptionException(name + " must not be negative");
            }
            return value;
        }
    }
}
