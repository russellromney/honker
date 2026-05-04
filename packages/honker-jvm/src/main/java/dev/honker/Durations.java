package dev.honker;

import java.time.Duration;
import java.util.Objects;

final class Durations {
    private Durations() {
    }

    static Duration positiveSeconds(Duration value, String name) {
        Duration d = wholeSeconds(Objects.requireNonNull(value, name), name);
        if (d.isZero() || d.isNegative()) {
            throw new HonkerInvalidOptionException(name + " must be positive");
        }
        return d;
    }

    static Duration nonNegativeSeconds(Duration value, String name) {
        Duration d = wholeSeconds(Objects.requireNonNull(value, name), name);
        if (d.isNegative()) {
            throw new HonkerInvalidOptionException(name + " must not be negative");
        }
        return d;
    }

    static long seconds(Duration value, String name) {
        return wholeSeconds(Objects.requireNonNull(value, name), name).toSeconds();
    }

    private static Duration wholeSeconds(Duration value, String name) {
        if (value.getNano() != 0) {
            throw new HonkerInvalidOptionException(name + " must be a whole-second duration");
        }
        return value;
    }
}
