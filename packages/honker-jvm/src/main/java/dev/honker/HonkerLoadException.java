package dev.honker;

public final class HonkerLoadException extends HonkerException {
    public HonkerLoadException(String message) {
        super(message);
    }

    public HonkerLoadException(String message, Throwable cause) {
        super(message, cause);
    }
}
