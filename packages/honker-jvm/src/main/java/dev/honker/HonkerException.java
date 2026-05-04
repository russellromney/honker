package dev.honker;

public class HonkerException extends RuntimeException {
    public HonkerException(String message) {
        super(message);
    }

    public HonkerException(String message, Throwable cause) {
        super(message, cause);
    }
}
