package dev.honker;

@FunctionalInterface
public interface StreamHandler {
    void handle(Event event) throws Exception;
}
