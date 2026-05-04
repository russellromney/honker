package dev.honker;

@FunctionalInterface
public interface JobHandler {
    void handle(Job job) throws Exception;
}
