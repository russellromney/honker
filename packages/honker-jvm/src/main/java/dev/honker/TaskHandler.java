package dev.honker;

@FunctionalInterface
public interface TaskHandler {
    String handle(TaskCall call) throws Exception;
}
