package dev.honker;

@FunctionalInterface
public interface TypedJobHandler<T> {
    void handle(TypedJob<T> job) throws Exception;
}
