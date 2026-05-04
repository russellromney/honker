package dev.honker;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public final class TaskResult {
    private final Queue queue;
    private final long id;

    TaskResult(Queue queue, long id) {
        this.queue = queue;
        this.id = id;
    }

    public long id() {
        return id;
    }

    public Optional<String> get() {
        return queue.getResult(id);
    }

    public String waitFor(WaitOptions options) {
        return queue.waitResult(id, options);
    }

    public CompletableFuture<String> waitForAsync(WaitOptions options) {
        return queue.waitResultAsync(id, options);
    }

    public CompletableFuture<String> waitForAsync(WaitOptions options, Executor executor) {
        return queue.waitResultAsync(id, options, executor);
    }

    public <T> TypedTaskResult<T> typed(JsonCodec<T> codec) {
        return new TypedTaskResult<>(this, codec);
    }
}
