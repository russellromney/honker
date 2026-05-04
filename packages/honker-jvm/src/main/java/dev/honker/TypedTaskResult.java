package dev.honker;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public final class TypedTaskResult<T> {
    private final TaskResult raw;
    private final JsonCodec<T> codec;

    TypedTaskResult(TaskResult raw, JsonCodec<T> codec) {
        this.raw = raw;
        this.codec = codec;
    }

    public TaskResult raw() {
        return raw;
    }

    public long id() {
        return raw.id();
    }

    public Optional<T> get() {
        return raw.get().map(codec::decode);
    }

    public T waitFor(WaitOptions options) {
        return codec.decode(raw.waitFor(options));
    }

    public CompletableFuture<T> waitForAsync(WaitOptions options) {
        return raw.waitForAsync(options).thenApply(codec::decode);
    }

    public CompletableFuture<T> waitForAsync(WaitOptions options, Executor executor) {
        return raw.waitForAsync(options, executor).thenApply(codec::decode);
    }
}
