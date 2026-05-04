package dev.honker;

import java.util.List;
import java.util.Optional;

public final class TypedQueue<T> {
    private final Queue queue;
    private final JsonCodec<T> codec;

    TypedQueue(Queue queue, JsonCodec<T> codec) {
        this.queue = queue;
        this.codec = codec;
    }

    public Queue raw() {
        return queue;
    }

    public String name() {
        return queue.name();
    }

    public long enqueue(T payload) {
        return queue.enqueue(codec.encode(payload));
    }

    public long enqueue(T payload, EnqueueOptions options) {
        return queue.enqueue(codec.encode(payload), options);
    }

    public long enqueue(Transaction tx, T payload) {
        return queue.enqueue(tx, codec.encode(payload));
    }

    public long enqueue(Transaction tx, T payload, EnqueueOptions options) {
        return queue.enqueue(tx, codec.encode(payload), options);
    }

    public Optional<TypedJob<T>> claimOne(String workerId) {
        return queue.claimOne(workerId).map(job -> new TypedJob<>(job, codec));
    }

    public List<TypedJob<T>> claimBatch(String workerId, int n) {
        return queue.claimBatch(workerId, n).stream()
            .map(job -> new TypedJob<>(job, codec))
            .toList();
    }

    public WorkerHandle worker(String workerId, TypedJobHandler<T> handler) {
        return worker(workerId, handler, WorkerOptions.defaults());
    }

    public WorkerHandle worker(String workerId, TypedJobHandler<T> handler, WorkerOptions options) {
        return queue.worker(workerId, job -> handler.handle(new TypedJob<>(job, codec)), options);
    }
}
