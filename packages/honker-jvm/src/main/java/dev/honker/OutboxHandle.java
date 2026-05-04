package dev.honker;

import java.util.Optional;

public final class OutboxHandle implements AutoCloseable {
    private final WorkerHandle worker;

    OutboxHandle(WorkerHandle worker) {
        this.worker = worker;
    }

    @Override
    public void close() {
        worker.close();
    }

    public boolean failed() {
        return worker.failed();
    }

    public Optional<Throwable> failure() {
        return worker.failure();
    }

    public void throwIfFailed() {
        worker.throwIfFailed();
    }
}
