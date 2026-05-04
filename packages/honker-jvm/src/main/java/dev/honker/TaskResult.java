package dev.honker;

import java.util.Optional;

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
}
