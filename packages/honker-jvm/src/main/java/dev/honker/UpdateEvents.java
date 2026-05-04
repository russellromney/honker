package dev.honker;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

public final class UpdateEvents implements AutoCloseable {
    private final Database db;
    private final SharedUpdateWatcher watcher;
    private final long id;
    private final SharedUpdateWatcher.Subscriber subscriber;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    UpdateEvents(Database db, SharedUpdateWatcher watcher, long id, SharedUpdateWatcher.Subscriber subscriber) {
        this.db = db;
        this.watcher = watcher;
        this.id = id;
        this.subscriber = subscriber;
    }

    public boolean awaitUpdate(Duration timeout) {
        if (closed.get()) {
            return false;
        }
        watcher.throwIfFailed();
        try {
            boolean updated = subscriber.await(timeout);
            watcher.throwIfFailed();
            return updated && !closed.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    boolean isClosed() {
        return closed.get() || subscriber.isClosed();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        watcher.unsubscribe(id);
        db.unregister(this);
    }
}
