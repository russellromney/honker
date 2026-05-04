package dev.honker;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Optional;

public final class ListenHandle implements AutoCloseable {
    private final Database db;
    private final Listener listener;
    private final NotificationHandler handler;
    private final ListenOptions options;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private Thread ownedThread;

    ListenHandle(Database db, Listener listener, NotificationHandler handler, ListenOptions options) {
        this.db = db;
        this.listener = listener;
        this.handler = handler;
        this.options = options;
        db.register(this);
        start();
    }

    private void start() {
        Runnable loop = this::runLoop;
        Executor executor = options.executor() != null ? options.executor() : db.executor();
        if (executor != null) {
            executor.execute(loop);
        } else {
            ownedThread = new Thread(loop, "honker-listen");
            ownedThread.setDaemon(true);
            ownedThread.start();
        }
    }

    private void runLoop() {
        try {
            while (!closed.get()) {
                listener.next(options.pollTimeout()).ifPresent(n -> {
                    try {
                        handler.handle(n);
                    } catch (Exception e) {
                        throw new HonkerException("notification handler failed", e);
                    }
                });
            }
        } catch (Throwable t) {
            if (!closed.get()) {
                failure.compareAndSet(null, t);
            }
        }
    }

    public boolean failed() {
        return failure.get() != null;
    }

    public Optional<Throwable> failure() {
        return Optional.ofNullable(failure.get());
    }

    public void throwIfFailed() {
        Throwable t = failure.get();
        if (t == null) {
            return;
        }
        if (t instanceof RuntimeException e) {
            throw e;
        }
        if (t instanceof Error e) {
            throw e;
        }
        throw new HonkerException("listener failed", t);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        listener.close();
        if (ownedThread != null) {
            ownedThread.interrupt();
            try {
                ownedThread.join(1_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        db.unregister(this);
    }
}
