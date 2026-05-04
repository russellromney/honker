package dev.honker;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Optional;

public final class StreamHandle implements AutoCloseable {
    private final Database db;
    private final Stream stream;
    private final StreamHandler handler;
    private final SubscribeOptions options;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private Thread ownedThread;
    private UpdateEvents updates;

    StreamHandle(Database db, Stream stream, StreamHandler handler, SubscribeOptions options) {
        this.db = db;
        this.stream = stream;
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
            ownedThread = new Thread(loop, "honker-stream-subscribe");
            ownedThread.setDaemon(true);
            ownedThread.start();
        }
    }

    private void runLoop() {
        updates = db.updateEvents();
        long offset = options.fromOffset() != null
            ? options.fromOffset()
            : options.consumer() == null ? 0L : stream.getOffset(options.consumer());
        int sinceSave = 0;
        long lastSave = System.nanoTime();
        try {
            while (!closed.get()) {
                List<Event> events = stream.readSince(offset, 1000);
                if (events.isEmpty()) {
                    updates.awaitUpdate(options.pollTimeout());
                    continue;
                }
                for (Event event : events) {
                    if (closed.get()) {
                        return;
                    }
                    handler.handle(event);
                    offset = event.offset();
                    sinceSave++;
                    if (shouldSave(sinceSave, lastSave)) {
                        stream.saveOffset(options.consumer(), offset);
                        sinceSave = 0;
                        lastSave = System.nanoTime();
                    }
                }
            }
        } catch (Throwable t) {
            if (!closed.get()) {
                failure.compareAndSet(null, t instanceof HonkerException ? t : new HonkerException("stream handler failed", t));
            }
        } finally {
            if (updates != null) {
                updates.close();
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
        throw new HonkerException("stream failed", t);
    }

    private boolean shouldSave(int sinceSave, long lastSave) {
        if (options.consumer() == null) {
            return false;
        }
        boolean count = options.saveEveryN() > 0 && sinceSave >= options.saveEveryN();
        boolean time = !options.saveEvery().isZero()
            && Duration.ofNanos(System.nanoTime() - lastSave).compareTo(options.saveEvery()) >= 0;
        return count || time;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (updates != null) {
            updates.close();
        }
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
