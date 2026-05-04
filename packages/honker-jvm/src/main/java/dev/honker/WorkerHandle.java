package dev.honker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Optional;

public final class WorkerHandle implements AutoCloseable {
    private final Database db;
    private final Queue queue;
    private final String workerId;
    private final JobHandler handler;
    private final WorkerOptions options;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final List<Thread> ownedThreads = new ArrayList<>();
    private final List<UpdateEvents> updates = new ArrayList<>();

    WorkerHandle(Database db, Queue queue, String workerId, JobHandler handler, WorkerOptions options) {
        this.db = db;
        this.queue = queue;
        this.workerId = workerId;
        this.handler = handler;
        this.options = options;
        db.register(this);
        start();
    }

    private void start() {
        Executor executor = options.executor() != null ? options.executor() : db.executor();
        for (int i = 0; i < options.concurrency(); i++) {
            int idx = i;
            Runnable loop = () -> runLoop(workerId + "-" + idx);
            if (executor != null) {
                executor.execute(loop);
            } else {
                Thread t = new Thread(loop, "honker-worker-" + queue.name() + "-" + idx);
                t.setDaemon(true);
                ownedThreads.add(t);
                t.start();
            }
        }
    }

    private void runLoop(String worker) {
        try (UpdateEvents update = db.updateEvents()) {
            synchronized (updates) {
                updates.add(update);
            }
            while (!closed.get()) {
                var claimed = queue.claimOne(worker);
                if (claimed.isPresent()) {
                    handle(claimed.get());
                    continue;
                }
                Duration wait = options.idlePollInterval();
                long next = queue.nextClaimAt();
                if (next > 0L) {
                    long millis = Math.max(1L, next * 1000L - System.currentTimeMillis());
                    Duration until = Duration.ofMillis(millis);
                    if (until.compareTo(wait) < 0) {
                        wait = until;
                    }
                }
                update.awaitUpdate(wait);
            }
        } catch (Throwable t) {
            if (!closed.get()) {
                failure.compareAndSet(null, t);
            }
        } finally {
            synchronized (updates) {
                updates.removeIf(UpdateEvents::isClosed);
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
        throw new HonkerException("worker failed", t);
    }

    private void handle(Job job) {
        try {
            handler.handle(job);
            job.ack();
        } catch (RetryableException e) {
            job.retry(e.delay(), e.getMessage());
        } catch (Exception e) {
            job.retry(options.defaultRetryDelay(), e.toString());
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        synchronized (updates) {
            for (UpdateEvents update : updates) {
                update.close();
            }
        }
        for (Thread thread : ownedThreads) {
            thread.interrupt();
        }
        for (Thread thread : ownedThreads) {
            try {
                thread.join(1_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        db.unregister(this);
    }
}
