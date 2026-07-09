package dev.honker;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Optional;

public final class SchedulerHandle implements AutoCloseable {
    private final Database db;
    private final Scheduler scheduler;
    private final SchedulerOptions options;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private Thread ownedThread;
    private UpdateEvents updates;
    private LockHandle lock;

    SchedulerHandle(Database db, Scheduler scheduler, SchedulerOptions options) {
        this.db = db;
        this.scheduler = scheduler;
        this.options = options;
        this.lock = db.lock(options.lockName(), LockOptions.builder().ttl(options.lockTtl()).build());
        this.updates = db.updateEvents();
        try {
            db.register(this);
            start();
        } catch (RuntimeException | Error e) {
            close();
            throw e;
        }
    }

    private void start() {
        Runnable loop = this::runLoop;
        Executor executor = options.executor() != null ? options.executor() : db.executor();
        if (executor != null) {
            executor.execute(loop);
        } else {
            ownedThread = new Thread(loop, "honker-scheduler");
            ownedThread.setDaemon(true);
            ownedThread.start();
        }
    }

    private void runLoop() {
        long lastHeartbeat = System.nanoTime();
        try {
            while (!closed.get()) {
                scheduler.tick(Instant.now());
                if (Duration.ofNanos(System.nanoTime() - lastHeartbeat).compareTo(options.heartbeatEvery()) >= 0) {
                    heartbeat();
                    lastHeartbeat = System.nanoTime();
                }
                Duration wait = scheduler.soonest()
                    .map(t -> Duration.between(Instant.now(), t))
                    .filter(d -> !d.isNegative() && !d.isZero())
                    .orElse(options.idlePollInterval());
                if (wait.compareTo(options.idlePollInterval()) > 0) {
                    wait = options.idlePollInterval();
                }
                updates.awaitUpdate(wait);
            }
        } catch (Throwable t) {
            if (!closed.get()) {
                failure.compareAndSet(null, t);
            }
        } finally {
            if (updates != null) {
                updates.close();
            }
            if (lock != null) {
                lock.close();
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
        throw new HonkerException("scheduler failed", t);
    }

    private void heartbeat() {
        // honker_lock_renew(name, owner, ttl_s). Must check return
        // value so a stolen lock stops the leader loop.
        int ok = db.transaction(tx -> tx.query(
            "SELECT honker_lock_renew(?, ?, ?) AS r",
            Params.of(
                lock.name(),
                lock.owner(),
                Durations.seconds(options.lockTtl(), "lockTtl")
            )
        ).get(0).getInt("r"));
        if (ok == 0) {
            closed.set(true);
        }
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
        if (lock != null) {
            lock.close();
        }
        db.unregister(this);
    }
}
