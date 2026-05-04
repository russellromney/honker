package dev.honker;

import org.sqlite.SQLiteConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

final class SharedUpdateWatcher implements AutoCloseable {
    private static final Boolean TICK = Boolean.TRUE;
    private static final Boolean CLOSED = Boolean.FALSE;

    private final Path path;
    private final WatcherOptions options;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong nextSubscriberId = new AtomicLong();
    private final Map<Long, Subscriber> subscribers = new ConcurrentHashMap<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final Thread thread;

    SharedUpdateWatcher(Path path, WatcherOptions options) {
        this.path = path.toAbsolutePath().normalize();
        this.options = Objects.requireNonNull(options, "options");
        if (options.backend() != WatcherBackend.PRAGMA_DATA_VERSION) {
            throw new HonkerInvalidOptionException("unsupported watcher backend: " + options.backend());
        }
        this.thread = new Thread(this::run, "honker-update-poll");
        this.thread.setDaemon(true);
        this.thread.start();
    }

    UpdateEvents subscribe(Database db) {
        throwIfFailed();
        long id = nextSubscriberId.incrementAndGet();
        Subscriber subscriber = new Subscriber(options.subscriberBufferSize());
        subscribers.put(id, subscriber);
        return new UpdateEvents(db, this, id, subscriber);
    }

    int subscriberCount() {
        return subscribers.size();
    }

    boolean failed() {
        return failure.get() != null;
    }

    void throwIfFailed() {
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
        throw new HonkerException("update watcher failed", t);
    }

    void unsubscribe(long id) {
        Subscriber subscriber = subscribers.remove(id);
        if (subscriber != null) {
            subscriber.close();
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (Subscriber subscriber : subscribers.values()) {
            subscriber.close();
        }
        subscribers.clear();
        thread.interrupt();
        try {
            thread.join(1_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void run() {
        Object initialIdentity = fileIdentity();
        long nextIdentityCheck = System.nanoTime() + options.identityCheckInterval().toNanos();
        Connection conn = null;
        long version = 0L;
        try {
            while (!closed.get()) {
                try {
                    if (conn == null) {
                        conn = openWatcherConnection();
                        version = dataVersion(conn);
                        wakeSubscribers();
                    } else {
                        long current = dataVersion(conn);
                        if (current != version) {
                            version = current;
                            wakeSubscribers();
                        }
                    }
                } catch (SQLException e) {
                    closeQuietly(conn);
                    conn = null;
                    wakeSubscribers();
                }

                long now = System.nanoTime();
                if (initialIdentity != null && now >= nextIdentityCheck) {
                    nextIdentityCheck = now + options.identityCheckInterval().toNanos();
                    Object currentIdentity = fileIdentity();
                    if (currentIdentity != null && !initialIdentity.equals(currentIdentity)) {
                        throw new HonkerException("database file replaced while update watcher was active: " + path);
                    }
                }

                sleep(options.pollInterval());
            }
        } catch (Throwable t) {
            if (!closed.get()) {
                failure.compareAndSet(null, t);
                wakeSubscribers();
            }
        } finally {
            closeQuietly(conn);
            for (Subscriber subscriber : subscribers.values()) {
                subscriber.close();
            }
        }
    }

    private Connection openWatcherConnection() throws SQLException {
        SQLiteConfig config = new SQLiteConfig();
        config.setReadOnly(true);
        return DriverManager.getConnection("jdbc:sqlite:" + path, config.toProperties());
    }

    private static long dataVersion(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("PRAGMA data_version")) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }

    private Object fileIdentity() {
        try {
            BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
            return attrs.fileKey();
        } catch (IOException e) {
            return null;
        }
    }

    private void wakeSubscribers() {
        for (Subscriber subscriber : subscribers.values()) {
            subscriber.wake();
        }
    }

    private void sleep(Duration duration) {
        try {
            long millis = Math.max(0L, duration.toMillis());
            int nanos = duration.minusMillis(millis).getNano();
            Thread.sleep(millis, nanos);
        } catch (InterruptedException e) {
            if (!closed.get()) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void closeQuietly(Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            conn.close();
        } catch (SQLException ignored) {
        }
    }

    static final class Subscriber {
        private final ArrayBlockingQueue<Boolean> queue;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        Subscriber(int bufferSize) {
            this.queue = new ArrayBlockingQueue<>(bufferSize);
        }

        boolean await(Duration timeout) throws InterruptedException {
            Boolean value;
            if (timeout == null) {
                value = queue.take();
            } else if (timeout.isZero() || timeout.isNegative()) {
                value = queue.poll();
            } else {
                value = queue.poll(timeout.toNanos(), TimeUnit.NANOSECONDS);
            }
            return Boolean.TRUE.equals(value);
        }

        boolean isClosed() {
            return closed.get();
        }

        void wake() {
            if (!closed.get()) {
                queue.offer(TICK);
            }
        }

        void close() {
            if (closed.compareAndSet(false, true)) {
                queue.offer(CLOSED);
            }
        }
    }
}
