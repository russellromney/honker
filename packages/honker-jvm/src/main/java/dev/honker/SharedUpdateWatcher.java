package dev.honker;

import org.sqlite.SQLiteConfig;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
    private static final int WAL_INDEX_VERSION = 3_007_000;
    private static final int WAL_INDEX_HEADER_SIZE = 48;
    private static final int WAL_INDEX_I_CHANGE_OFFSET = 8;
    private static final int WAL_INDEX_MIN_SIZE = WAL_INDEX_HEADER_SIZE * 2;

    private final Path path;
    private final WatcherOptions options;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong nextSubscriberId = new AtomicLong();
    private final Map<Long, Subscriber> subscribers = new ConcurrentHashMap<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicReference<WatcherBackend> activeBackend = new AtomicReference<>();
    private final Thread thread;

    SharedUpdateWatcher(Path path, WatcherOptions options) {
        this.path = path.toAbsolutePath().normalize();
        this.options = Objects.requireNonNull(options, "options");
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

    WatcherBackend activeBackend() {
        return activeBackend.get();
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
        VersionSource source = null;
        long version = 0L;
        try {
            while (!closed.get()) {
                try {
                    if (source == null) {
                        source = openVersionSource();
                        activeBackend.set(source.backend());
                        version = source.version();
                        wakeSubscribers();
                    } else {
                        long current = source.version();
                        if (current != version) {
                            version = current;
                            wakeSubscribers();
                        }
                    }
                } catch (IOException | SQLException | RuntimeException e) {
                    closeQuietly(source);
                    source = null;
                    wakeSubscribers();
                    if (options.backend() == WatcherBackend.MMAP_SHM && e instanceof HonkerException) {
                        throw e;
                    }
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
            closeQuietly(source);
            for (Subscriber subscriber : subscribers.values()) {
                subscriber.close();
            }
        }
    }

    private VersionSource openVersionSource() throws SQLException, IOException {
        if (options.backend() == WatcherBackend.AUTO || options.backend() == WatcherBackend.PRAGMA_DATA_VERSION) {
            return new PragmaVersionSource(path);
        }
        if (options.backend() == WatcherBackend.MMAP_SHM) {
            return new ShmVersionSource(path);
        }
        return new PragmaVersionSource(path);
    }

    private static Connection openWatcherConnection(Path path) throws SQLException {
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

    private static void closeQuietly(VersionSource source) {
        if (source == null) {
            return;
        }
        try {
            source.close();
        } catch (Exception ignored) {
        }
    }

    private interface VersionSource extends AutoCloseable {
        long version() throws SQLException, IOException;

        WatcherBackend backend();

        @Override
        void close() throws Exception;
    }

    private static final class PragmaVersionSource implements VersionSource {
        private final Connection conn;

        PragmaVersionSource(Path path) throws SQLException {
            this.conn = openWatcherConnection(path);
        }

        @Override
        public long version() throws SQLException {
            return dataVersion(conn);
        }

        @Override
        public WatcherBackend backend() {
            return WatcherBackend.PRAGMA_DATA_VERSION;
        }

        @Override
        public void close() throws SQLException {
            conn.close();
        }
    }

    private static final class ShmVersionSource implements VersionSource {
        private final FileChannel channel;
        private final MappedByteBuffer mmap;

        ShmVersionSource(Path dbPath) throws IOException {
            this.channel = FileChannel.open(shmPath(dbPath), StandardOpenOption.READ);
            if (channel.size() < WAL_INDEX_MIN_SIZE) {
                channel.close();
                throw new HonkerException("SQLite WAL index is too small for mmap watcher: " + shmPath(dbPath));
            }
            this.mmap = channel.map(FileChannel.MapMode.READ_ONLY, 0, WAL_INDEX_MIN_SIZE);
            this.mmap.order(ByteOrder.LITTLE_ENDIAN);
            int version = readInt(0);
            int version2 = readInt(WAL_INDEX_HEADER_SIZE);
            if (version != WAL_INDEX_VERSION || version2 != WAL_INDEX_VERSION) {
                try {
                    channel.close();
                } catch (IOException ignored) {
                }
                throw new HonkerException("unsupported SQLite WAL index version for mmap watcher: " + version + "/" + version2);
            }
        }

        @Override
        public long version() {
            return Integer.toUnsignedLong(readStableInt(WAL_INDEX_I_CHANGE_OFFSET));
        }

        @Override
        public WatcherBackend backend() {
            return WatcherBackend.MMAP_SHM;
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

        private int readStableInt(int offset) {
            for (int i = 0; i < 10_000; i++) {
                int first = readInt(offset);
                VarHandleFence.acquireFence();
                int second = readInt(WAL_INDEX_HEADER_SIZE + offset);
                if (first == second) {
                    return first;
                }
                Thread.onSpinWait();
            }
            return readInt(offset);
        }

        private int readInt(int offset) {
            return mmap.getInt(offset);
        }

        private static Path shmPath(Path dbPath) {
            return Path.of(dbPath.toString() + "-shm");
        }
    }

    private static final class VarHandleFence {
        private VarHandleFence() {
        }

        static void acquireFence() {
            java.lang.invoke.VarHandle.acquireFence();
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
