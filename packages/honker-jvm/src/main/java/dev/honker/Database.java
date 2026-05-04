package dev.honker;

import org.sqlite.SQLiteConfig;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Database implements AutoCloseable {
    private final Path path;
    private final OpenOptions options;
    private final Connection conn;
    private final Object lock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final List<AutoCloseable> children = new CopyOnWriteArrayList<>();
    private SharedUpdateWatcher updateWatcher;

    private Database(Path path, OpenOptions options, Connection conn) {
        this.path = path;
        this.options = options;
        this.conn = conn;
    }

    static Database open(Path path, OpenOptions options) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(options, "options");
        Path dbPath = path.toAbsolutePath().normalize();
        Path extension = NativeLoader.resolve(options);
        SQLiteConfig config = new SQLiteConfig();
        config.enableLoadExtension(true);
        config.setBusyTimeout(Math.toIntExact(options.busyTimeout().toMillis()));
        try {
            Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath, config.toProperties());
            Database db = new Database(dbPath, options, conn);
            db.initialize(extension);
            return db;
        } catch (SQLException e) {
            throw Sql.error("failed to open Honker database", e);
        }
    }

    public Path path() {
        return path;
    }

    public Queue queue(String name) {
        return queue(name, QueueOptions.defaults());
    }

    public Queue queue(String name, QueueOptions options) {
        ensureOpen();
        return new Queue(this, name, options);
    }

    public Outbox outbox(String name, Delivery delivery) {
        return outbox(name, delivery, OutboxOptions.defaults());
    }

    public Outbox outbox(String name, Delivery delivery, OutboxOptions options) {
        ensureOpen();
        return new Outbox(this, name, delivery, options);
    }

    public Stream stream(String name) {
        ensureOpen();
        return new Stream(this, name);
    }

    public Scheduler scheduler() {
        ensureOpen();
        return new Scheduler(this);
    }

    public LockHandle lock(String name) {
        return lock(name, LockOptions.defaults());
    }

    public LockHandle lock(String name, LockOptions options) {
        ensureOpen();
        String owner = options.owner() == null ? java.util.UUID.randomUUID().toString() : options.owner();
        boolean acquired = transaction(tx -> tx.query(
            "SELECT honker_lock_acquire(?, ?, ?) AS r",
            List.of(name, owner, Durations.seconds(options.ttl(), "lock ttl"))
        ).get(0).getBoolean("r"));
        if (!acquired) {
            throw new LockHeldException("lock " + name + " is already held");
        }
        return new LockHandle(this, name, owner);
    }

    public boolean tryRateLimit(String name, int limit, Duration per) {
        if (limit <= 0 || per == null || per.isZero() || per.isNegative()) {
            throw new HonkerInvalidOptionException("limit and per must be positive");
        }
        return transaction(tx -> tx.query(
            "SELECT honker_rate_limit_try(?, ?, ?) AS r",
            List.of(name, limit, Durations.seconds(per, "per"))
        ).get(0).getBoolean("r"));
    }

    public int sweepRateLimits(Duration olderThan) {
        Duration d = Objects.requireNonNull(olderThan, "olderThan");
        if (d.isNegative()) {
            throw new HonkerInvalidOptionException("olderThan must not be negative");
        }
        return transaction(tx -> tx.query(
            "SELECT honker_rate_limit_sweep(?) AS n",
            List.of(Durations.seconds(d, "olderThan"))
        ).get(0).getInt("n"));
    }

    public long notify(String channel, String payloadJson) {
        return transaction(tx -> tx.notify(channel, payloadJson));
    }

    public int pruneNotifications(NotificationPruneOptions options) {
        Objects.requireNonNull(options, "options");
        List<String> conditions = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        if (options.olderThan() != null) {
            conditions.add("created_at < unixepoch() - ?");
            params.add(Durations.seconds(options.olderThan(), "olderThan"));
        }
        if (options.maxKeep() != null) {
            long maxId = query("SELECT COALESCE(MAX(id), 0) AS m FROM _honker_notifications")
                .get(0).getLong("m");
            long threshold = maxId - options.maxKeep();
            if (threshold >= 1L) {
                conditions.add("id <= ?");
                params.add(threshold);
            }
        }
        if (conditions.isEmpty()) {
            return 0;
        }
        return transaction(tx -> tx.query(
            "DELETE FROM _honker_notifications WHERE " + String.join(" OR ", conditions) + " RETURNING id",
            params
        ).size());
    }

    public Listener listen(String channel) {
        ensureOpen();
        Listener listener = new Listener(this, channel);
        register(listener);
        return listener;
    }

    public ListenHandle listen(String channel, NotificationHandler handler) {
        return listen(channel, handler, ListenOptions.defaults());
    }

    public ListenHandle listen(String channel, NotificationHandler handler, ListenOptions options) {
        return new ListenHandle(this, listen(channel), handler, options);
    }

    public UpdateEvents updateEvents() {
        ensureOpen();
        UpdateEvents updates = sharedUpdateWatcher().subscribe(this);
        register(updates);
        return updates;
    }

    public TaskWorkerHandle runTasks(TaskRegistry registry) {
        return runTasks(registry, TaskWorkerOptions.defaults());
    }

    public TaskWorkerHandle runTasks(TaskRegistry registry, TaskWorkerOptions options) {
        return new TaskWorkerHandle(this, registry, options);
    }

    public <T> T transaction(TransactionCallback<T> callback) {
        Objects.requireNonNull(callback, "callback");
        ensureOpen();
        synchronized (lock) {
            Transaction tx = new Transaction(this, conn);
            try {
                execRaw(conn, "BEGIN IMMEDIATE");
                T result = callback.run(tx);
                execRaw(conn, "COMMIT");
                tx.closeForUser();
                return result;
            } catch (RuntimeException | Error e) {
                rollbackQuietly(conn);
                tx.closeForUser();
                throw e;
            }
        }
    }

    public void transactionVoid(TransactionBody body) {
        transaction(tx -> {
            body.run(tx);
            return null;
        });
    }

    public List<Row> query(String sql) {
        return query(sql, List.of());
    }

    public List<Row> query(String sql, List<?> params) {
        ensureOpen();
        synchronized (lock) {
            return queryOn(conn, sql, params);
        }
    }

    List<Row> queryOn(Connection connection, String sql, List<?> params) {
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            bind(stmt, params);
            try (ResultSet rs = stmt.executeQuery()) {
                return rows(rs);
            }
        } catch (SQLException e) {
            throw Sql.error("query failed", e);
        }
    }

    void register(AutoCloseable child) {
        children.add(child);
    }

    void unregister(AutoCloseable child) {
        children.remove(child);
    }

    Executor executor() {
        return options.executor();
    }

    Duration fallbackPollInterval() {
        return options.fallbackPollInterval();
    }

    int updateWatcherSubscriberCount() {
        SharedUpdateWatcher watcher;
        synchronized (lock) {
            watcher = updateWatcher;
        }
        return watcher == null ? 0 : watcher.subscriberCount();
    }

    void ensureOpen() {
        if (closed.get()) {
            throw new HonkerClosedException("Database is closed");
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (AutoCloseable child : children) {
            try {
                child.close();
            } catch (Exception ignored) {
            }
        }
        synchronized (lock) {
            if (updateWatcher != null) {
                updateWatcher.close();
                updateWatcher = null;
            }
            try {
                conn.close();
            } catch (SQLException e) {
                throw Sql.error("failed to close database", e);
            }
        }
    }

    private SharedUpdateWatcher sharedUpdateWatcher() {
        synchronized (lock) {
            ensureOpen();
            if (updateWatcher == null || updateWatcher.failed()) {
                if (updateWatcher != null) {
                    updateWatcher.close();
                }
                updateWatcher = new SharedUpdateWatcher(path, options.watcherOptions());
            }
            return updateWatcher;
        }
    }

    private void initialize(Path extension) {
        synchronized (lock) {
            execRaw(conn, "PRAGMA journal_mode=" + options.journalMode());
            execRaw(conn, "PRAGMA wal_autocheckpoint=" + options.walAutocheckpoint());
            try (PreparedStatement stmt = conn.prepareStatement("SELECT load_extension(?, ?)")) {
                stmt.setString(1, extension.toString());
                stmt.setString(2, NativeLoader.ENTRYPOINT);
                stmt.executeQuery().close();
            } catch (SQLException e) {
                throw new HonkerLoadException("failed to load Honker extension from " + extension, e);
            }
            queryOn(conn, "SELECT honker_bootstrap() AS ok", List.of());
        }
    }

    private static void execRaw(Connection c, String sql) {
        try (Statement stmt = c.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw Sql.error("execute failed", e);
        }
    }

    private static void rollbackQuietly(Connection c) {
        try (Statement stmt = c.createStatement()) {
            stmt.execute("ROLLBACK");
        } catch (SQLException ignored) {
        }
    }

    static void bind(PreparedStatement stmt, List<?> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            Object value = params.get(i);
            int idx = i + 1;
            if (value == null) {
                stmt.setObject(idx, null);
            } else if (value instanceof Integer v) {
                stmt.setInt(idx, v);
            } else if (value instanceof Long v) {
                stmt.setLong(idx, v);
            } else if (value instanceof Boolean v) {
                stmt.setInt(idx, v ? 1 : 0);
            } else if (value instanceof Number v) {
                stmt.setDouble(idx, v.doubleValue());
            } else {
                stmt.setString(idx, value.toString());
            }
        }
    }

    private static List<Row> rows(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int n = meta.getColumnCount();
        List<Row> rows = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> values = new java.util.LinkedHashMap<>();
            for (int i = 1; i <= n; i++) {
                values.put(meta.getColumnLabel(i), rs.getObject(i));
            }
            rows.add(Row.mutable(values));
        }
        return rows;
    }
}
