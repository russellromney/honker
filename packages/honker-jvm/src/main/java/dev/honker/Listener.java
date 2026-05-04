package dev.honker;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Listener implements AutoCloseable {
    private final Database db;
    private final String channel;
    private final UpdateEvents updates;
    private final Queue<Notification> buffer = new ArrayDeque<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private long lastSeen;

    Listener(Database db, String channel) {
        this.db = db;
        this.channel = channel;
        this.updates = db.updateEvents();
        List<Row> rows = db.query(
            "SELECT COALESCE(MAX(id), 0) AS m FROM _honker_notifications WHERE channel=?",
            Params.of(channel)
        );
        this.lastSeen = rows.get(0).getLong("m");
    }

    public Optional<Notification> next(Duration timeout) {
        long deadline = timeout == null ? 0L : System.nanoTime() + timeout.toNanos();
        while (!closed.get()) {
            if (!buffer.isEmpty()) {
                return Optional.of(buffer.remove());
            }
            List<Row> rows = db.query(
                "SELECT id, channel, payload, created_at FROM _honker_notifications WHERE channel=? AND id > ? ORDER BY id",
                Params.of(channel, lastSeen)
            );
            for (Row row : rows) {
                lastSeen = row.getLong("id");
                buffer.add(new Notification(
                    row.getLong("id"),
                    row.getString("channel"),
                    row.getString("payload"),
                    row.getLong("created_at")
                ));
            }
            if (!buffer.isEmpty()) {
                continue;
            }
            Duration wait = db.fallbackPollInterval();
            if (deadline != 0L) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) {
                    return Optional.empty();
                }
                Duration r = Duration.ofNanos(remaining);
                if (r.compareTo(wait) < 0) {
                    wait = r;
                }
            }
            updates.awaitUpdate(wait);
        }
        return Optional.empty();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        updates.close();
        db.unregister(this);
    }
}
