package dev.honker;

import java.util.concurrent.atomic.AtomicBoolean;

public final class LockHandle implements AutoCloseable {
    private final Database db;
    private final String name;
    private final String owner;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    LockHandle(Database db, String name, String owner) {
        this.db = db;
        this.name = name;
        this.owner = owner;
        db.register(this);
    }

    public String name() {
        return name;
    }

    public String owner() {
        return owner;
    }

    /**
     * Extend this lock's TTL for the current owner. Returns true if we
     * still hold it; false if the TTL elapsed and another owner took
     * over (or the row was released).
     *
     * Uses {@code honker_lock_renew} — {@code honker_lock_acquire}
     * does not refresh {@code expires_at} for an existing owner.
     */
    public boolean renew(java.time.Duration ttl) {
        if (closed.get()) {
            return false;
        }
        long ok = db.transaction(tx -> tx.query(
            "SELECT honker_lock_renew(?, ?, ?) AS r",
            Params.of(name, owner, Durations.seconds(ttl, "ttl"))
        ).get(0).getInt("r"));
        return ok != 0;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        db.transaction(tx -> tx.query("SELECT honker_lock_release(?, ?)", Params.of(name, owner)));
        db.unregister(this);
    }
}
