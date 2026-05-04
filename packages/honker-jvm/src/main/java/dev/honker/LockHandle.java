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

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        db.transaction(tx -> tx.query("SELECT honker_lock_release(?, ?)", Params.of(name, owner)));
        db.unregister(this);
    }
}
