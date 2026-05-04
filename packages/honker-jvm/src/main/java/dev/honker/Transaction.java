package dev.honker;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public final class Transaction {
    private final Database db;
    private final Connection conn;
    private boolean active = true;

    Transaction(Database db, Connection conn) {
        this.db = db;
        this.conn = conn;
    }

    public void execute(String sql) {
        execute(sql, List.of());
    }

    public void execute(String sql, List<?> params) {
        ensureActive();
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            Database.bind(stmt, params);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw Sql.error("execute failed", e);
        }
    }

    public List<Row> query(String sql) {
        return query(sql, List.of());
    }

    public List<Row> query(String sql, List<?> params) {
        ensureActive();
        return db.queryOn(conn, sql, params);
    }

    public long notify(String channel, String payloadJson) {
        ensureActive();
        return query("SELECT notify(?, ?) AS id", List.of(channel, payloadJson)).get(0).getLong("id");
    }

    void closeForUser() {
        active = false;
    }

    private void ensureActive() {
        if (!active) {
            throw new HonkerClosedException("Transaction is no longer active");
        }
    }
}
