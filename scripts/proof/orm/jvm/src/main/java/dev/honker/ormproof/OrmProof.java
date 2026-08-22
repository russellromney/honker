package dev.honker.ormproof;

// The documented JVM recipes from guides/orm/jvm.mdx: raw JDBC, the
// HonkerSql helper the page defines, Hibernate's doWork unwrap, and
// jOOQ's transaction callback. Each runs an enqueue -> claim -> ack
// round trip with bound parameters.

import dev.honker.HonkerExtension;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import org.hibernate.Session;
import org.hibernate.cfg.Configuration;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.sqlite.SQLiteConfig;

public final class OrmProof {

    // The helper exactly as the page defines it.
    static long enqueue(Connection conn, String queue, String payloadJson) throws Exception {
        try (var stmt = conn.prepareStatement(
                "SELECT honker_enqueue(?, ?, NULL, NULL, 0, 3, NULL) AS id")) {
            stmt.setString(1, queue);
            stmt.setString(2, payloadJson);
            try (var rs = stmt.executeQuery()) {
                rs.next();
                return rs.getLong("id");
            }
        }
    }

    static void claimAndAck(Connection conn, String queue, long id) throws Exception {
        String claimed;
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT honker_claim_batch(?, ?, ?, ?)")) {
            stmt.setString(1, queue);
            stmt.setString(2, "w1");
            stmt.setInt(3, 8);
            stmt.setInt(4, 300);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                claimed = rs.getString(1);
            }
        }
        if (!claimed.contains("\"id\":" + id)) {
            throw new IllegalStateException("claimed the wrong job: " + claimed);
        }
        try (PreparedStatement stmt = conn.prepareStatement("SELECT honker_ack(?, ?)")) {
            stmt.setLong(1, id);
            stmt.setString(2, "w1");
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                if (rs.getInt(1) != 1) {
                    throw new IllegalStateException("ack must match the claim");
                }
            }
        }
    }

    static String jdbcUrl(String dbPath) {
        return "jdbc:sqlite:" + dbPath;
    }

    // The page's wiring: enable extension loading before the connection
    // opens, then load with an explicit entry point.
    static Properties extensionProps() {
        var config = new SQLiteConfig();
        config.enableLoadExtension(true);
        return config.toProperties();
    }

    static void loadHonker(Statement stmt) throws Exception {
        stmt.execute("SELECT load_extension('" + HonkerExtension.path() + "', '"
                + HonkerExtension.entrypoint() + "')");
        stmt.execute("SELECT honker_bootstrap()");
    }

    public static void main(String[] args) throws Exception {
        String dbPath = System.getenv("HONKER_TEST_DB");
        if (dbPath == null || dbPath.isBlank()) {
            throw new IllegalStateException("HONKER_TEST_DB is required");
        }

        // ---- raw JDBC -------------------------------------------------
        try (var conn = DriverManager.getConnection(jdbcUrl(dbPath), extensionProps());
                var stmt = conn.createStatement()) {
            loadHonker(stmt);
            long id = enqueue(conn, "emails_jdbc", "{\"to\":\"alice@example.com\"}");
            if (id <= 0) {
                throw new IllegalStateException("expected a job id, got " + id);
            }
            claimAndAck(conn, "emails_jdbc", id);
        }
        System.out.println("PASS jvm-jdbc");

        // ---- Hibernate / JPA -----------------------------------------
        var cfg = new Configuration()
                .setProperty("hibernate.connection.driver_class", "org.sqlite.JDBC")
                .setProperty("hibernate.connection.url", jdbcUrl(dbPath))
                .setProperty("hibernate.dialect", "org.hibernate.community.dialect.SQLiteDialect")
                .setProperty("hibernate.connection.enable_load_extension", "true")
                .setProperty("hibernate.hbm2ddl.auto", "none");
        try (var sf = cfg.buildSessionFactory(); var session = sf.openSession()) {
            session.beginTransaction();
            // The documented shape: unwrap the JDBC connection inside the
            // ORM's own transaction so the enqueue shares it.
            session.doWork(conn -> {
                try (var stmt = conn.createStatement()) {
                    loadHonker(stmt);
                    long id = enqueue(conn, "emails_hib", "{\"to\":\"alice@example.com\"}");
                    if (id <= 0) {
                        throw new IllegalStateException("expected a job id, got " + id);
                    }
                    claimAndAck(conn, "emails_hib", id);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            session.getTransaction().commit();
        }
        System.out.println("PASS jvm-hibernate");

        // ---- jOOQ -----------------------------------------------------
        try (var conn = DriverManager.getConnection(jdbcUrl(dbPath), extensionProps());
                var stmt = conn.createStatement()) {
            loadHonker(stmt);
            var ctx = DSL.using(conn, SQLDialect.SQLITE);
            ctx.transaction(configuration -> {
                var tx = DSL.using(configuration);
                Object raw = tx.fetchValue(
                        "SELECT honker_enqueue(?, ?, NULL, NULL, 0, 3, NULL)",
                        "emails_jooq",
                        "{\"to\":\"alice@example.com\"}");
                Long id = raw == null ? null : ((Number) raw).longValue();
                if (id == null || id <= 0) {
                    throw new IllegalStateException("expected a job id, got " + id);
                }
                claimAndAck(conn, "emails_jooq", id);
            });
        }
        System.out.println("PASS jvm-jooq");
    }
}
