package dev.honker.ormproof;

import dev.honker.HonkerExtension;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import org.hibernate.cfg.Configuration;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.sqlite.SQLiteConfig;

public final class OrmProof {

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

    static String jdbcUrl(String dbPath) {
        return "jdbc:sqlite:" + dbPath;
    }

    static void proveAtomicity(Connection conn, String queue, int commitId, int rollbackId) throws Exception {
        try (var stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, user_id INTEGER)");
        }
        conn.setAutoCommit(false);
        try (var ins = conn.prepareStatement("INSERT INTO orders (id, user_id) VALUES (?, ?)")) {
            ins.setInt(1, commitId);
            ins.setInt(2, 1);
            ins.executeUpdate();
        }
        long jobId;
        try (var enq = conn.prepareStatement(
                "SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)")) {
            enq.setString(1, queue);
            enq.setString(2, "{\"order_id\":" + commitId + "}");
            enq.setInt(3, 0);
            enq.setInt(4, 3);
            try (var rs = enq.executeQuery()) {
                rs.next();
                jobId = rs.getLong(1);
            }
        }
        conn.commit();
        conn.setAutoCommit(true);

        try (var check = conn.prepareStatement("SELECT COUNT(*) FROM orders WHERE id = ?")) {
            check.setInt(1, commitId);
            try (var rs = check.executeQuery()) {
                rs.next();
                if (rs.getInt(1) != 1) {
                    throw new IllegalStateException("missing committed order");
                }
            }
        }
        try (var job = conn.prepareStatement("SELECT honker_get_job(?)")) {
            job.setLong(1, jobId);
            try (var rs = job.executeQuery()) {
                rs.next();
                if (!rs.getString(1).contains("order_id")) {
                    throw new IllegalStateException("missing committed job");
                }
            }
        }

        conn.setAutoCommit(false);
        try (var ins = conn.prepareStatement("INSERT INTO orders (id, user_id) VALUES (?, ?)")) {
            ins.setInt(1, rollbackId);
            ins.setInt(2, 1);
            ins.executeUpdate();
        }
        long rolled;
        try (var enq = conn.prepareStatement(
                "SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)")) {
            enq.setString(1, queue);
            enq.setString(2, "{\"order_id\":" + rollbackId + "}");
            enq.setInt(3, 0);
            enq.setInt(4, 3);
            try (var rs = enq.executeQuery()) {
                rs.next();
                rolled = rs.getLong(1);
            }
        }
        conn.rollback();
        conn.setAutoCommit(true);

        try (var check = conn.prepareStatement("SELECT COUNT(*) FROM orders WHERE id = ?")) {
            check.setInt(1, rollbackId);
            try (var rs = check.executeQuery()) {
                rs.next();
                if (rs.getInt(1) != 0) {
                    throw new IllegalStateException("rollback left an order");
                }
            }
        }
        try (var job = conn.prepareStatement("SELECT honker_get_job(?)")) {
            job.setLong(1, rolled);
            try (var rs = job.executeQuery()) {
                rs.next();
                if (!rs.getString(1).isEmpty()) {
                    throw new IllegalStateException("rollback left a job");
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String dbPath = System.getenv("HONKER_TEST_DB");
        if (dbPath == null || dbPath.isBlank()) {
            throw new IllegalStateException("HONKER_TEST_DB is required");
        }

        try (var conn = DriverManager.getConnection(jdbcUrl(dbPath), extensionProps());
                var stmt = conn.createStatement()) {
            loadHonker(stmt);
            Surface.run(conn, "jdbc");
            proveAtomicity(conn, "jdbc_atomic", 42, 43);
        }
        System.out.println("PASS jvm-jdbc");

        var cfg = new Configuration()
                .setProperty("hibernate.connection.driver_class", "org.sqlite.JDBC")
                .setProperty("hibernate.connection.url", jdbcUrl(dbPath))
                .setProperty("hibernate.dialect", "org.hibernate.community.dialect.SQLiteDialect")
                .setProperty("hibernate.connection.enable_load_extension", "true")
                .setProperty("hibernate.hbm2ddl.auto", "none");
        try (var sf = cfg.buildSessionFactory(); var session = sf.openSession()) {
            session.beginTransaction();
            session.doWork(conn -> {
                try (var stmt = conn.createStatement()) {
                    loadHonker(stmt);
                    Surface.run(conn, "hib");
                    stmt.execute("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, user_id INTEGER)");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            session.getTransaction().commit();

            session.beginTransaction();
            final long[] committed = new long[1];
            session.doWork(conn -> {
                try (var ins = conn.prepareStatement("INSERT INTO orders (id, user_id) VALUES (?, ?)")) {
                    ins.setInt(1, 52);
                    ins.setInt(2, 1);
                    ins.executeUpdate();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                try (var enq = conn.prepareStatement(
                        "SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)")) {
                    enq.setString(1, "hib_atomic");
                    enq.setString(2, "{\"order_id\":52}");
                    enq.setInt(3, 0);
                    enq.setInt(4, 3);
                    try (var rs = enq.executeQuery()) {
                        rs.next();
                        committed[0] = rs.getLong(1);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            session.getTransaction().commit();

            session.beginTransaction();
            session.doWork(conn -> {
                try (var check = conn.prepareStatement("SELECT COUNT(*) FROM orders WHERE id = 52")) {
                    try (var rs = check.executeQuery()) {
                        rs.next();
                        if (rs.getInt(1) != 1) {
                            throw new IllegalStateException("missing committed order");
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            session.getTransaction().commit();

            session.beginTransaction();
            session.doWork(conn -> {
                try (var ins = conn.prepareStatement("INSERT INTO orders (id, user_id) VALUES (?, ?)")) {
                    ins.setInt(1, 53);
                    ins.setInt(2, 1);
                    ins.executeUpdate();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                try (var enq = conn.prepareStatement(
                        "SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)")) {
                    enq.setString(1, "hib_atomic");
                    enq.setString(2, "{\"order_id\":53}");
                    enq.setInt(3, 0);
                    enq.setInt(4, 3);
                    try (var rs = enq.executeQuery()) {
                        rs.next();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            session.getTransaction().rollback();

            session.beginTransaction();
            session.doWork(conn -> {
                try (var check = conn.prepareStatement("SELECT COUNT(*) FROM orders WHERE id = 53")) {
                    try (var rs = check.executeQuery()) {
                        rs.next();
                        if (rs.getInt(1) != 0) {
                            throw new IllegalStateException("rollback left an order");
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            session.getTransaction().commit();
        }
        System.out.println("PASS jvm-hibernate");

        try (var conn = DriverManager.getConnection(jdbcUrl(dbPath), extensionProps());
                var stmt = conn.createStatement()) {
            loadHonker(stmt);
            var ctx = DSL.using(conn, SQLDialect.SQLITE);
            Surface.run(conn, "jooq");
            ctx.transaction(configuration -> {
                var tx = DSL.using(configuration);
                tx.execute("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, user_id INTEGER)");
                tx.execute("INSERT INTO orders (id, user_id) VALUES (?, ?)", 62, 1);
                Object raw = tx.fetchValue(
                        "SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)",
                        "jooq_atomic",
                        "{\"order_id\":62}",
                        0,
                        3);
                Long id = ((Number) raw).longValue();
                if (id <= 0) {
                    throw new IllegalStateException("expected a job id");
                }
            });
            try {
                ctx.transaction(configuration -> {
                    var tx = DSL.using(configuration);
                    tx.execute("INSERT INTO orders (id, user_id) VALUES (?, ?)", 63, 1);
                    tx.fetchValue(
                            "SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)",
                            "jooq_atomic",
                            "{\"order_id\":63}",
                            0,
                            3);
                    throw new RuntimeException("rollback");
                });
            } catch (RuntimeException e) {
                boolean rollback = false;
                for (Throwable t = e; t != null; t = t.getCause()) {
                    if ("rollback".equals(t.getMessage())) {
                        rollback = true;
                        break;
                    }
                }
                if (!rollback) {
                    throw e;
                }
            }
            try (var check = conn.prepareStatement("SELECT COUNT(*) FROM orders WHERE id = 63")) {
                try (var rs = check.executeQuery()) {
                    rs.next();
                    if (rs.getInt(1) != 0) {
                        throw new IllegalStateException("rollback left an order");
                    }
                }
            }
        }
        System.out.println("PASS jvm-jooq");
    }
}
