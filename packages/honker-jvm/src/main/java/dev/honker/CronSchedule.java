package dev.honker;

import org.sqlite.SQLiteConfig;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

public final class CronSchedule {
    private final String expression;

    private CronSchedule(String expression) {
        this.expression = expression;
        nextAfter(Instant.EPOCH);
    }

    public static CronSchedule crontab(String expression) {
        if (expression == null || expression.trim().startsWith("@every")) {
            throw new HonkerInvalidOptionException("use every(Duration) for interval schedules");
        }
        return new CronSchedule(expression);
    }

    public static CronSchedule every(Duration interval) {
        if (interval == null || interval.isZero() || interval.isNegative() || interval.getNano() != 0) {
            throw new HonkerInvalidOptionException("every interval must be a positive whole-second duration");
        }
        return new CronSchedule("@every " + interval.toSeconds() + "s");
    }

    public Instant nextAfter(Instant instant) {
        Path ext = NativeLoader.resolve(OpenOptions.defaults());
        SQLiteConfig config = new SQLiteConfig();
        config.enableLoadExtension(true);
        try (Connection c = DriverManager.getConnection("jdbc:sqlite::memory:", config.toProperties())) {
            try (PreparedStatement load = c.prepareStatement("SELECT load_extension(?, ?)")) {
                load.setString(1, ext.toString());
                load.setString(2, NativeLoader.ENTRYPOINT);
                load.executeQuery().close();
            }
            try (PreparedStatement stmt = c.prepareStatement("SELECT honker_cron_next_after(?, ?) AS t")) {
                stmt.setString(1, expression);
                stmt.setLong(2, instant.getEpochSecond());
                try (ResultSet rs = stmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new HonkerException("cron parser returned no result");
                    }
                    return Instant.ofEpochSecond(rs.getLong("t"));
                }
            }
        } catch (SQLException e) {
            throw Sql.error("cron next_after failed", e);
        }
    }

    public String expression() {
        return expression;
    }
}
