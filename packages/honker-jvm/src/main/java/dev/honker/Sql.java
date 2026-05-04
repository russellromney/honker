package dev.honker;

import java.sql.SQLException;

final class Sql {
    private Sql() {
    }

    static HonkerSqlException error(String message, SQLException cause) {
        return new HonkerSqlException(message + ": " + cause.getMessage(), cause);
    }
}
