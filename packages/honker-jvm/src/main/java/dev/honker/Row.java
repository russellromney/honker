package dev.honker;

import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public final class Row {
    private final Map<String, Object> values;

    Row(Map<String, Object> values) {
        this.values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }

    public Object get(String column) {
        return values.get(column);
    }

    public String getString(String column) {
        Object value = require(column);
        return value == null ? null : value.toString();
    }

    public long getLong(String column) {
        Object value = require(column);
        if (value instanceof Number n) {
            return n.longValue();
        }
        return Long.parseLong(value.toString());
    }

    public int getInt(String column) {
        return Math.toIntExact(getLong(column));
    }

    public boolean getBoolean(String column) {
        Object value = require(column);
        if (value instanceof Boolean b) {
            return b;
        }
        if (value instanceof Number n) {
            return n.longValue() != 0L;
        }
        return Boolean.parseBoolean(value.toString());
    }

    public boolean isNull(String column) {
        return values.containsKey(column) && values.get(column) == null;
    }

    public Map<String, Object> asMap() {
        return values;
    }

    private Object require(String column) {
        if (!values.containsKey(column)) {
            throw new HonkerInvalidOptionException("row has no column " + Objects.requireNonNull(column));
        }
        return values.get(column);
    }

    static Row mutable(Map<String, Object> values) {
        return new Row(new LinkedHashMap<>(values));
    }
}
