package dev.honker.ormproof;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;

final class Surface {
    private Surface() {}

    static Path catalogPath() {
        String env = System.getenv("HONKER_ORM_SURFACE");
        if (env != null && !env.isBlank()) {
            Path p = Path.of(env);
            if (!Files.isRegularFile(p)) {
                throw new IllegalStateException("HONKER_ORM_SURFACE=" + env + " is not a file");
            }
            return p;
        }
        Path cwd = Path.of("").toAbsolutePath();
        Path[] candidates = new Path[] {
            cwd.resolve("surface.json"),
            cwd.resolve("../surface.json"),
            cwd.resolve("scripts/proof/orm/surface.json"),
        };
        for (Path p : candidates) {
            if (Files.isRegularFile(p)) {
                return p;
            }
        }
        throw new IllegalStateException("surface.json not found; set HONKER_ORM_SURFACE");
    }

    static long asInt(Object value) {
        if (value instanceof Number n) {
            return n.longValue();
        }
        if (value instanceof String s) {
            return Long.parseLong(s);
        }
        throw new IllegalStateException("expected int, got " + value);
    }

    static String asText(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    static Object resolve(Object token, String prefix, Map<String, Object> vars) {
        if (!(token instanceof String s)) {
            if (token == JSONObject.NULL) {
                return null;
            }
            return token;
        }
        if (s.startsWith("$ns:")) {
            return prefix + "_" + s.substring(4);
        }
        if (s.startsWith("$json:")) {
            JSONArray ids = new JSONArray();
            for (String key : s.substring(6).split(",")) {
                ids.put(asInt(vars.get(key)));
            }
            return ids.toString();
        }
        if (s.startsWith("$")) {
            return vars.get(s.substring(1));
        }
        return s;
    }

    static String resolveText(String text, String prefix, Map<String, Object> vars) {
        String out = text;
        for (Map.Entry<String, Object> e : vars.entrySet()) {
            out = out.replace("$" + e.getKey(), asText(e.getValue()));
        }
        return out.replace("$ns:", prefix + "_");
    }

    static Object scalar(Connection conn, String sql, List<Object> args) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < args.size(); i++) {
                Object arg = args.get(i);
                if (arg == null) {
                    stmt.setNull(i + 1, Types.NULL);
                } else if (arg instanceof Integer n) {
                    stmt.setInt(i + 1, n);
                } else if (arg instanceof Long n) {
                    stmt.setLong(i + 1, n);
                } else if (arg instanceof Number n) {
                    stmt.setLong(i + 1, n.longValue());
                } else {
                    stmt.setString(i + 1, String.valueOf(arg));
                }
            }
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                Object value = rs.getObject(1);
                return rs.wasNull() ? null : value;
            }
        }
    }

    static void check(JSONObject expect, Object result, String prefix, Map<String, Object> vars) {
        String kind = expect.getString("kind");
        switch (kind) {
            case "int_gt" -> {
                if (!(asInt(result) > expect.getLong("n"))) {
                    throw new IllegalStateException("got " + result);
                }
            }
            case "int_eq" -> {
                if (asInt(result) != expect.getLong("n")) {
                    throw new IllegalStateException("got " + result);
                }
            }
            case "int_ge" -> {
                if (asInt(result) < expect.getLong("n")) {
                    throw new IllegalStateException("got " + result);
                }
            }
            case "int_gt_ref" -> {
                if (!(asInt(result) > asInt(vars.get(expect.getString("ref"))))) {
                    throw new IllegalStateException("got " + result);
                }
            }
            case "eq_ref" -> {
                if (asInt(result) != asInt(vars.get(expect.getString("ref")))) {
                    throw new IllegalStateException("got " + result);
                }
            }
            case "json_len" -> {
                if (new JSONArray(asText(result)).length() != expect.getInt("n")) {
                    throw new IllegalStateException("got " + result);
                }
            }
            case "json_id_eq_ref" -> {
                JSONArray parsed = new JSONArray(asText(result));
                if (parsed.length() != 1
                        || asInt(parsed.getJSONObject(0).get("id")) != asInt(vars.get(expect.getString("ref")))) {
                    throw new IllegalStateException("got " + result);
                }
            }
            case "contains" -> {
                String needle = resolveText(expect.getString("s"), prefix, vars);
                if (!asText(result).contains(needle)) {
                    throw new IllegalStateException(needle + " not in " + result);
                }
            }
            case "empty_string" -> {
                if (!asText(result).isEmpty()) {
                    throw new IllegalStateException("expected empty string, got " + result);
                }
            }
            case "is_null" -> {
                if (result != null) {
                    throw new IllegalStateException("expected NULL, got " + result);
                }
            }
            default -> throw new IllegalStateException("unknown expect kind " + kind);
        }
    }

    static void run(Connection conn, String prefix) throws Exception {
        JSONObject catalog = new JSONObject(Files.readString(catalogPath()));
        Map<String, Object> vars = new HashMap<>();
        JSONArray steps = catalog.getJSONArray("steps");
        for (int i = 0; i < steps.length(); i++) {
            JSONObject step = steps.getJSONObject(i);
            String id = step.getString("id");
            JSONArray rawArgs = step.getJSONArray("args");
            List<Object> args = new ArrayList<>();
            for (int a = 0; a < rawArgs.length(); a++) {
                args.add(resolve(rawArgs.get(a), prefix, vars));
            }
            Object result;
            try {
                result = scalar(conn, step.getString("sql"), args);
            } catch (Exception e) {
                throw new IllegalStateException(id + " failed: " + e.getMessage(), e);
            }
            if (step.has("store") && !step.isNull("store")) {
                vars.put(step.getString("store"), result);
            }
            if (step.has("expect")) {
                try {
                    check(step.getJSONObject("expect"), result, prefix, vars);
                } catch (Exception e) {
                    throw new IllegalStateException(id + ": " + e.getMessage(), e);
                }
            }
        }
    }
}
