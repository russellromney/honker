package dev.honker;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class Json {
    private Json() {
    }

    static List<Map<String, Object>> objectArray(String json) {
        Object value = new Parser(json).parse();
        if (!(value instanceof List<?> list)) {
            throw new HonkerSqlException("expected JSON array", null);
        }
        List<Map<String, Object>> out = new ArrayList<>();
        for (Object item : list) {
            if (!(item instanceof Map<?, ?> raw)) {
                throw new HonkerSqlException("expected JSON object array", null);
            }
            Map<String, Object> map = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : raw.entrySet()) {
                map.put(String.valueOf(e.getKey()), e.getValue());
            }
            out.add(map);
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> object(String json) {
        Object value = new Parser(json).parse();
        if (!(value instanceof Map<?, ?> raw)) {
            throw new HonkerException("expected JSON object");
        }
        Map<String, Object> map = new LinkedHashMap<>();
        for (Map.Entry<?, ?> e : raw.entrySet()) {
            map.put(String.valueOf(e.getKey()), e.getValue());
        }
        return map;
    }

    static String taskEnvelope(String taskName, String argsJson, String kwargsJson) {
        return "{\"__honker_task__\":{\"task\":"
            + quote(taskName)
            + ",\"args\":"
            + (argsJson == null || argsJson.isBlank() ? "[]" : argsJson)
            + ",\"kwargs\":"
            + (kwargsJson == null || kwargsJson.isBlank() ? "{}" : kwargsJson)
            + "}}";
    }

    @SuppressWarnings("unchecked")
    static String stringify(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String s) {
            return quote(s);
        }
        if (value instanceof Double d && d.doubleValue() == Math.rint(d.doubleValue())) {
            return Long.toString(d.longValue());
        }
        if (value instanceof Float f && f.floatValue() == Math.rint(f.floatValue())) {
            return Long.toString(f.longValue());
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof List<?> list) {
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(stringify(list.get(i)));
            }
            return sb.append(']').toString();
        }
        if (value instanceof Map<?, ?> map) {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<?, ?> e : map.entrySet()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                sb.append(quote(String.valueOf(e.getKey()))).append(':').append(stringify(e.getValue()));
            }
            return sb.append('}').toString();
        }
        return quote(value.toString());
    }

    static String idArray(List<Long> ids) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(ids.get(i));
        }
        return sb.append(']').toString();
    }

    static String quote(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    private static final class Parser {
        private final String s;
        private int i;

        Parser(String s) {
            this.s = s == null ? "" : s;
        }

        Object parse() {
            Object v = value();
            ws();
            if (i != s.length()) {
                fail("trailing content");
            }
            return v;
        }

        private Object value() {
            ws();
            if (i >= s.length()) {
                fail("unexpected end");
            }
            char c = s.charAt(i);
            return switch (c) {
                case '"' -> string();
                case '{' -> object();
                case '[' -> array();
                case 'n' -> literal("null", null);
                case 't' -> literal("true", Boolean.TRUE);
                case 'f' -> literal("false", Boolean.FALSE);
                default -> number();
            };
        }

        private Map<String, Object> object() {
            expect('{');
            Map<String, Object> out = new LinkedHashMap<>();
            ws();
            if (peek('}')) {
                i++;
                return out;
            }
            while (true) {
                String k = string();
                ws();
                expect(':');
                out.put(k, value());
                ws();
                if (peek('}')) {
                    i++;
                    return out;
                }
                expect(',');
            }
        }

        private List<Object> array() {
            expect('[');
            List<Object> out = new ArrayList<>();
            ws();
            if (peek(']')) {
                i++;
                return out;
            }
            while (true) {
                out.add(value());
                ws();
                if (peek(']')) {
                    i++;
                    return out;
                }
                expect(',');
            }
        }

        private String string() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (i < s.length()) {
                char c = s.charAt(i++);
                if (c == '"') {
                    return sb.toString();
                }
                if (c != '\\') {
                    sb.append(c);
                    continue;
                }
                if (i >= s.length()) {
                    fail("bad escape");
                }
                char e = s.charAt(i++);
                switch (e) {
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case '/' -> sb.append('/');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'u' -> {
                        if (i + 4 > s.length()) {
                            fail("bad unicode escape");
                        }
                        sb.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                        i += 4;
                    }
                    default -> fail("bad escape");
                }
            }
            fail("unterminated string");
            return "";
        }

        private Object number() {
            int start = i;
            if (peek('-')) {
                i++;
            }
            while (i < s.length() && Character.isDigit(s.charAt(i))) {
                i++;
            }
            boolean real = false;
            if (peek('.')) {
                real = true;
                i++;
                while (i < s.length() && Character.isDigit(s.charAt(i))) {
                    i++;
                }
            }
            if (peek('e') || peek('E')) {
                real = true;
                i++;
                if (peek('+') || peek('-')) {
                    i++;
                }
                while (i < s.length() && Character.isDigit(s.charAt(i))) {
                    i++;
                }
            }
            if (start == i) {
                fail("expected number");
            }
            String n = s.substring(start, i);
            return real ? Double.parseDouble(n) : Long.parseLong(n);
        }

        private Object literal(String lit, Object value) {
            if (!s.startsWith(lit, i)) {
                fail("expected " + lit);
            }
            i += lit.length();
            return value;
        }

        private void ws() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
                i++;
            }
        }

        private void expect(char c) {
            ws();
            if (i >= s.length() || s.charAt(i) != c) {
                fail("expected " + c);
            }
            i++;
        }

        private boolean peek(char c) {
            return i < s.length() && s.charAt(i) == c;
        }

        private void fail(String msg) {
            throw new HonkerException("invalid JSON from Honker: " + msg + " at byte " + i);
        }
    }
}
