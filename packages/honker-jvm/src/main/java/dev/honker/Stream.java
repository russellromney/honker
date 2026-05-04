package dev.honker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class Stream {
    private final Database db;
    private final String name;

    Stream(Database db, String name) {
        this.db = db;
        this.name = name;
    }

    public void publish(String payloadJson) {
        publish(payloadJson, null);
    }

    public void publish(String payloadJson, String key) {
        db.transactionVoid(tx -> publish(tx, payloadJson, key));
    }

    public void publish(Transaction tx, String payloadJson) {
        publish(tx, payloadJson, null);
    }

    public void publish(Transaction tx, String payloadJson, String key) {
        tx.query("SELECT honker_stream_publish(?, ?, ?)", Params.of(name, key, payloadJson));
    }

    public List<Event> readSince(long offset, int limit) {
        String rowsJson = db.transaction(tx -> tx.query(
            "SELECT honker_stream_read_since(?, ?, ?) AS rows_json",
            Params.of(name, offset, limit)
        ).get(0).getString("rows_json"));
        List<Event> out = new ArrayList<>();
        for (Map<String, Object> row : Json.objectArray(rowsJson)) {
            out.add(new Event(
                number(row.get("offset")),
                string(row.get("topic")),
                string(row.get("key")),
                string(row.get("payload")),
                number(row.get("created_at"))
            ));
        }
        return out;
    }

    public void saveOffset(String consumer, long offset) {
        db.transactionVoid(tx -> tx.query(
            "SELECT honker_stream_save_offset(?, ?, ?)",
            Params.of(consumer, name, offset)
        ));
    }

    public long getOffset(String consumer) {
        return db.transaction(tx -> tx.query(
            "SELECT honker_stream_get_offset(?, ?) AS v",
            Params.of(consumer, name)
        ).get(0).getLong("v"));
    }

    public StreamHandle subscribe(StreamHandler handler) {
        return subscribe(handler, SubscribeOptions.defaults());
    }

    public StreamHandle subscribe(StreamHandler handler, SubscribeOptions options) {
        return new StreamHandle(db, this, handler, options);
    }

    private static String string(Object value) {
        return value == null ? null : value.toString();
    }

    private static long number(Object value) {
        if (value instanceof Number n) {
            return n.longValue();
        }
        return Long.parseLong(value.toString());
    }
}
