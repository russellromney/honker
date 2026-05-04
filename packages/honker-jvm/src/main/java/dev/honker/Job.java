package dev.honker;

import java.time.Duration;
import java.util.Map;

public final class Job {
    private final Queue queueRef;
    private final long id;
    private final String queue;
    private final String payloadJson;
    private final String workerId;
    private final int attempts;
    private final long claimExpiresAt;

    private Job(Queue queueRef, long id, String queue, String payloadJson, String workerId, int attempts, long claimExpiresAt) {
        this.queueRef = queueRef;
        this.id = id;
        this.queue = queue;
        this.payloadJson = payloadJson;
        this.workerId = workerId;
        this.attempts = attempts;
        this.claimExpiresAt = claimExpiresAt;
    }

    static Job from(Queue queue, Map<String, Object> row) {
        return new Job(
            queue,
            number(row.get("id")),
            string(row.get("queue")),
            string(row.get("payload")),
            string(row.get("worker_id")),
            Math.toIntExact(number(row.get("attempts"))),
            number(row.get("claim_expires_at"))
        );
    }

    public long id() {
        return id;
    }

    public String queue() {
        return queue;
    }

    public String payloadJson() {
        return payloadJson;
    }

    public String workerId() {
        return workerId;
    }

    public int attempts() {
        return attempts;
    }

    public long claimExpiresAt() {
        return claimExpiresAt;
    }

    public boolean ack() {
        return queueRef.ack(id, workerId);
    }

    public boolean retry(Duration delay, String error) {
        return queueRef.retry(id, workerId, delay, error);
    }

    public boolean fail(String error) {
        return queueRef.fail(id, workerId, error);
    }

    public boolean heartbeat() {
        return queueRef.heartbeat(id, workerId);
    }

    public boolean heartbeat(Duration extendBy) {
        return queueRef.heartbeat(id, workerId, extendBy);
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
