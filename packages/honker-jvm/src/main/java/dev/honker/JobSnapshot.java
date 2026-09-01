package dev.honker;

import java.util.Map;

/**
 * A read-only view of one live job row, carrying every field the core
 * returns from {@code honker_claim_batch} and {@code honker_get_job}.
 *
 * <p>Data only. Ack, retry, fail and heartbeat live on a claimed
 * {@link Job}, because only the worker that holds the claim may call them.
 *
 * <p>Times are unix epoch seconds. Three components are {@code null} when
 * the row has no value for them: a pending job has no {@code workerId} and
 * no {@code claimExpiresAt}, and a job enqueued without {@code expires} has
 * no {@code expiresAt}.
 */
public record JobSnapshot(
    long id,
    String queue,
    String payloadJson,
    String state,
    int priority,
    long runAt,
    String workerId,
    Long claimExpiresAt,
    int attempts,
    int maxAttempts,
    long createdAt,
    Long expiresAt
) {
    /**
     * Decode {@link #payloadJson()} with {@code codec}.
     *
     * <p>The type is a compile-time contract between the callers that share
     * this queue. Honker never inspects or validates payload shape in the
     * database, so every writer must agree on the JSON it produces.
     */
    public <T> T payload(JsonCodec<T> codec) {
        return codec.decode(payloadJson);
    }

    static JobSnapshot from(Map<String, Object> row) {
        return new JobSnapshot(
            requiredLong(row, "id"),
            requiredString(row, "queue"),
            requiredString(row, "payload"),
            requiredString(row, "state"),
            Math.toIntExact(requiredLong(row, "priority")),
            requiredLong(row, "run_at"),
            nullableString(row.get("worker_id")),
            nullableLong(row, "claim_expires_at"),
            Math.toIntExact(requiredLong(row, "attempts")),
            Math.toIntExact(requiredLong(row, "max_attempts")),
            requiredLong(row, "created_at"),
            nullableLong(row, "expires_at")
        );
    }

    private static String requiredString(Map<String, Object> row, String field) {
        Object value = required(row, field);
        return value.toString();
    }

    private static long requiredLong(Map<String, Object> row, String field) {
        return toLong(required(row, field), field);
    }

    private static Object required(Map<String, Object> row, String field) {
        Object value = row.get(field);
        if (value == null) {
            throw new HonkerException("job row from Honker is missing required field " + field);
        }
        return value;
    }

    private static String nullableString(Object value) {
        return value == null ? null : value.toString();
    }

    private static Long nullableLong(Map<String, Object> row, String field) {
        Object value = row.get(field);
        return value == null ? null : toLong(value, field);
    }

    private static long toLong(Object value, String field) {
        if (value instanceof Number n) {
            return n.longValue();
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            throw new HonkerException("job row field " + field + " is not a number: " + value);
        }
    }
}
