package dev.honker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public final class Queue {
    private final Database db;
    private final String name;
    private final QueueOptions options;

    Queue(Database db, String name, QueueOptions options) {
        if (name == null || name.isBlank()) {
            throw new HonkerInvalidOptionException("queue name must not be blank");
        }
        this.db = db;
        this.name = name;
        this.options = options;
    }

    public String name() {
        return name;
    }

    public long enqueue(String payloadJson) {
        return enqueue(payloadJson, EnqueueOptions.defaults());
    }

    public long enqueue(String payloadJson, EnqueueOptions enqueueOptions) {
        return db.transaction(tx -> enqueue(tx, payloadJson, enqueueOptions));
    }

    public long enqueue(Transaction tx, String payloadJson) {
        return enqueue(tx, payloadJson, EnqueueOptions.defaults());
    }

    public long enqueue(Transaction tx, String payloadJson, EnqueueOptions enqueueOptions) {
        Long runAt = enqueueOptions.runAt() == null ? null : enqueueOptions.runAt().getEpochSecond();
        Long delay = enqueueOptions.delay() == null ? null : Durations.seconds(enqueueOptions.delay(), "delay");
        Long expires = enqueueOptions.expires() == null ? null : Durations.seconds(enqueueOptions.expires(), "expires");
        return tx.query(
            "SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?) AS id",
            Params.of(name, payloadJson, runAt, delay, enqueueOptions.priority(), options.maxAttempts(), expires)
        ).get(0).getLong("id");
    }

    public Optional<Job> claimOne(String workerId) {
        List<Job> jobs = claimBatch(workerId, 1);
        return jobs.isEmpty() ? Optional.empty() : Optional.of(jobs.get(0));
    }

    public List<Job> claimBatch(String workerId, int n) {
        if (n <= 0) {
            return List.of();
        }
        String rowsJson = db.transaction(tx -> tx.query(
            "SELECT honker_claim_batch(?, ?, ?, ?) AS rows_json",
            Params.of(name, workerId, n, Durations.seconds(options.visibilityTimeout(), "visibilityTimeout"))
        ).get(0).getString("rows_json"));
        List<Job> out = new ArrayList<>();
        for (Map<String, Object> row : Json.objectArray(rowsJson)) {
            out.add(Job.from(this, row));
        }
        return out;
    }

    public long nextClaimAt() {
        return db.transaction(tx -> tx.query(
            "SELECT honker_queue_next_claim_at(?) AS t",
            Params.of(name)
        ).get(0).getLong("t"));
    }

    public Database database() {
        return db;
    }

    public int ackBatch(List<Long> jobIds, String workerId) {
        if (jobIds == null || jobIds.isEmpty()) {
            return 0;
        }
        return db.transaction(tx -> tx.query(
            "SELECT honker_ack_batch(?, ?) AS n",
            Params.of(Json.idArray(jobIds), workerId)
        ).get(0).getInt("n"));
    }

    public int sweepExpired() {
        return db.transaction(tx -> tx.query(
            "SELECT honker_sweep_expired(?) AS n",
            Params.of(name)
        ).get(0).getInt("n"));
    }

    public void saveResult(long jobId, String valueJson) {
        saveResult(jobId, valueJson, ResultOptions.neverExpires());
    }

    public void saveResult(long jobId, String valueJson, ResultOptions resultOptions) {
        db.transactionVoid(tx -> saveResult(tx, jobId, valueJson, resultOptions));
    }

    public void saveResult(Transaction tx, long jobId, String valueJson, ResultOptions resultOptions) {
        long ttl = resultOptions.ttl() == null ? 0 : Durations.seconds(resultOptions.ttl(), "result ttl");
        tx.query("SELECT honker_result_save(?, ?, ?)", Params.of(jobId, valueJson, ttl));
    }

    public Optional<String> getResult(long jobId) {
        String raw = db.transaction(tx -> tx.query(
            "SELECT honker_result_get(?) AS v",
            Params.of(jobId)
        ).get(0).getString("v"));
        return Optional.ofNullable(raw);
    }

    public String waitResult(long jobId, WaitOptions waitOptions) {
        long deadline = deadlineNanos(waitOptions.timeout());
        try (UpdateEvents updates = db.updateEvents()) {
            while (true) {
                Optional<String> found = getResult(jobId);
                if (found.isPresent()) {
                    return found.get();
                }
                Duration wait = remaining(deadline, waitOptions.fallbackPollInterval());
                if (!updates.awaitUpdate(wait) && deadlineExpired(deadline)) {
                    throw new HonkerTimeoutException("waitResult(" + jobId + ") timed out");
                }
            }
        }
    }

    public CompletableFuture<String> waitResultAsync(long jobId, WaitOptions waitOptions) {
        return waitResultAsync(jobId, waitOptions, db.executor());
    }

    public CompletableFuture<String> waitResultAsync(long jobId, WaitOptions waitOptions, Executor executor) {
        if (executor == null) {
            return CompletableFuture.supplyAsync(() -> waitResult(jobId, waitOptions));
        }
        return CompletableFuture.supplyAsync(() -> waitResult(jobId, waitOptions), executor);
    }

    public <T> TypedQueue<T> typed(JsonCodec<T> codec) {
        return new TypedQueue<>(this, codec);
    }

    public int sweepResults() {
        return db.transaction(tx -> tx.query("SELECT honker_result_sweep() AS n").get(0).getInt("n"));
    }

    public boolean ack(long jobId, String workerId) {
        return db.transaction(tx -> tx.query(
            "SELECT honker_ack(?, ?) AS r",
            Params.of(jobId, workerId)
        ).get(0).getBoolean("r"));
    }

    public boolean retry(long jobId, String workerId, Duration delay, String error) {
        return db.transaction(tx -> tx.query(
            "SELECT honker_retry(?, ?, ?, ?) AS r",
            Params.of(jobId, workerId, Durations.seconds(delay, "retry delay"), error)
        ).get(0).getBoolean("r"));
    }

    public boolean fail(long jobId, String workerId, String error) {
        return db.transaction(tx -> tx.query(
            "SELECT honker_fail(?, ?, ?) AS r",
            Params.of(jobId, workerId, error)
        ).get(0).getBoolean("r"));
    }

    public boolean heartbeat(long jobId, String workerId) {
        return heartbeat(jobId, workerId, options.visibilityTimeout());
    }

    public boolean heartbeat(long jobId, String workerId, Duration extendBy) {
        return db.transaction(tx -> tx.query(
            "SELECT honker_heartbeat(?, ?, ?) AS r",
            Params.of(jobId, workerId, Durations.seconds(extendBy, "extendBy"))
        ).get(0).getBoolean("r"));
    }

    public WorkerHandle worker(String workerId, JobHandler handler) {
        return worker(workerId, handler, WorkerOptions.defaults());
    }

    public WorkerHandle worker(String workerId, JobHandler handler, WorkerOptions options) {
        return new WorkerHandle(db, this, workerId, handler, options);
    }

    private static long deadlineNanos(Duration timeout) {
        return timeout == null ? 0L : System.nanoTime() + timeout.toNanos();
    }

    private static boolean deadlineExpired(long deadline) {
        return deadline != 0L && System.nanoTime() >= deadline;
    }

    private static Duration remaining(long deadline, Duration fallback) {
        if (deadline == 0L) {
            return fallback;
        }
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0L) {
            return Duration.ZERO;
        }
        Duration d = Duration.ofNanos(nanos);
        return d.compareTo(fallback) < 0 ? d : fallback;
    }
}
