package dev.honker;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class TaskWorkerHandle implements AutoCloseable {
    private final List<WorkerHandle> workers = new ArrayList<>();

    TaskWorkerHandle(Database db, TaskRegistry registry, TaskWorkerOptions options) {
        for (String queueName : registry.queues()) {
            Queue queue = db.queue(queueName);
            workers.add(queue.worker(
                "honker-task-" + queueName,
                job -> dispatch(queue, registry, job),
                WorkerOptions.builder()
                    .concurrency(options.concurrency())
                    .idlePollInterval(options.idlePollInterval())
                    .defaultRetryDelay(java.time.Duration.ZERO)
                    .build()
            ));
        }
    }

    private void dispatch(Queue queue, TaskRegistry registry, Job job) throws Exception {
        String payload = job.payloadJson();
        Object envelopeRaw = Json.object(payload).get("__honker_task__");
        if (!(envelopeRaw instanceof java.util.Map<?, ?> envelope)) {
            job.fail("raw (non-task) payload on a task queue");
            return;
        }
        String taskName = String.valueOf(envelope.get("task"));
        TaskSpec spec = registry.get(taskName).orElse(null);
        if (spec == null) {
            job.fail("unknown task: " + taskName + ". Registered tasks: " + registry.names());
            return;
        }
        String args = envelope.get("args") == null ? "[]" : Json.stringify(envelope.get("args"));
        String kwargs = envelope.get("kwargs") == null ? "{}" : Json.stringify(envelope.get("kwargs"));
        try {
            String result = spec.handler().handle(new TaskCall(args, kwargs));
            if (spec.options().storeResult()) {
                queue.saveResult(job.id(), result == null ? "null" : result, ResultOptions.ttl(spec.options().resultTtl()));
            }
        } catch (RetryableException e) {
            job.retry(e.delay(), e.getMessage());
            return;
        } catch (Exception e) {
            job.retry(spec.options().retryDelay(), e.toString());
            return;
        }
        job.ack();
    }

    @Override
    public void close() {
        for (WorkerHandle worker : workers) {
            worker.close();
        }
    }

    public boolean failed() {
        return workers.stream().anyMatch(WorkerHandle::failed);
    }

    public Optional<Throwable> failure() {
        return workers.stream().flatMap(worker -> worker.failure().stream()).findFirst();
    }

    public void throwIfFailed() {
        for (WorkerHandle worker : workers) {
            worker.throwIfFailed();
        }
    }
}
