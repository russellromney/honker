package dev.honker;

public final class Outbox {
    private final Queue queue;
    private final Delivery delivery;
    private final OutboxOptions options;

    Outbox(Database db, String name, Delivery delivery, OutboxOptions options) {
        if (delivery == null) {
            throw new HonkerInvalidOptionException("delivery must not be null");
        }
        this.queue = db.queue(
            "_outbox:" + name,
            QueueOptions.builder()
                .maxAttempts(options.maxAttempts())
                .visibilityTimeout(options.visibilityTimeout())
                .build()
        );
        this.delivery = delivery;
        this.options = options;
    }

    public Queue queue() {
        return queue;
    }

    public void enqueue(String payloadJson) {
        queue.enqueue(payloadJson);
    }

    public void enqueue(String payloadJson, EnqueueOptions options) {
        queue.enqueue(payloadJson, options);
    }

    public void enqueue(Transaction tx, String payloadJson) {
        queue.enqueue(tx, payloadJson);
    }

    public void enqueue(Transaction tx, String payloadJson, EnqueueOptions options) {
        queue.enqueue(tx, payloadJson, options);
    }

    public boolean deliverOne(String workerId) {
        var job = queue.claimOne(workerId);
        if (job.isEmpty()) {
            return false;
        }
        Job j = job.get();
        try {
            delivery.deliver(j.payloadJson());
            j.ack();
        } catch (Exception e) {
            j.retry(backoffDelay(j), e.toString());
        }
        return true;
    }

    public OutboxHandle runWorker(String workerId) {
        return runWorker(workerId, WorkerOptions.defaults());
    }

    public OutboxHandle runWorker(String workerId, WorkerOptions workerOptions) {
        WorkerHandle worker = queue.worker(workerId, job -> {
            try {
                delivery.deliver(job.payloadJson());
            } catch (Exception e) {
                throw new RetryableException(e.toString(), backoffDelay(job));
            }
        }, workerOptions);
        return new OutboxHandle(worker);
    }

    private java.time.Duration backoffDelay(Job job) {
        long multiplier = 1L << Math.max(0, job.attempts() - 1);
        return options.baseBackoff().multipliedBy(multiplier);
    }
}
