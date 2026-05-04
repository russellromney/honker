package dev.honker;

public final class TaskHandle {
    private final Queue queue;
    private final TaskSpec spec;

    TaskHandle(Queue queue, TaskSpec spec) {
        this.queue = queue;
        this.spec = spec;
    }

    public TaskResult enqueue(String argsJson, String kwargsJson) {
        return enqueue(new TaskCall(argsJson, kwargsJson));
    }

    public TaskResult enqueue(TaskCall call) {
        EnqueueOptions options = EnqueueOptions.builder()
            .priority(spec.options().priority())
            .expires(spec.options().expires())
            .build();
        long id = queue.enqueue(Json.taskEnvelope(spec.name(), call.argsJson(), call.kwargsJson()), options);
        return new TaskResult(queue, id);
    }

    public String name() {
        return spec.name();
    }
}
