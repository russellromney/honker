package dev.honker;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class TaskRegistry {
    private final Database db;
    private final Map<String, TaskSpec> byName = new ConcurrentHashMap<>();

    public TaskRegistry(Database db) {
        this.db = db;
    }

    public TaskHandle register(String name, String queue, TaskHandler handler) {
        return register(name, queue, handler, TaskOptions.defaults());
    }

    public TaskHandle register(String name, String queue, TaskHandler handler, TaskOptions options) {
        TaskSpec spec = new TaskSpec(name, queue, handler, options);
        TaskSpec existing = byName.putIfAbsent(name, spec);
        if (existing != null && existing.handler() != handler) {
            throw new HonkerInvalidOptionException("duplicate task name " + name);
        }
        return new TaskHandle(db.queue(queue, QueueOptions.builder().maxAttempts(options.retries()).build()), spec);
    }

    public Optional<TaskSpec> get(String name) {
        return Optional.ofNullable(byName.get(name));
    }

    public java.util.List<String> names() {
        return new ArrayList<>(byName.keySet().stream().sorted().toList());
    }

    public Set<String> queues() {
        Set<String> out = new LinkedHashSet<>();
        for (TaskSpec spec : byName.values()) {
            out.add(spec.queue());
        }
        return out;
    }
}
