package dev.honker;

public final class TypedTaskHandle<T> {
    private final TaskHandle raw;
    private final JsonCodec<T> resultCodec;

    TypedTaskHandle(TaskHandle raw, JsonCodec<T> resultCodec) {
        this.raw = raw;
        this.resultCodec = resultCodec;
    }

    public TaskHandle raw() {
        return raw;
    }

    public String name() {
        return raw.name();
    }

    public TypedTaskResult<T> enqueue(String argsJson, String kwargsJson) {
        return new TypedTaskResult<>(raw.enqueue(argsJson, kwargsJson), resultCodec);
    }

    public TypedTaskResult<T> enqueue(TaskCall call) {
        return new TypedTaskResult<>(raw.enqueue(call), resultCodec);
    }
}
