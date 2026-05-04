package dev.honker;

public record TaskCall(String argsJson, String kwargsJson) {
    public static TaskCall empty() {
        return new TaskCall("[]", "{}");
    }
}
