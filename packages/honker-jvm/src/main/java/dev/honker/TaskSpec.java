package dev.honker;

public record TaskSpec(String name, String queue, TaskHandler handler, TaskOptions options) {
}
