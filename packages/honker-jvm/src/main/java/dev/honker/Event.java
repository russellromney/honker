package dev.honker;

public record Event(long offset, String topic, String key, String payloadJson, long createdAt) {
}
