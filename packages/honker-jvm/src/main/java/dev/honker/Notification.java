package dev.honker;

public record Notification(long id, String channel, String payloadJson, long createdAt) {
}
