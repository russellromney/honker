package dev.honker;

@FunctionalInterface
public interface NotificationHandler {
    void handle(Notification notification) throws Exception;
}
