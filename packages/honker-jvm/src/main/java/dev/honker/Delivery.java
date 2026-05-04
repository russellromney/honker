package dev.honker;

@FunctionalInterface
public interface Delivery {
    void deliver(String payloadJson) throws Exception;
}
