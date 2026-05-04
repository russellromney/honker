package dev.honker;

public interface JsonCodec<T> {
    String encode(T value);

    T decode(String json);
}
