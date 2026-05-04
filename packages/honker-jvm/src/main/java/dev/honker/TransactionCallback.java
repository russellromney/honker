package dev.honker;

@FunctionalInterface
public interface TransactionCallback<T> {
    T run(Transaction tx);
}
