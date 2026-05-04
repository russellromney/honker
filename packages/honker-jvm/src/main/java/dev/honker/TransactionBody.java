package dev.honker;

@FunctionalInterface
public interface TransactionBody {
    void run(Transaction tx);
}
