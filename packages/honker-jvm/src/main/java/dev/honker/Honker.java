package dev.honker;

import java.nio.file.Path;

public final class Honker {
    private Honker() {
    }

    public static Database open(String path) {
        return open(Path.of(path), OpenOptions.defaults());
    }

    public static Database open(String path, OpenOptions options) {
        return open(Path.of(path), options);
    }

    public static Database open(Path path) {
        return open(path, OpenOptions.defaults());
    }

    public static Database open(Path path, OpenOptions options) {
        return Database.open(path, options);
    }
}
