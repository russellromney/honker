package dev.honker;

public enum WatcherBackend {
    AUTO,
    PRAGMA_DATA_VERSION,
    MMAP_SHM,
    KERNEL_EVENTS
}
