package dev.honker;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class HonkerJvmChild {
    private HonkerJvmChild() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException("usage: <mode> <db> <extension> <ready> <done>");
        }
        String mode = args[0];
        Path dbPath = Path.of(args[1]);
        Path extensionPath = Path.of(args[2]);
        Path readyPath = Path.of(args[3]);
        Path donePath = Path.of(args[4]);
        if ("worker-marker".equals(mode)) {
            workerMarker(dbPath, extensionPath, readyPath, donePath);
        } else if ("listener-marker".equals(mode)) {
            listenerMarker(dbPath, extensionPath, readyPath, donePath, WatcherBackend.PRAGMA_DATA_VERSION);
        } else if ("listener-mmap-marker".equals(mode)) {
            listenerMarker(dbPath, extensionPath, readyPath, donePath, WatcherBackend.MMAP_SHM);
        } else if ("stream-marker".equals(mode)) {
            streamMarker(dbPath, extensionPath, readyPath, donePath);
        } else {
            throw new IllegalArgumentException("unknown child mode: " + mode);
        }
    }

    private static void workerMarker(Path dbPath, Path extensionPath, Path readyPath, Path donePath) throws Exception {
        CountDownLatch handled = new CountDownLatch(1);
        try (Database db = Honker.open(dbPath, OpenOptions.builder()
            .extensionPath(extensionPath)
            .fallbackPollInterval(Duration.ofSeconds(30))
            .build());
             WorkerHandle ignored = db.queue("multiprocess").worker(
                 "child",
                 job -> {
                     Files.writeString(donePath, job.payloadJson(), StandardCharsets.UTF_8);
                     handled.countDown();
                 },
                 WorkerOptions.builder().idlePollInterval(Duration.ofSeconds(30)).build()
             )) {
            Files.writeString(readyPath, "ready", StandardCharsets.UTF_8);
            if (!handled.await(10, TimeUnit.SECONDS)) {
                System.err.println("child worker timed out");
                System.exit(2);
            }
        }
    }

    private static void listenerMarker(Path dbPath, Path extensionPath, Path readyPath, Path donePath, WatcherBackend backend) throws Exception {
        try (Database db = Honker.open(dbPath, OpenOptions.builder()
            .extensionPath(extensionPath)
            .fallbackPollInterval(Duration.ofSeconds(30))
            .watcherOptions(WatcherOptions.builder().backend(backend).build())
            .build());
             Listener listener = db.listen("multiprocess-listen")) {
            Files.writeString(readyPath, "ready", StandardCharsets.UTF_8);
            Notification notification = listener.next(Duration.ofSeconds(10)).orElse(null);
            if (notification == null) {
                System.err.println("child listener timed out");
                System.exit(2);
            }
            Files.writeString(donePath, notification.payloadJson(), StandardCharsets.UTF_8);
        }
    }

    private static void streamMarker(Path dbPath, Path extensionPath, Path readyPath, Path donePath) throws Exception {
        CountDownLatch handled = new CountDownLatch(1);
        try (Database db = Honker.open(dbPath, OpenOptions.builder()
            .extensionPath(extensionPath)
            .fallbackPollInterval(Duration.ofSeconds(30))
            .build());
             StreamHandle ignored = db.stream("multiprocess-stream").subscribe(
                 event -> {
                     Files.writeString(donePath, event.payloadJson(), StandardCharsets.UTF_8);
                     handled.countDown();
                 },
                 SubscribeOptions.builder().pollTimeout(Duration.ofSeconds(30)).build()
             )) {
            Files.writeString(readyPath, "ready", StandardCharsets.UTF_8);
            if (!handled.await(10, TimeUnit.SECONDS)) {
                System.err.println("child stream subscriber timed out");
                System.exit(2);
            }
        }
    }
}
