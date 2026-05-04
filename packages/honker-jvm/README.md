# honker-jvm

Early JVM binding for Honker.

Current shape:

- Java-compatible core package, intended for `dev.honker:honker`
- JDBC via `org.xerial:sqlite-jdbc`
- owned SQLite connection
- explicit path / `HONKER_EXTENSION_PATH` / packaged-native discovery
- JSON strings as the core payload and result type
- blocking queue, stream, notify/listen, scheduler, lock, outbox, and
  rate-limit APIs
- closeable queue worker, listener, stream subscriber, scheduler, outbox,
  and task-worker loops backed by one shared watcher per `Database`,
  plus deadline wakes
- watcher backends: `AUTO` / `PRAGMA_DATA_VERSION` for the stable
  PRAGMA watcher, plus explicit experimental `MMAP_SHM`
- explicit task registry helpers
- Python-parity wake tests: subscribe-before-snapshot races,
  cross-process listener/worker/stream wake, delayed deadlines, concurrent
  claims, and listener churn/resource bounds

Not in this first slice:

- external `Connection` / `DataSource` wrapping
- Java async/reactive APIs
- framework integrations
- annotation scanning / CLI

## Quick start

Build the extension first:

```bash
cargo build -p honker-extension
```

Then use the JVM binding:

```java
try (Database db = Honker.open("app.db")) {
    Queue queue = db.queue("emails");
    long id = queue.enqueue("{\"to\":\"alice@example.com\"}");

    Job job = queue.claimOne("worker-1").orElseThrow();
    sendEmail(job.payloadJson());
    job.ack();
}
```

For an explicit extension path:

```java
try (Database db = Honker.open("app.db", OpenOptions.builder()
    .extensionPath("./target/debug/libhonker_ext.dylib")
    .build())) {
    // ...
}
```

Watcher tuning stays boring by default, but can be made explicit:

```java
try (Database db = Honker.open("app.db", OpenOptions.builder()
    .watcherOptions(WatcherOptions.builder()
        .backend(WatcherBackend.PRAGMA_DATA_VERSION)
        .pollInterval(Duration.ofMillis(1))
        .build())
    .build())) {
    // ...
}
```

`AUTO` and `PRAGMA_DATA_VERSION` use the stable `PRAGMA data_version`
watcher. `MMAP_SHM` is explicitly experimental and can be selected for
tests/benchmarks. `SQLITE_FCNTL_DATA_VERSION` is intentionally not
exposed as a backend; Honker's proof scripts show it is not a correct
idle cross-connection watcher.

## Local test

```bash
cargo build -p honker-extension
mvn -f packages/honker-jvm/pom.xml test
```
