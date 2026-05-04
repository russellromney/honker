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
  PRAGMA watcher, plus explicit experimental `MMAP_SHM` and
  `KERNEL_EVENTS`
- explicit task registry helpers, typed queue/task wrappers, and
  `CompletableFuture` result waits
- Python-parity wake tests: subscribe-before-snapshot races,
  cross-process listener/worker/stream wake, delayed deadlines, concurrent
  claims, and listener churn/resource bounds

Optional integrations such as Spring Boot auto-configuration and
annotation scanning are intentionally kept outside the core runtime.

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

Typed helpers stay JSON-library neutral. Bring Jackson, Gson, JSON-B,
or your own mapper by implementing `JsonCodec<T>`:

```java
JsonCodec<Email> emailsJson = new JsonCodec<>() {
    public String encode(Email value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Email decode(String json) {
        try {
            return mapper.readValue(json, Email.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
};

TypedQueue<Email> emails = db.queue("emails").typed(emailsJson);
emails.enqueue(new Email("alice@example.com"));
TypedJob<Email> job = emails.claimOne("worker-1").orElseThrow();
sendEmail(job.payload());
job.ack();
```

Result waits can use `CompletableFuture`:

```java
queue.waitResultAsync(id, WaitOptions.timeout(Duration.ofSeconds(10)))
    .thenAccept(this::handleResult);
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
watcher. `MMAP_SHM` and `KERNEL_EVENTS` are explicitly experimental and
can be selected for tests/benchmarks. `KERNEL_EVENTS` uses Java's
directory watch service over the database, WAL, and SHM filenames; it
may emit spurious wakes and its latency is platform/JDK dependent, so
consumers still re-read SQLite state after every wake. `SQLITE_FCNTL_DATA_VERSION`
is intentionally not exposed as a backend; Honker's proof scripts show
it is not a correct idle cross-connection watcher.

## Local test

```bash
cargo build -p honker-extension
mvn -f packages/honker-jvm/pom.xml test
```
