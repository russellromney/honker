# honker-kotlin

Kotlin convenience wrapper for the Honker JVM binding.

Current shape:

- depends on `dev.honker:honker`
- does not duplicate database, native-loading, or SQL behavior
- adds Kotlin open helpers and option builders
- adds `Flow<Event>` over Java stream subscriptions
- adds `Flow<Notification>` and `Flow<Job>` wrappers
- adds coroutine `TaskResult.await(...)`
- adds codec-based typed queue/task helpers without forcing a JSON
  library dependency

## Quick start

```kotlin
honker("app.db").use { db ->
    val q = db.queue("emails")
    q.enqueue("""{"to":"alice@example.com"}""")
}
```

Flow wrapper:

```kotlin
db.stream("events")
    .asFlow()
    .collect { event -> println(event.payloadJson) }
```

Typed helpers:

```kotlin
val strings = object : JsonCodec<String> {
    override fun encode(value: String) = """"$value""""
    override fun decode(json: String) = json.trim('"')
}

db.queue("emails").enqueue("alice@example.com", strings)
val job = db.queue("emails").asFlow("worker").first()
println(job.decode(strings))
job.ack()
```

## Local test

```bash
make test-kotlin
```
