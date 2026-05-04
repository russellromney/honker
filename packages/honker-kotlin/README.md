# honker-kotlin

Kotlin convenience wrapper for the Honker JVM binding.

Current shape:

- depends on `dev.honker:honker`
- does not duplicate database, native-loading, or SQL behavior
- adds Kotlin open helpers and option builders
- adds `Flow<Event>` over Java stream subscriptions
- adds coroutine `TaskResult.await(...)`

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

## Local test

```bash
make test-kotlin
```
