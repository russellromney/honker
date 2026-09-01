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
- adds a `JobDetails<T>` data class over the JVM job fields, with Kotlin
  nullability and nullable `jobDetails(id)` lookups instead of `Optional`

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

## Job fields

`JobDetails<T>` is a Kotlin data class carrying every field the core
returns for a job row: `id`, `queue`, `payload`, `payloadJson`, `state`,
`priority`, `runAt`, `workerId`, `claimExpiresAt`, `attempts`,
`maxAttempts`, `createdAt`, `expiresAt`. Times are unix epoch seconds.
`workerId`, `claimExpiresAt` and `expiresAt` are Kotlin nullable, so the
compiler knows a pending job has no worker.

It is data only. Ack, retry, fail and heartbeat stay on the claimed
`Job`, because only the worker holding the claim may call them.

```kotlin
val emails = db.queue("emails")
val id = emails.enqueueJson("""{"to":"alice@example.com"}""")

val pending = emails.jobDetails(id)!!
println(pending.state)      // "pending"
println(pending.workerId)   // null

val claimed = emails.claimOne("worker-1").orElseThrow()
val job = claimed.details()
println("${job.state} ${job.attempts}/${job.maxAttempts} ${job.priority}")
claimed.ack()

emails.jobDetails(id)       // null
```

`jobDetails(id)` on a `Queue` only returns jobs from that queue; job ids
are globally unique, so an unscoped hit could hand back a payload that
does not match the queue's type. `Database.jobDetails(id)` is the global
lookup.

Pass a `JsonCodec<T>` — or use a `TypedQueue<T>` — to get the payload
decoded:

```kotlin
val typed = db.queue("emails").typed(strings)
val details: JobDetails<String>? = typed.jobDetails(id)
```

The payload type is a compile-time contract between the callers that
share a queue. Honker does not validate payload shape in the database.

## Local test

```bash
make test-kotlin
```
