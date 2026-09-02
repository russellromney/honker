package dev.honker.kotlin

import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.Timeout.ThreadMode
import org.junit.jupiter.api.io.TempDir
import dev.honker.Database
import dev.honker.HonkerInvalidOptionException
import dev.honker.JsonCodec
import dev.honker.WatcherBackend
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.assertFailsWith

// The whole suite runs in well under a second. The cap exists so a flow
// wrapper that stops producing fails by name instead of parking the
// runner until the CI job's own 20-minute timeout kills it.
@Timeout(value = 60, threadMode = ThreadMode.SEPARATE_THREAD)
class HonkerKotlinTest {
    @TempDir
    lateinit var tmp: Path

    private val stringCodec = object : JsonCodec<String> {
        override fun encode(value: String): String = """"$value""""

        override fun decode(json: String): String = json.trim('"')
    }

    @Test
    fun flowWrapperUsesJavaStreamRuntime() = runBlocking {
        honker(tmp.resolve("app.db")) {
            fallbackPollInterval(Duration.ofMillis(2))
        }.use { db ->
            val stream = db.stream("events")
            val flow = stream.asFlow(subscribeOptions {
                pollTimeout(Duration.ofMillis(50))
            })
            stream.publish("""{"hello":"kotlin"}""")
            assertEquals("""{"hello":"kotlin"}""", flow.first().payloadJson)
        }
    }

    @Test
    fun listenerAndQueueFlowWrappersUseJavaRuntime() = runBlocking {
        honker(tmp.resolve("flows.db")) {
            fallbackPollInterval(Duration.ofMillis(2))
        }.use { db ->
            val listener = db.listen("orders")
            val notificationDeferred = async { listener.asFlow(Duration.ofMillis(50)).first() }
            db.notify("orders", """{"id":1}""")
            val notification = notificationDeferred.await()
            assertEquals("""{"id":1}""", notification.payloadJson)

            val queue = db.queue("flow-work")
            val jobs = queue.asFlow("worker", workerOptions {
                idlePollInterval(Duration.ofMillis(20))
            })
            val jobDeferred = async { jobs.first() }
            queue.enqueueJson("""{"work":true}""")
            val job = jobDeferred.await()
            assertEquals("""{"work":true}""", job.payloadJson())
            assertTrue(job.ack())
        }
    }

    @Test
    fun taskResultAwaitWrapsJavaResult() = runBlocking {
        honker(tmp.resolve("task.db")) {
            fallbackPollInterval(Duration.ofMillis(2))
        }.use { db ->
            val registry = db.taskRegistry()
            val task = registry.registerJson("hello", "tasks", taskOptions {
                resultTtl(Duration.ofSeconds(60))
            }) { """"world"""" }
            val result = task.enqueueJson()
            db.runTasks(registry, taskWorkerOptions {
                concurrency(1)
                idlePollInterval(Duration.ofMillis(20))
            }).use {
                assertEquals(""""world"""", result.await(Duration.ofSeconds(2)))
            }
        }
    }

    @Test
    fun typedCodecHelpersWorkForQueuesAndTasks() = runBlocking {
        honker(tmp.resolve("typed.db")) {
            fallbackPollInterval(Duration.ofMillis(2))
        }.use { db ->
            val queue = db.queue("typed")
            queue.enqueue("hello", stringCodec)
            val job = queue.claimOne("worker").orElseThrow()
            assertEquals("hello", job.decode(stringCodec))
            job.ack()

            val registry = db.taskRegistry()
            val task = registry.registerTypedJson("typed-task", "typed-tasks", stringCodec) { "world" }
            val result = task.enqueue("[]", "{}")
            db.runTasks(registry, taskWorkerOptions {
                concurrency(1)
                idlePollInterval(Duration.ofMillis(20))
            }).use {
                assertEquals("world", result.raw().await(stringCodec, Duration.ofSeconds(2)))
            }
        }
    }

    @Test
    fun claimedJobDetailsCarryEveryCoreField() {
        honker(tmp.resolve("job-fields.db")) {
            fallbackPollInterval(Duration.ofMillis(2))
        }.use { db ->
            val queue = db.queue("full-fields", queueOptions {
                visibilityTimeout(Duration.ofSeconds(300))
                maxAttempts(5)
            })
            val runAt = unixNow(db) - 5

            val enqueuedBefore = unixNow(db)
            val id = queue.enqueueJson("""{"to":"alice@example.com"}""", enqueueOptions {
                runAt(Instant.ofEpochSecond(runAt))
                priority(7)
                expires(Duration.ofSeconds(600))
            })
            val enqueuedAfter = unixNow(db)

            val claimedBefore = unixNow(db)
            val claimed = queue.claimOne("worker-1").orElseThrow()
            val claimedAfter = unixNow(db)

            val job = claimed.details()
            assertEquals(id, job.id)
            assertEquals("full-fields", job.queue)
            assertEquals("""{"to":"alice@example.com"}""", job.payload)
            assertEquals("""{"to":"alice@example.com"}""", job.payloadJson)
            assertEquals("processing", job.state)
            assertEquals(7, job.priority)
            assertEquals(runAt, job.runAt)
            assertEquals("worker-1", job.workerId)
            assertEquals(1, job.attempts)
            assertEquals(5, job.maxAttempts)
            assertBetween(job.createdAt, enqueuedBefore, enqueuedAfter, "createdAt")
            assertBetween(assertNotNull(job.expiresAt), enqueuedBefore + 600, enqueuedAfter + 600, "expiresAt")
            assertBetween(
                assertNotNull(job.claimExpiresAt),
                claimedBefore + 300,
                claimedAfter + 300,
                "claimExpiresAt",
            )

            // Details are data only; the claim operations stay on the claimed job.
            assertTrue(claimed.ack())
        }
    }

    @Test
    fun jobDetailsFollowPendingProcessingAndAckedStates() {
        honker(tmp.resolve("snapshots.db")) {
            fallbackPollInterval(Duration.ofMillis(2))
        }.use { worker ->
            honker(tmp.resolve("snapshots.db")) {
                fallbackPollInterval(Duration.ofMillis(2))
            }.use { reader ->
                val writes = worker.queue("snapshots", queueOptions {
                    visibilityTimeout(Duration.ofSeconds(120))
                    maxAttempts(4)
                })
                val reads = reader.queue("snapshots")

                val runAt = unixNow(worker) - 2
                val enqueuedBefore = unixNow(worker)
                val id = writes.enqueueJson("""{"n":1}""", enqueueOptions {
                    runAt(Instant.ofEpochSecond(runAt))
                    priority(2)
                })
                val enqueuedAfter = unixNow(worker)

                val pending = assertNotNull(reads.jobDetails(id))
                assertEquals(id, pending.id)
                assertEquals("snapshots", pending.queue)
                assertEquals("""{"n":1}""", pending.payload)
                assertEquals("pending", pending.state)
                assertEquals(2, pending.priority)
                assertEquals(runAt, pending.runAt)
                assertNull(pending.workerId, "a pending job has no worker")
                assertNull(pending.claimExpiresAt, "a pending job has no claim deadline")
                assertEquals(0, pending.attempts)
                assertEquals(4, pending.maxAttempts)
                assertBetween(pending.createdAt, enqueuedBefore, enqueuedAfter, "createdAt")
                assertNull(pending.expiresAt, "no expires means no expiresAt")

                val claimedBefore = unixNow(worker)
                val claimed = writes.claimOne("worker-9").orElseThrow()
                val claimedAfter = unixNow(worker)

                val processing = assertNotNull(reads.jobDetails(id))
                assertEquals(id, processing.id)
                assertEquals("processing", processing.state)
                assertEquals("worker-9", processing.workerId)
                assertEquals(1, processing.attempts)
                assertEquals(4, processing.maxAttempts)
                assertEquals(2, processing.priority)
                assertEquals(runAt, processing.runAt)
                assertEquals(pending.createdAt, processing.createdAt)
                assertBetween(
                    assertNotNull(processing.claimExpiresAt),
                    claimedBefore + 120,
                    claimedAfter + 120,
                    "claimExpiresAt",
                )

                assertTrue(claimed.ack())
                assertNull(reads.jobDetails(id), "an ack'd job leaves no live row")
            }
        }
    }

    @Test
    fun delayedJobDetailsReportItsRunAt() {
        honker(tmp.resolve("delayed.db")) {
            fallbackPollInterval(Duration.ofMillis(2))
        }.use { db ->
            val queue = db.queue("delayed-snapshot")

            val before = unixNow(db)
            val id = queue.enqueueJson("""{"later":true}""", enqueueOptions {
                delay(Duration.ofSeconds(30))
            })
            val after = unixNow(db)

            val details = assertNotNull(queue.jobDetails(id))
            assertEquals("pending", details.state)
            assertBetween(details.runAt, before + 30, after + 30, "runAt")
            assertTrue(details.runAt > unixNow(db), "a delayed job runs in the future")
            assertTrue(queue.claimOne("too-early").isEmpty, "a delayed job is not claimable yet")
        }
    }

    @Test
    fun typedQueueDetailsDecodePayloadAndKeepEveryField() {
        honker(tmp.resolve("typed-fields.db")) {
            fallbackPollInterval(Duration.ofMillis(2))
        }.use { db ->
            val typed = db.queue("typed-fields", queueOptions {
                visibilityTimeout(Duration.ofSeconds(60))
                maxAttempts(2)
            }).typed(stringCodec)

            val runAt = unixNow(db) - 1
            val enqueuedBefore = unixNow(db)
            val id = typed.enqueue("welcome", enqueueOptions {
                runAt(Instant.ofEpochSecond(runAt))
                priority(4)
                expires(Duration.ofSeconds(900))
            })
            val enqueuedAfter = unixNow(db)

            val pending = assertNotNull(typed.jobDetails(id))
            assertEquals("welcome", pending.payload)
            assertEquals(""""welcome"""", pending.payloadJson)
            assertEquals("pending", pending.state)
            assertEquals(4, pending.priority)
            assertEquals(runAt, pending.runAt)
            assertEquals(0, pending.attempts)
            assertEquals(2, pending.maxAttempts)
            assertNull(pending.workerId)
            assertNull(pending.claimExpiresAt)
            assertBetween(pending.createdAt, enqueuedBefore, enqueuedAfter, "createdAt")
            assertBetween(assertNotNull(pending.expiresAt), enqueuedBefore + 900, enqueuedAfter + 900, "expiresAt")

            val claimedBefore = unixNow(db)
            val claimed = typed.claimOne("typed-worker").orElseThrow()
            val claimedAfter = unixNow(db)

            val job = claimed.details()
            assertEquals(id, job.id)
            assertEquals("typed-fields", job.queue)
            assertEquals("welcome", job.payload)
            assertEquals(""""welcome"""", job.payloadJson)
            assertEquals("processing", job.state)
            assertEquals(4, job.priority)
            assertEquals(runAt, job.runAt)
            assertEquals("typed-worker", job.workerId)
            assertEquals(1, job.attempts)
            assertEquals(2, job.maxAttempts)
            assertBetween(job.createdAt, enqueuedBefore, enqueuedAfter, "createdAt")
            assertBetween(assertNotNull(job.expiresAt), enqueuedBefore + 900, enqueuedAfter + 900, "expiresAt")
            assertBetween(assertNotNull(job.claimExpiresAt), claimedBefore + 60, claimedAfter + 60, "claimExpiresAt")

            assertTrue(claimed.ack())
            assertNull(typed.jobDetails(id))
        }
    }

    @Test
    fun jobDetailsAreScopedToTheirQueueWhileDatabaseLookupIsGlobal() {
        honker(tmp.resolve("scoped.db")) {
            fallbackPollInterval(Duration.ofMillis(2))
        }.use { db ->
            val emails = db.queue("scoped-emails")
            val reports = db.queue("scoped-reports")
            val id = emails.enqueueJson("""{"to":"alice@example.com"}""")

            assertNull(reports.jobDetails(id), "another queue must not see this job")
            assertEquals("scoped-emails", assertNotNull(emails.jobDetails(id)).queue)
            assertEquals("scoped-emails", assertNotNull(db.jobDetails(id)).queue)
            assertNull(db.jobDetails(id + 10_000), "an unknown id has no snapshot")
        }
    }

    @Test
    fun codecOverloadsDecodeOnEveryReceiver() {
        honker(tmp.resolve("codecs.db")) {
            fallbackPollInterval(Duration.ofMillis(2))
        }.use { db ->
            val emails = db.queue("codec-emails")
            val reports = db.queue("codec-reports")
            val id = emails.enqueue("alice@example.com", stringCodec)

            val fromQueue = assertNotNull(emails.jobDetails(id, stringCodec))
            assertEquals("alice@example.com", fromQueue.payload)
            assertEquals(""""alice@example.com"""", fromQueue.payloadJson)
            assertEquals("codec-emails", fromQueue.queue)
            assertNull(
                reports.jobDetails(id, stringCodec),
                "the codec overload is scoped to its queue like the raw one",
            )

            val fromDatabase = assertNotNull(db.jobDetails(id, stringCodec))
            assertEquals("alice@example.com", fromDatabase.payload)
            assertEquals(""""alice@example.com"""", fromDatabase.payloadJson)

            val snapshot = emails.getJob(id).orElseThrow()
            assertEquals("alice@example.com", snapshot.details(stringCodec).payload)
            assertEquals(""""alice@example.com"""", snapshot.details().payload)

            val claimed = emails.claimOne("codec-worker").orElseThrow()
            val decoded = claimed.details(stringCodec)
            assertEquals("alice@example.com", decoded.payload)
            assertEquals(""""alice@example.com"""", decoded.payloadJson)
            assertEquals("processing", decoded.state)
            assertEquals("codec-worker", decoded.workerId)
            // The raw overload on the same job leaves the payload as JSON.
            assertEquals(""""alice@example.com"""", claimed.details().payload)

            assertTrue(claimed.ack())
            assertNull(db.jobDetails(id, stringCodec))
        }
    }

    @Test
    fun retriedJobDetailsDropBackToPendingWithNoWorker() {
        honker(tmp.resolve("retried.db")) {
            fallbackPollInterval(Duration.ofMillis(2))
        }.use { db ->
            val queue = db.queue("retries", queueOptions {
                visibilityTimeout(Duration.ofSeconds(60))
                maxAttempts(3)
            })
            val id = queue.enqueueJson("""{"n":1}""")

            val claimed = queue.claimOne("worker-a").orElseThrow()
            val processing = assertNotNull(queue.jobDetails(id))
            assertEquals("processing", processing.state)
            assertEquals("worker-a", processing.workerId)
            assertNotNull(processing.claimExpiresAt, "a claimed job has a claim deadline")

            val retriedBefore = unixNow(db)
            assertTrue(claimed.retry(Duration.ofSeconds(45), "boom"))
            val retriedAfter = unixNow(db)

            // The only path where a non-null worker and claim deadline go
            // back to null on the same row.
            val pending = assertNotNull(queue.jobDetails(id))
            assertEquals("pending", pending.state)
            assertNull(pending.workerId, "a retried job releases its worker")
            assertNull(pending.claimExpiresAt, "a retried job releases its claim deadline")
            assertEquals(1, pending.attempts, "the spent attempt still counts")
            assertEquals(3, pending.maxAttempts)
            assertEquals(processing.createdAt, pending.createdAt)
            assertBetween(pending.runAt, retriedBefore + 45, retriedAfter + 45, "runAt")
        }
    }

    @Test
    fun deadLetteredJobDetailsAreGoneFromBothLookups() {
        honker(tmp.resolve("dead.db")) {
            fallbackPollInterval(Duration.ofMillis(2))
        }.use { db ->
            val queue = db.queue("dead-letters", queueOptions {
                visibilityTimeout(Duration.ofSeconds(60))
                maxAttempts(1)
            })
            val id = queue.enqueueJson("""{"n":1}""")

            val claimed = queue.claimOne("worker-b").orElseThrow()
            assertEquals("processing", assertNotNull(queue.jobDetails(id)).state)

            assertTrue(claimed.fail("nope"))
            assertNull(queue.jobDetails(id), "a dead-lettered job leaves no live row")
            assertNull(db.jobDetails(id), "the global lookup does not see dead letters either")
        }
    }

    @Test
    // `runBlocking<Unit>` is deliberate: an expression-bodied test whose
    // last expression is not Unit compiles to a non-void method and JUnit
    // silently never runs it.
    fun queueFlowEndsWhenTheDatabaseClosesInsteadOfParkingTheCollector() = runBlocking<Unit> {
        val db = honker(tmp.resolve("flow-close.db")) {
            fallbackPollInterval(Duration.ofMillis(2))
        }
        val jobs = db.queue("flow-close").asFlow("flow-close-worker", workerOptions {
            idlePollInterval(Duration.ofMillis(20))
        })
        val drained = async { runCatching { jobs.collect { } } }
        delay(200)

        // The producer thread is now inside its claim loop. Closing the
        // database makes every call it depends on throw. It must end the
        // flow — normally or with the failure — never leave the collector
        // parked on a producer that already died.
        db.close()
        val finished = withTimeoutOrNull(15_000) { drained.await() }
        drained.cancel()
        assertNotNull(finished, "closing the database must end the queue flow")
    }

    private fun unixNow(db: Database): Long =
        db.query("SELECT unixepoch() AS t").first().getLong("t")

    private fun assertBetween(actual: Long, low: Long, high: Long, field: String) {
        assertTrue(
            actual in low..high,
            "$field should be within [$low, $high] but was $actual",
        )
    }

    @Test
    fun dslHelpersForwardJavaRuntimeAndValidation() {
        honker(tmp.resolve("dsl.db")) {
            fallbackPollInterval(Duration.ofMillis(2))
            watcherOptions(watcherOptions {
                backend(WatcherBackend.PRAGMA_DATA_VERSION)
                pollInterval(Duration.ofMillis(1))
                subscriberBufferSize(32)
            })
        }.use { db ->
            val queue = db.queue("kotlin", queueOptions {
                visibilityTimeout(Duration.ofSeconds(1))
                maxAttempts(1)
            })
            queue.enqueueJson("""{"from":"kotlin"}""", enqueueOptions {
                priority(3)
            })
            val seen = CountDownLatch(1)
            queue.worker("worker", workerOptions {
                concurrency(1)
                idlePollInterval(Duration.ofMillis(20))
            }) { job ->
                assertEquals("""{"from":"kotlin"}""", job.payloadJson())
                seen.countDown()
            }.use {
                assertTrue(seen.await(2, TimeUnit.SECONDS))
            }
        }

        assertFailsWith<HonkerInvalidOptionException> {
            queueOptions { visibilityTimeout(Duration.ofMillis(500)) }
        }
    }
}
