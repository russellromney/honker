package dev.honker.kotlin

import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.async
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import dev.honker.HonkerInvalidOptionException
import dev.honker.JsonCodec
import dev.honker.WatcherBackend
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.assertFailsWith

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
