package dev.honker.kotlin

import dev.honker.Database
import dev.honker.Job
import dev.honker.JobSnapshot
import dev.honker.JsonCodec
import dev.honker.Queue
import dev.honker.TypedJob
import dev.honker.TypedJobSnapshot
import dev.honker.TypedQueue

/**
 * Every field the core returns for one live job row, with the payload
 * decoded to [T].
 *
 * Data only: ack, retry, fail and heartbeat stay on a claimed [Job],
 * because only the worker holding the claim may call them.
 *
 * The Java [JobSnapshot] record carries the same twelve fields with
 * platform types. This data class restates them in Kotlin so the compiler
 * knows which columns can be absent: a pending job has no [workerId] and
 * no [claimExpiresAt], and a job enqueued without `expires` has no
 * [expiresAt]. Times are unix epoch seconds.
 *
 * [T] is a compile-time contract between the callers that share a queue.
 * Honker never inspects payload shape in the database, so every writer
 * must agree on the JSON it produces.
 */
data class JobDetails<T>(
    val id: Long,
    val queue: String,
    val payload: T,
    val payloadJson: String,
    val state: String,
    val priority: Int,
    val runAt: Long,
    val workerId: String?,
    val claimExpiresAt: Long?,
    val attempts: Int,
    val maxAttempts: Int,
    val createdAt: Long,
    val expiresAt: Long?,
)

private fun <T> JobSnapshot.toDetails(payload: T): JobDetails<T> = JobDetails(
    id = id(),
    queue = queue(),
    payload = payload,
    payloadJson = payloadJson(),
    state = state(),
    priority = priority(),
    runAt = runAt(),
    workerId = workerId(),
    claimExpiresAt = claimExpiresAt(),
    attempts = attempts(),
    maxAttempts = maxAttempts(),
    createdAt = createdAt(),
    expiresAt = expiresAt(),
)

/** Every field, payload left as the raw JSON string. */
fun JobSnapshot.details(): JobDetails<String> = toDetails(payloadJson())

/** Every field, payload decoded with [codec]. */
fun <T> JobSnapshot.details(codec: JsonCodec<T>): JobDetails<T> =
    toDetails(codec.decode(payloadJson()))

/** Every field, keeping the payload this snapshot already decoded. */
fun <T> TypedJobSnapshot<T>.details(): JobDetails<T> = raw().toDetails(payload())

/** Every field of a job this worker holds a claim on, payload left as JSON. */
fun Job.details(): JobDetails<String> = snapshot().details()

/** Every field of a job this worker holds a claim on, payload decoded with [codec]. */
fun <T> Job.details(codec: JsonCodec<T>): JobDetails<T> = snapshot().details(codec)

/** Every field of a claimed typed job, payload decoded by the queue's codec. */
// `raw().snapshot()`, not `snapshot()`: TypedJob.snapshot() returns the
// decoded TypedJobSnapshot<T> on the current JVM binding and the undecoded
// JobSnapshot on older ones. Job.snapshot() is JobSnapshot either way.
fun <T> TypedJob<T>.details(): JobDetails<T> = raw().snapshot().toDetails(payload())

/**
 * A snapshot of one live job in *this* queue, or `null` when the job was
 * ack'd, dead-lettered, never existed, or belongs to another queue.
 */
fun Queue.jobDetails(jobId: Long): JobDetails<String>? =
    getJob(jobId).map { it.details() }.orElse(null)

/** As [jobDetails], with the payload decoded by [codec]. */
fun <T> Queue.jobDetails(jobId: Long, codec: JsonCodec<T>): JobDetails<T>? =
    getJob(jobId).map { it.details(codec) }.orElse(null)

/** As [jobDetails], with the payload decoded by this typed queue's codec. */
fun <T> TypedQueue<T>.jobDetails(jobId: Long): JobDetails<T>? =
    getJob(jobId).map { it.details() }.orElse(null)

/**
 * A snapshot of one live job looked up by id across every queue, or
 * `null` when the job is gone. Use [Queue.jobDetails] to scope the lookup
 * to one queue.
 */
fun Database.jobDetails(jobId: Long): JobDetails<String>? =
    getJob(jobId).map { it.details() }.orElse(null)

/** As [jobDetails], with the payload decoded by [codec]. */
fun <T> Database.jobDetails(jobId: Long, codec: JsonCodec<T>): JobDetails<T>? =
    getJob(jobId).map { it.details(codec) }.orElse(null)
