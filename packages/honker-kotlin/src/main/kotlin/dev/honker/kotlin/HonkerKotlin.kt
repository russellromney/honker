package dev.honker.kotlin

import dev.honker.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.withContext
import java.nio.file.Path
import java.time.Duration

fun honker(path: String): Database = Honker.open(path)

fun honker(path: String, options: OpenOptions): Database = Honker.open(path, options)

fun honker(path: String, build: OpenOptions.Builder.() -> Unit): Database =
    Honker.open(path, openOptions(build))

fun honker(path: Path): Database = Honker.open(path)

fun honker(path: Path, options: OpenOptions): Database = Honker.open(path, options)

fun honker(path: Path, build: OpenOptions.Builder.() -> Unit): Database =
    Honker.open(path, openOptions(build))

fun openOptions(build: OpenOptions.Builder.() -> Unit): OpenOptions =
    OpenOptions.builder().apply(build).build()

fun watcherOptions(build: WatcherOptions.Builder.() -> Unit): WatcherOptions =
    WatcherOptions.builder().apply(build).build()

fun queueOptions(build: QueueOptions.Builder.() -> Unit): QueueOptions =
    QueueOptions.builder().apply(build).build()

fun enqueueOptions(build: EnqueueOptions.Builder.() -> Unit): EnqueueOptions =
    EnqueueOptions.builder().apply(build).build()

fun waitOptions(build: WaitOptions.Builder.() -> Unit): WaitOptions =
    WaitOptions.builder().apply(build).build()

fun workerOptions(build: WorkerOptions.Builder.() -> Unit): WorkerOptions =
    WorkerOptions.builder().apply(build).build()

fun listenOptions(build: ListenOptions.Builder.() -> Unit): ListenOptions =
    ListenOptions.builder().apply(build).build()

fun subscribeOptions(build: SubscribeOptions.Builder.() -> Unit): SubscribeOptions =
    SubscribeOptions.builder().apply(build).build()

fun schedulerOptions(build: SchedulerOptions.Builder.() -> Unit): SchedulerOptions =
    SchedulerOptions.builder().apply(build).build()

fun scheduleOptions(build: ScheduleOptions.Builder.() -> Unit): ScheduleOptions =
    ScheduleOptions.builder().apply(build).build()

fun lockOptions(build: LockOptions.Builder.() -> Unit): LockOptions =
    LockOptions.builder().apply(build).build()

fun outboxOptions(build: OutboxOptions.Builder.() -> Unit): OutboxOptions =
    OutboxOptions.builder().apply(build).build()

fun taskOptions(build: TaskOptions.Builder.() -> Unit): TaskOptions =
    TaskOptions.builder().apply(build).build()

fun taskWorkerOptions(build: TaskWorkerOptions.Builder.() -> Unit): TaskWorkerOptions =
    TaskWorkerOptions.builder().apply(build).build()

fun notificationPruneOptions(build: NotificationPruneOptions.Builder.() -> Unit): NotificationPruneOptions =
    NotificationPruneOptions.builder().apply(build).build()

fun Stream.asFlow(options: SubscribeOptions = SubscribeOptions.defaults()): Flow<Event> = callbackFlow {
    val handle = subscribe({ event -> trySend(event).isSuccess }, options)
    awaitClose { handle.close() }
}

suspend fun TaskResult.await(timeout: Duration? = null): String =
    withContext(Dispatchers.IO) {
        waitFor(if (timeout == null) WaitOptions.forever() else WaitOptions.timeout(timeout))
    }

fun Queue.enqueueJson(json: String): Long = enqueue(json)

fun Queue.enqueueJson(json: String, options: EnqueueOptions): Long = enqueue(json, options)

fun Queue.worker(
    workerId: String,
    options: WorkerOptions = WorkerOptions.defaults(),
    handler: (Job) -> Unit,
): WorkerHandle = worker(workerId, JobHandler { job -> handler(job) }, options)

fun Database.taskRegistry(): TaskRegistry = TaskRegistry(this)

fun TaskRegistry.registerJson(
    name: String,
    queue: String,
    options: TaskOptions = TaskOptions.defaults(),
    handler: (TaskCall) -> String,
): TaskHandle = register(name, queue, TaskHandler { call -> handler(call) }, options)

fun TaskHandle.enqueueJson(argsJson: String = "[]", kwargsJson: String = "{}"): TaskResult =
    enqueue(argsJson, kwargsJson)
