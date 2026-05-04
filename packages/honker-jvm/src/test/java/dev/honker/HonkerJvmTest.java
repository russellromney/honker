package dev.honker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class HonkerJvmTest {
    @TempDir
    Path tmp;

    @Test
    void queueClaimAckAndResultRoundTrip() {
        try (Database db = open()) {
            Queue q = db.queue("emails");
            long id = q.enqueue("{\"to\":\"alice@example.com\"}");

            Job job = q.claimOne("worker-1").orElseThrow();
            assertEquals(id, job.id());
            assertEquals("{\"to\":\"alice@example.com\"}", job.payloadJson());

            q.saveResult(job.id(), "{\"ok\":true}", ResultOptions.ttl(Duration.ofMinutes(5)));
            assertEquals("{\"ok\":true}", q.getResult(job.id()).orElseThrow());
            assertTrue(job.ack());
            assertTrue(q.claimOne("worker-2").isEmpty());
        }
    }

    @Test
    void queueNegativePathsAndBatchOperations() {
        try (Database db = open()) {
            Queue q = db.queue("batch", QueueOptions.builder()
                .visibilityTimeout(Duration.ofSeconds(1))
                .maxAttempts(2)
                .build());
            long first = q.enqueue("{\"n\":1}", EnqueueOptions.builder().priority(10).build());
            long second = q.enqueue("{\"n\":2}");

            assertTrue(q.claimBatch("worker", 0).isEmpty());
            List<Job> jobs = q.claimBatch("worker", 10);
            assertEquals(2, jobs.size());
            assertEquals(first, jobs.get(0).id());
            assertEquals(second, jobs.get(1).id());
            assertFalse(q.ack(jobs.get(0).id(), "wrong"));
            assertTrue(jobs.get(0).heartbeat(Duration.ofSeconds(1)));
            assertEquals(2, q.ackBatch(List.of(first, second), "worker"));
            assertTrue(q.claimOne("next").isEmpty());
        }
    }

    @Test
    void queueDelayExpiryAndWholeSecondValidation() throws Exception {
        try (Database db = open()) {
            Queue delayed = db.queue("delay-expire");
            delayed.enqueue("{\"later\":true}", EnqueueOptions.builder().delay(Duration.ofSeconds(1)).build());
            assertTrue(delayed.claimOne("early").isEmpty());
            waitUntil(Duration.ofSeconds(3), () -> delayed.claimOne("late").isPresent(), "delayed job should become claimable");

            Queue expiring = db.queue("expiring");
            expiring.enqueue("{\"expire\":true}", EnqueueOptions.builder().expires(Duration.ofSeconds(1)).build());
            Thread.sleep(1_100);
            assertEquals(1, expiring.sweepExpired());
            assertEquals(1, count(db, "SELECT COUNT(*) AS n FROM _honker_dead WHERE queue='expiring'"));

            assertThrows(HonkerInvalidOptionException.class, () -> EnqueueOptions.builder().delay(Duration.ofMillis(1)));
            assertThrows(HonkerInvalidOptionException.class, () -> QueueOptions.builder().visibilityTimeout(Duration.ofMillis(500)));
            assertThrows(HonkerInvalidOptionException.class, () -> ResultOptions.ttl(Duration.ofMillis(500)));
            assertThrows(HonkerInvalidOptionException.class, () -> new RetryableException("soon", Duration.ofMillis(500)));
        }
    }

    @Test
    void resultTimeoutAndSweepExpiredResults() throws Exception {
        try (Database db = open()) {
            Queue q = db.queue("results");
            assertThrows(HonkerTimeoutException.class, () -> q.waitResult(99, WaitOptions.builder()
                .timeout(Duration.ofMillis(50))
                .fallbackPollInterval(Duration.ofMillis(5))
                .build()));

            long id = q.enqueue("{\"work\":true}");
            q.saveResult(id, "{\"ok\":true}", ResultOptions.ttl(Duration.ofSeconds(1)));
            assertEquals("{\"ok\":true}", q.waitResult(id, WaitOptions.timeout(Duration.ofSeconds(1))));
            Thread.sleep(1_100);
            assertTrue(q.getResult(id).isEmpty());
            assertEquals(1, q.sweepResults());
        }
    }

    @Test
    void transactionRollbackDropsEnqueue() {
        try (Database db = open()) {
            Queue q = db.queue("rollback");
            assertThrows(RuntimeException.class, () -> db.transaction(tx -> {
                q.enqueue(tx, "{\"n\":1}");
                throw new RuntimeException("boom");
            }));
            assertTrue(q.claimOne("worker").isEmpty());
        }
    }

    @Test
    void transactionCommitsMultipleFeatureWritesAtomically() {
        try (Database db = open()) {
            Queue q = db.queue("atomic");
            Stream stream = db.stream("atomic-stream");
            try (Listener listener = db.listen("atomic-channel")) {
                db.transactionVoid(tx -> {
                    q.enqueue(tx, "{\"job\":1}");
                    stream.publish(tx, "{\"event\":1}");
                    tx.notify("atomic-channel", "{\"note\":1}");
                });
                assertEquals("{\"note\":1}", listener.next(Duration.ofSeconds(1)).orElseThrow().payloadJson());
            }
            assertEquals("{\"job\":1}", q.claimOne("worker").orElseThrow().payloadJson());
            assertEquals("{\"event\":1}", stream.readSince(0, 10).get(0).payloadJson());
            assertEquals(1, count(db, "SELECT COUNT(*) AS n FROM _honker_notifications WHERE channel='atomic-channel' AND payload='{\"note\":1}'"));
        }
    }

    @Test
    void notifyListenStartsFromCurrentMaxAndWakes() {
        try (Database db = open()) {
            db.notify("orders", "{\"old\":true}");
            try (Listener listener = db.listen("orders")) {
                db.notify("other", "{\"skip\":true}");
                db.notify("orders", "{\"id\":42}");
                Notification n = listener.next(Duration.ofSeconds(2)).orElseThrow();
                assertEquals("orders", n.channel());
                assertEquals("{\"id\":42}", n.payloadJson());
                assertTrue(listener.next(Duration.ofMillis(30)).isEmpty());
            }
        }
    }

    @Test
    void listenerCallbackAndPruneWork() throws Exception {
        try (Database db = open()) {
            CountDownLatch seen = new CountDownLatch(1);
            try (ListenHandle ignored = db.listen(
                "callback",
                n -> {
                    if ("{\"ok\":true}".equals(n.payloadJson())) {
                        seen.countDown();
                    }
                },
                ListenOptions.builder().pollTimeout(Duration.ofMillis(50)).build()
            )) {
                db.notify("callback", "{\"ok\":true}");
                assertTrue(seen.await(2, TimeUnit.SECONDS));
            }
            db.notify("callback", "{\"keep\":1}");
            db.notify("callback", "{\"keep\":2}");
            assertEquals(2, db.pruneNotifications(NotificationPruneOptions.builder().maxKeep(1).build()));
            db.transactionVoid(tx -> tx.execute("UPDATE _honker_notifications SET created_at = unixepoch() - 120 WHERE channel='callback'"));
            assertEquals(1, db.pruneNotifications(NotificationPruneOptions.builder().olderThan(Duration.ofSeconds(60)).build()));
            assertThrows(HonkerInvalidOptionException.class, () -> NotificationPruneOptions.builder().olderThan(Duration.ofMillis(1)));
        }
    }

    @Test
    void listenerAndStreamCallbackFailuresAreObservable() {
        try (Database db = open()) {
            try (ListenHandle listen = db.listen(
                "explode",
                n -> {
                    throw new IllegalStateException("listener exploded");
                },
                ListenOptions.builder().pollTimeout(Duration.ofMillis(20)).build()
            )) {
                db.notify("explode", "{\"boom\":true}");
                waitUntil(Duration.ofSeconds(2), listen::failed, "listener failure should be visible");
                HonkerException e = assertThrows(HonkerException.class, listen::throwIfFailed);
                assertTrue(e.getMessage().contains("notification handler failed"));
            }

            Stream stream = db.stream("explode-stream");
            try (StreamHandle handle = stream.subscribe(
                event -> {
                    throw new IllegalStateException("stream exploded");
                },
                SubscribeOptions.builder().pollTimeout(Duration.ofMillis(20)).build()
            )) {
                stream.publish("{\"boom\":true}");
                waitUntil(Duration.ofSeconds(2), handle::failed, "stream failure should be visible");
                HonkerException e = assertThrows(HonkerException.class, handle::throwIfFailed);
                assertTrue(e.getMessage().contains("stream handler failed"));
            }
        }
    }

    @Test
    void streamPublishReadAndOffsets() {
        try (Database db = open()) {
            Stream stream = db.stream("events");
            stream.publish("{\"n\":1}", "a");
            stream.publish("{\"n\":2}", "b");

            List<Event> events = stream.readSince(0, 10);
            assertEquals(2, events.size());
            assertEquals("a", events.get(0).key());
            assertEquals("{\"n\":2}", events.get(1).payloadJson());

            stream.saveOffset("consumer-1", events.get(1).offset());
            stream.saveOffset("consumer-1", events.get(0).offset());
            assertEquals(events.get(1).offset(), stream.getOffset("consumer-1"));

            assertEquals(1, stream.readSince(events.get(0).offset(), 10).size());
            assertTrue(stream.readSince(events.get(1).offset(), 10).isEmpty());
        }
    }

    @Test
    void streamSubscribeReplaysGoesLiveAndSavesOffset() throws Exception {
        try (Database db = open()) {
            Stream stream = db.stream("sub");
            stream.publish("{\"n\":1}");
            CountDownLatch seen = new CountDownLatch(2);
            try (StreamHandle ignored = stream.subscribe(
                event -> seen.countDown(),
                SubscribeOptions.builder()
                    .consumer("consumer")
                    .saveEveryN(1)
                    .pollTimeout(Duration.ofMillis(50))
                    .build()
            )) {
                stream.publish("{\"n\":2}");
                assertTrue(seen.await(2, TimeUnit.SECONDS));
            }
            assertEquals(2, stream.getOffset("consumer"));
        }
    }

    @Test
    void streamSubscribeCanStartFromOffsetAndDisableAutoSave() throws Exception {
        try (Database db = open()) {
            Stream stream = db.stream("manual-sub");
            stream.publish("{\"n\":1}");
            stream.publish("{\"n\":2}");
            CountDownLatch seen = new CountDownLatch(1);
            try (StreamHandle ignored = stream.subscribe(
                event -> {
                    assertEquals("{\"n\":2}", event.payloadJson());
                    seen.countDown();
                },
                SubscribeOptions.builder()
                    .consumer("manual")
                    .fromOffset(1L)
                    .saveEveryN(0)
                    .saveEvery(Duration.ZERO)
                    .pollTimeout(Duration.ofMillis(50))
                    .build()
            )) {
                assertTrue(seen.await(2, TimeUnit.SECONDS));
            }
            assertEquals(0, stream.getOffset("manual"));
            assertThrows(HonkerInvalidOptionException.class, () -> SubscribeOptions.builder().fromOffset(-1L));
        }
    }

    @Test
    void lockBlocksSecondOwnerUntilReleased() {
        try (Database db = open()) {
            LockOptions firstOwner = LockOptions.builder()
                .owner("owner-1")
                .ttl(Duration.ofSeconds(30))
                .build();
            LockOptions secondOwner = LockOptions.builder()
                .owner("owner-2")
                .ttl(Duration.ofSeconds(30))
                .build();
            try (LockHandle ignored = db.lock("nightly", firstOwner)) {
                assertThrows(LockHeldException.class, () -> db.lock("nightly", secondOwner));
            }
            try (LockHandle ignored = db.lock("nightly", secondOwner)) {
                assertEquals("owner-2", ignored.owner());
            }
        }
    }

    @Test
    void lockExpiresAndValidatesWholeSecondTtl() throws Exception {
        try (Database db = open()) {
            db.lock("short", LockOptions.builder().owner("old").ttl(Duration.ofSeconds(1)).build()).close();
            try (LockHandle ignored = db.lock("held", LockOptions.builder().owner("old").ttl(Duration.ofSeconds(1)).build())) {
                assertThrows(LockHeldException.class, () -> db.lock("held", LockOptions.builder().owner("new").ttl(Duration.ofSeconds(1)).build()));
            }
            try (LockHandle ignored = db.lock("held", LockOptions.builder().owner("new").ttl(Duration.ofSeconds(1)).build())) {
                assertEquals("new", ignored.owner());
            }
            try (LockHandle ignored = db.lock("expires", LockOptions.builder().owner("old").ttl(Duration.ofSeconds(1)).build())) {
                assertThrows(LockHeldException.class, () -> db.lock("expires", LockOptions.builder().owner("new").ttl(Duration.ofSeconds(1)).build()));
                Thread.sleep(1_100);
                try (LockHandle stolen = db.lock("expires", LockOptions.builder().owner("new").ttl(Duration.ofSeconds(1)).build())) {
                    assertEquals("new", stolen.owner());
                }
            }
            assertThrows(HonkerInvalidOptionException.class, () -> LockOptions.builder().ttl(Duration.ofMillis(500)));
        }
    }

    @Test
    void schedulerRegistersAndTicksDueWork() {
        try (Database db = open()) {
            Scheduler scheduler = db.scheduler();
            scheduler.add("fast", "scheduled", CronSchedule.every(Duration.ofSeconds(1)), "{\"kind\":\"tick\"}");
            Instant soonest = scheduler.soonest().orElseThrow();

            assertEquals(1, scheduler.tick(soonest));
            Job job = db.queue("scheduled").claimOne("worker").orElseThrow();
            assertEquals("{\"kind\":\"tick\"}", job.payloadJson());
            assertTrue(job.ack());
            assertTrue(scheduler.remove("fast"));
        }
    }

    @Test
    void schedulerRunFiresDueWork() throws Exception {
        try (Database db = open()) {
            Queue queue = db.queue("sched-run");
            CountDownLatch seen = new CountDownLatch(1);
            try (WorkerHandle worker = queue.worker(
                "worker",
                job -> seen.countDown(),
                WorkerOptions.builder().idlePollInterval(Duration.ofSeconds(30)).build()
            )) {
                db.scheduler().add("fast-run", "sched-run", CronSchedule.every(Duration.ofSeconds(1)), "{\"kind\":\"tick\"}");
                try (SchedulerHandle ignored = db.scheduler().run(SchedulerOptions.builder()
                    .idlePollInterval(Duration.ofMillis(50))
                    .heartbeatEvery(Duration.ofMillis(100))
                    .lockTtl(Duration.ofSeconds(5))
                    .build())) {
                    assertTrue(seen.await(3, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test
    void schedulerRunFailsSynchronouslyWhenLeaderLockHeld() {
        try (Database db = open();
             LockHandle ignored = db.lock("honker-scheduler", LockOptions.builder().ttl(Duration.ofSeconds(5)).build())) {
            db.scheduler().add("fast-lock", "sched-lock", CronSchedule.every(Duration.ofSeconds(1)), "{\"kind\":\"tick\"}");
            assertThrows(LockHeldException.class, () -> db.scheduler().run(SchedulerOptions.builder()
                .idlePollInterval(Duration.ofMillis(50))
                .lockTtl(Duration.ofSeconds(5))
                .build()));
        }
    }

    @Test
    void schedulerRemoveInvalidCronAndOptionValidation() {
        try (Database db = open()) {
            assertFalse(db.scheduler().remove("missing"));
            assertThrows(RuntimeException.class, () -> CronSchedule.crontab("* *"));
            assertThrows(HonkerInvalidOptionException.class, () -> CronSchedule.every(Duration.ofMillis(500)));
            assertThrows(HonkerInvalidOptionException.class, () -> ScheduleOptions.builder().expires(Duration.ofMillis(500)));
            assertThrows(HonkerInvalidOptionException.class, () -> SchedulerOptions.builder().lockTtl(Duration.ofMillis(500)));
        }
    }

    @Test
    void rateLimitIsFixedWindow() {
        try (Database db = open()) {
            assertTrue(db.tryRateLimit("api", 2, Duration.ofSeconds(60)));
            assertTrue(db.tryRateLimit("api", 2, Duration.ofSeconds(60)));
            assertFalse(db.tryRateLimit("api", 2, Duration.ofSeconds(60)));
            assertThrows(HonkerInvalidOptionException.class, () -> db.tryRateLimit("bad", 0, Duration.ofSeconds(1)));
            assertThrows(HonkerInvalidOptionException.class, () -> db.tryRateLimit("bad", 1, Duration.ofMillis(500)));
            db.transactionVoid(tx -> tx.execute("UPDATE _honker_rate_limits SET window_start = 1 WHERE name='api'"));
            assertTrue(db.sweepRateLimits(Duration.ofSeconds(60)) >= 1);
        }
    }

    @Test
    void outboxDeliverOneRetriesThenAcks() {
        try (Database db = open()) {
            AtomicInteger calls = new AtomicInteger();
            Outbox outbox = db.outbox("webhooks", payload -> {
                if (calls.incrementAndGet() == 1) {
                    throw new RuntimeException("not yet");
                }
            }, OutboxOptions.builder().baseBackoff(Duration.ofSeconds(2)).build());
            outbox.enqueue("{\"url\":\"https://example.test\"}");

            assertTrue(outbox.deliverOne("worker"));
            assertFalse(outbox.deliverOne("worker"));
            waitUntil(Duration.ofSeconds(4), () -> outbox.deliverOne("worker"), "outbox job should retry after backoff");
            assertEquals(2, calls.get());
        }
    }

    @Test
    void outboxRunWorkerRetriesWithOutboxBackoffThenDelivers() throws Exception {
        try (Database db = open()) {
            CountDownLatch seen = new CountDownLatch(1);
            AtomicInteger calls = new AtomicInteger();
            Outbox outbox = db.outbox("async", payload -> {
                if (calls.incrementAndGet() == 1) {
                    throw new RuntimeException("try again");
                }
                seen.countDown();
            }, OutboxOptions.builder().baseBackoff(Duration.ofSeconds(3)).build());
            try (OutboxHandle ignored = outbox.runWorker(
                "worker",
                WorkerOptions.builder().idlePollInterval(Duration.ofSeconds(30)).build()
            )) {
                outbox.enqueue("{\"send\":true}");
                Thread.sleep(500);
                assertEquals(1, calls.get());
                assertTrue(seen.await(5, TimeUnit.SECONDS));
                assertEquals(2, calls.get());
            }
        }
    }

    @Test
    void outboxMovesToDeadAfterMaxAttemptsAndValidatesDurations() {
        try (Database db = open()) {
            Outbox outbox = db.outbox("dead-outbox", payload -> {
                throw new RuntimeException("nope");
            }, OutboxOptions.builder()
                .maxAttempts(1)
                .baseBackoff(Duration.ofSeconds(1))
                .visibilityTimeout(Duration.ofSeconds(1))
                .build());
            outbox.enqueue("{\"send\":false}");
            assertTrue(outbox.deliverOne("worker"));
            assertEquals(1, count(db, "SELECT COUNT(*) AS n FROM _honker_dead WHERE queue='_outbox:dead-outbox'"));
            assertThrows(HonkerInvalidOptionException.class, () -> OutboxOptions.builder().baseBackoff(Duration.ofMillis(500)));
            assertThrows(HonkerInvalidOptionException.class, () -> WorkerOptions.builder().defaultRetryDelay(Duration.ofMillis(500)));
        }
    }

    @Test
    void taskRegistryRunsTaskAndStoresResult() {
        try (Database db = open()) {
            TaskRegistry registry = new TaskRegistry(db);
            TaskHandle task = registry.register("echo", "tasks", call -> "{\"args\":" + call.argsJson() + ",\"kwargs\":" + call.kwargsJson() + "}");
            TaskResult result = task.enqueue("[\"a\"]", "{\"b\":2}");
            try (TaskWorkerHandle ignored = db.runTasks(
                registry,
                TaskWorkerOptions.builder().concurrency(1).idlePollInterval(Duration.ofMillis(50)).build()
            )) {
                assertEquals("{\"args\":[\"a\"],\"kwargs\":{\"b\":2}}", result.waitFor(WaitOptions.timeout(Duration.ofSeconds(2))));
            }
        }
    }

    @Test
    void taskWorkerFailureModesAreVisible() {
        try (Database db = open()) {
            TaskRegistry registry = new TaskRegistry(db);
            registry.register("known", "tasks", call -> "\"ok\"");
            Queue tasks = db.queue("tasks");
            tasks.enqueue("{\"raw\":true}");
            tasks.enqueue("{\"__honker_task__\":{\"task\":\"missing\",\"args\":[],\"kwargs\":{}}}");

            try (TaskWorkerHandle ignored = db.runTasks(
                registry,
                TaskWorkerOptions.builder().concurrency(1).idlePollInterval(Duration.ofMillis(20)).build()
            )) {
                waitUntil(Duration.ofSeconds(2), () -> count(db, "SELECT COUNT(*) AS n FROM _honker_dead WHERE queue='tasks'") == 2,
                    "raw and unknown task payloads should be dead-lettered");
            }
            List<Row> dead = db.query("SELECT last_error FROM _honker_dead WHERE queue='tasks' ORDER BY id");
            assertTrue(dead.get(0).getString("last_error").contains("raw"));
            assertTrue(dead.get(1).getString("last_error").contains("unknown task"));
        }
    }

    @Test
    void taskWorkerRetriesExceptionsAndHonorsRetryable() {
        try (Database db = open()) {
            TaskRegistry registry = new TaskRegistry(db);
            AtomicInteger failingCalls = new AtomicInteger();
            TaskHandle failing = registry.register("fail", "task-retry", call -> {
                failingCalls.incrementAndGet();
                throw new RuntimeException("boom");
            }, TaskOptions.builder().retries(2).retryDelay(Duration.ofSeconds(1)).build());
            AtomicInteger retryableCalls = new AtomicInteger();
            TaskHandle retryable = registry.register("retryable", "task-retry", call -> {
                if (retryableCalls.incrementAndGet() == 1) {
                    throw new RetryableException("later", Duration.ofSeconds(1));
                }
                return "\"done\"";
            }, TaskOptions.builder().retries(2).retryDelay(Duration.ofSeconds(1)).build());
            AtomicInteger noStoreCalls = new AtomicInteger();
            TaskHandle noStore = registry.register("no-store", "task-retry", call -> {
                noStoreCalls.incrementAndGet();
                return "\"hidden\"";
            },
                TaskOptions.builder().storeResult(false).build());

            failing.enqueue("[]", "{}");
            TaskResult retryableResult = retryable.enqueue("[]", "{}");
            TaskResult noStoreResult = noStore.enqueue("[]", "{}");

            try (TaskWorkerHandle ignored = db.runTasks(
                registry,
                TaskWorkerOptions.builder().concurrency(2).idlePollInterval(Duration.ofMillis(20)).build()
            )) {
                assertEquals("\"done\"", retryableResult.waitFor(WaitOptions.timeout(Duration.ofSeconds(3))));
                waitUntil(Duration.ofSeconds(3), () -> count(db, "SELECT COUNT(*) AS n FROM _honker_dead WHERE queue='task-retry'") == 1,
                    "failing task should reach dead letter after retries");
                waitUntil(Duration.ofSeconds(2), () -> noStoreCalls.get() == 1,
                    "storeResult=false task should run");
                assertEquals(0, count(db, "SELECT COUNT(*) AS n FROM _honker_live WHERE id=" + noStoreResult.id()));
                assertEquals(0, count(db, "SELECT COUNT(*) AS n FROM _honker_dead WHERE id=" + noStoreResult.id()));
                assertThrows(HonkerTimeoutException.class, () -> noStoreResult.waitFor(WaitOptions.builder()
                    .timeout(Duration.ofMillis(100))
                    .fallbackPollInterval(Duration.ofMillis(10))
                    .build()));
            }
            assertEquals(2, failingCalls.get());
            assertEquals(2, retryableCalls.get());
        }
    }

    @Test
    void concurrentWorkersClaimEachJobOnce() throws Exception {
        try (Database db = open()) {
            Queue queue = db.queue("parallel", QueueOptions.builder()
                .visibilityTimeout(Duration.ofSeconds(5))
                .maxAttempts(1)
                .build());
            int total = 200;
            for (int i = 0; i < total; i++) {
                queue.enqueue("{\"i\":" + i + "}");
            }
            Set<Long> seen = ConcurrentHashMap.newKeySet();
            AtomicInteger duplicates = new AtomicInteger();
            CountDownLatch handled = new CountDownLatch(total);
            try (WorkerHandle ignored = queue.worker(
                "parallel",
                job -> {
                    if (!seen.add(job.id())) {
                        duplicates.incrementAndGet();
                    }
                    handled.countDown();
                },
                WorkerOptions.builder()
                    .concurrency(8)
                    .idlePollInterval(Duration.ofSeconds(30))
                    .defaultRetryDelay(Duration.ZERO)
                    .build()
            )) {
                assertTrue(handled.await(5, TimeUnit.SECONDS));
            }
            assertEquals(0, duplicates.get());
            assertEquals(total, seen.size());
            assertEquals(0, count(db, "SELECT COUNT(*) AS n FROM _honker_live WHERE queue='parallel'"));
            assertEquals(0, count(db, "SELECT COUNT(*) AS n FROM _honker_dead WHERE queue='parallel'"));
        }
    }

    @Test
    void childJvmWorkerWakesWhenParentEnqueues() throws Exception {
        Path dbPath = tmp.resolve("multiprocess.db");
        Path extension = NativeLoader.resolve(OpenOptions.defaults());
        Path ready = tmp.resolve("child.ready");
        Path done = tmp.resolve("child.done");
        Process child = startChild("worker-marker", dbPath, extension, ready, done);
        try {
            waitUntil(Duration.ofSeconds(10), () -> Files.isRegularFile(ready), "child worker should become ready");
            try (Database db = Honker.open(dbPath, OpenOptions.builder()
                .extensionPath(extension)
                .fallbackPollInterval(Duration.ofMillis(2))
                .build())) {
                db.queue("multiprocess").enqueue("{\"from\":\"parent\"}");
            }
            waitUntil(Duration.ofSeconds(5), () -> Files.isRegularFile(done), "child worker should process parent enqueue");
            assertEquals("{\"from\":\"parent\"}", Files.readString(done, StandardCharsets.UTF_8));
            assertEquals(0, child.waitFor());
        } finally {
            child.destroyForcibly();
        }
    }

    @Test
    void listenerWorkerStreamAndResultWaitUseWatcherInsteadOfFallbackPolling() throws Exception {
        try (Database db = openWithLongFallback("wake-races.db")) {
            long slowFallbackMillis = Duration.ofSeconds(30).toMillis();

            try (Listener listener = db.listen("orders")) {
                assertEquals(1, db.updateWatcherSubscriberCount());
                Thread publisher = new Thread(() -> {
                    sleep(50);
                    db.notify("orders", "{\"id\":42}");
                });
                long start = System.nanoTime();
                publisher.start();
                Notification n = listener.next(Duration.ofSeconds(2)).orElseThrow();
                publisher.join();
                assertEquals("{\"id\":42}", n.payloadJson());
                assertWakeWasFast(start, slowFallbackMillis);
            }
            assertEquals(0, db.updateWatcherSubscriberCount());

            Queue queue = db.queue("race-worker");
            CountDownLatch handled = new CountDownLatch(1);
            try (WorkerHandle ignored = queue.worker(
                "worker",
                job -> handled.countDown(),
                WorkerOptions.builder().idlePollInterval(Duration.ofSeconds(30)).build()
            )) {
                waitUntil(Duration.ofSeconds(2), () -> db.updateWatcherSubscriberCount() == 1,
                    "worker should subscribe before waiting");
                long start = System.nanoTime();
                queue.enqueue("{\"n\":1}");
                assertTrue(handled.await(2, TimeUnit.SECONDS));
                assertWakeWasFast(start, slowFallbackMillis);
            }
            waitUntil(Duration.ofSeconds(2), () -> db.updateWatcherSubscriberCount() == 0,
                "worker subscription should close");

            Stream stream = db.stream("race-stream");
            CountDownLatch eventSeen = new CountDownLatch(1);
            try (StreamHandle ignored = stream.subscribe(
                event -> eventSeen.countDown(),
                SubscribeOptions.builder().pollTimeout(Duration.ofSeconds(30)).build()
            )) {
                waitUntil(Duration.ofSeconds(2), () -> db.updateWatcherSubscriberCount() == 1,
                    "stream subscriber should subscribe before waiting");
                long start = System.nanoTime();
                stream.publish("{\"event\":true}");
                assertTrue(eventSeen.await(2, TimeUnit.SECONDS));
                assertWakeWasFast(start, slowFallbackMillis);
            }
            waitUntil(Duration.ofSeconds(2), () -> db.updateWatcherSubscriberCount() == 0,
                "stream subscription should close");

            Queue results = db.queue("race-results");
            long id = results.enqueue("{\"work\":true}");
            Thread saver = new Thread(() -> {
                sleep(50);
                results.saveResult(id, "{\"ok\":true}");
            });
            long start = System.nanoTime();
            saver.start();
            assertEquals("{\"ok\":true}", results.waitResult(id, WaitOptions.builder()
                .timeout(Duration.ofSeconds(2))
                .fallbackPollInterval(Duration.ofSeconds(30))
                .build()));
            saver.join();
            assertWakeWasFast(start, slowFallbackMillis);
            assertEquals(0, db.updateWatcherSubscriberCount());
        }
    }

    @Test
    void autoWatcherUsesStablePragmaBackend() {
        try (Database db = openWithBackend("auto-pragma.db", WatcherBackend.AUTO)) {
            try (UpdateEvents updates = db.updateEvents()) {
                waitUntil(Duration.ofSeconds(2), () -> db.updateWatcherBackend() == WatcherBackend.PRAGMA_DATA_VERSION,
                    "AUTO watcher should select the stable PRAGMA backend");
                db.notify("auto-pragma", "{\"ok\":true}");
                assertTrue(updates.awaitUpdate(Duration.ofSeconds(2)));
                assertEquals(WatcherBackend.PRAGMA_DATA_VERSION, db.updateWatcherBackend());
            }
        }
    }

    @Test
    void explicitMmapShmWatcherDetectsCommitsIgnoresRollbacksAndSurvivesCheckpoint() {
        try (Database db = openWithBackend("mmap-semantics.db", WatcherBackend.MMAP_SHM)) {
            try (UpdateEvents updates = db.updateEvents()) {
                waitUntil(Duration.ofSeconds(2), () -> db.updateWatcherBackend() == WatcherBackend.MMAP_SHM,
                    "explicit mmap-shm watcher should become active");
                updates.awaitUpdate(Duration.ofMillis(100));

                assertThrows(RuntimeException.class, () -> db.transactionVoid(tx -> {
                    tx.notify("mmap", "{\"rolled_back\":true}");
                    throw new RuntimeException("rollback");
                }));
                assertFalse(updates.awaitUpdate(Duration.ofMillis(100)), "rolled-back transaction must not wake mmap watcher");

                db.notify("mmap", "{\"committed\":true}");
                assertTrue(updates.awaitUpdate(Duration.ofSeconds(2)), "committed notify should wake mmap watcher");

                db.query("PRAGMA wal_checkpoint(TRUNCATE)");
                updates.awaitUpdate(Duration.ofMillis(100));
                db.notify("mmap", "{\"after_checkpoint\":true}");
                assertTrue(updates.awaitUpdate(Duration.ofSeconds(2)), "mmap watcher should still wake after WAL checkpoint truncate");
            }
        }
    }

    @Test
    void explicitPragmaWatcherStillWorksAsFallbackBackend() {
        try (Database db = openWithBackend("pragma-backend.db", WatcherBackend.PRAGMA_DATA_VERSION)) {
            try (UpdateEvents updates = db.updateEvents()) {
                waitUntil(Duration.ofSeconds(2), () -> db.updateWatcherBackend() == WatcherBackend.PRAGMA_DATA_VERSION,
                    "explicit PRAGMA watcher should become active");
                db.notify("pragma", "{\"ok\":true}");
                assertTrue(updates.awaitUpdate(Duration.ofSeconds(2)));
                assertEquals(WatcherBackend.PRAGMA_DATA_VERSION, db.updateWatcherBackend());
            }
        }
    }

    @Test
    void rapidEnqueuesDoNotLoseWorkerWakeTicks() throws Exception {
        try (Database db = openWithLongFallback("rapid-worker.db")) {
            Queue queue = db.queue("rapid");
            int total = 100;
            CountDownLatch handled = new CountDownLatch(total);
            try (WorkerHandle ignored = queue.worker(
                "worker",
                job -> handled.countDown(),
                WorkerOptions.builder().idlePollInterval(Duration.ofSeconds(30)).build()
            )) {
                waitUntil(Duration.ofSeconds(2), () -> db.updateWatcherSubscriberCount() == 1,
                    "worker should subscribe before rapid enqueue stream");
                long start = System.nanoTime();
                Thread enqueuer = new Thread(() -> {
                    for (int i = 0; i < total; i++) {
                        queue.enqueue("{\"i\":" + i + "}");
                        sleep(1);
                    }
                });
                enqueuer.start();
                assertTrue(handled.await(5, TimeUnit.SECONDS));
                enqueuer.join();
                assertTrue(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofSeconds(5)) < 0,
                    "rapid drain should finish well under the 30s fallback interval");
            }
        }
    }

    @Test
    void manyListenersShareOneWatcherAndUnsubscribeCleanly() {
        try (Database db = openWithLongFallback("listener-resource.db")) {
            long baseline = updatePollThreadCount();
            List<Listener> listeners = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                listeners.add(db.listen("ch-" + i));
            }
            assertEquals(100, db.updateWatcherSubscriberCount());
            assertEquals(baseline + 1, updatePollThreadCount());

            for (Listener listener : listeners) {
                listener.close();
            }
            waitUntil(Duration.ofSeconds(2), () -> db.updateWatcherSubscriberCount() == 0,
                "listener subscriptions should be removed on close");
            assertEquals(baseline + 1, updatePollThreadCount());
        }
        waitUntil(Duration.ofSeconds(2), () -> updatePollThreadCount() == 0,
            "database close should stop the shared update watcher");
    }

    @Test
    void listenerChurnDoesNotLeakWatcherSubscribersOrThreads() {
        long baselineThreads = updatePollThreadCount();
        try (Database db = openWithLongFallback("listener-churn.db")) {
            for (int i = 0; i < 300; i++) {
                try (Listener listener = db.listen("churn-" + i)) {
                    assertEquals(1, db.updateWatcherSubscriberCount());
                    db.notify("churn-" + i, "{\"ok\":true}");
                    assertEquals("{\"ok\":true}", listener.next(Duration.ofSeconds(2)).orElseThrow().payloadJson());
                }
                assertEquals(0, db.updateWatcherSubscriberCount());
                assertEquals(baselineThreads + 1, updatePollThreadCount());
            }
        }
        waitUntil(Duration.ofSeconds(2), () -> updatePollThreadCount() == baselineThreads,
            "listener churn should not leak update watcher threads");
    }

    @Test
    void crossProcessListenerWakeLatencyStaysBelowUserVisibleBounds() throws Exception {
        Path dbPath = tmp.resolve("listener-latency.db");
        Path extension = NativeLoader.resolve(OpenOptions.defaults());
        try (Database db = Honker.open(dbPath, OpenOptions.builder().extensionPath(extension).build())) {
            db.transactionVoid(tx -> tx.execute("CREATE TABLE IF NOT EXISTS _warmup (i INTEGER)"));
        }

        List<Long> samples = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            Path ready = tmp.resolve("listener-" + i + ".ready");
            Path done = tmp.resolve("listener-" + i + ".done");
            Process child = startChild("listener-marker", dbPath, extension, ready, done);
            try {
                waitUntil(Duration.ofSeconds(5), () -> Files.isRegularFile(ready), "child listener should become ready");
                long start = System.nanoTime();
                try (Database db = Honker.open(dbPath, OpenOptions.builder()
                    .extensionPath(extension)
                    .fallbackPollInterval(Duration.ofSeconds(30))
                    .build())) {
                    db.notify("multiprocess-listen", "{\"sample\":" + i + "}");
                }
                waitUntil(Duration.ofSeconds(5), () -> Files.isRegularFile(done), "child listener should wake");
                samples.add(Duration.ofNanos(System.nanoTime() - start).toMillis());
                assertEquals("{\"sample\":" + i + "}", Files.readString(done, StandardCharsets.UTF_8));
                assertEquals(0, child.waitFor());
            } finally {
                child.destroyForcibly();
            }
        }
        samples.sort(Long::compareTo);
        assertTrue(percentile(samples, 0.50) < 50, "listener wake p50 was too slow: " + samples);
        assertTrue(percentile(samples, 0.90) < 250, "listener wake p90 was too slow: " + samples);
    }

    @Test
    void childJvmStreamSubscriberWakesWhenParentPublishes() throws Exception {
        Path dbPath = tmp.resolve("stream-multiprocess.db");
        Path extension = NativeLoader.resolve(OpenOptions.defaults());
        Path ready = tmp.resolve("stream-child.ready");
        Path done = tmp.resolve("stream-child.done");
        Process child = startChild("stream-marker", dbPath, extension, ready, done);
        try {
            waitUntil(Duration.ofSeconds(5), () -> Files.isRegularFile(ready), "child stream subscriber should become ready");
            try (Database db = Honker.open(dbPath, OpenOptions.builder()
                .extensionPath(extension)
                .fallbackPollInterval(Duration.ofSeconds(30))
                .build())) {
                db.stream("multiprocess-stream").publish("{\"from\":\"parent\"}");
            }
            waitUntil(Duration.ofSeconds(5), () -> Files.isRegularFile(done), "child stream subscriber should receive parent publish");
            assertEquals("{\"from\":\"parent\"}", Files.readString(done, StandardCharsets.UTF_8));
            assertEquals(0, child.waitFor());
        } finally {
            child.destroyForcibly();
        }
    }

    @Test
    void childJvmMmapListenerWakesWhenParentNotifies() throws Exception {
        Path dbPath = tmp.resolve("mmap-listener-multiprocess.db");
        Path extension = NativeLoader.resolve(OpenOptions.defaults());
        Path ready = tmp.resolve("mmap-listener-child.ready");
        Path done = tmp.resolve("mmap-listener-child.done");
        Process child = startChild("listener-mmap-marker", dbPath, extension, ready, done);
        try {
            waitUntil(Duration.ofSeconds(5), () -> Files.isRegularFile(ready), "child mmap listener should become ready");
            try (Database db = Honker.open(dbPath, OpenOptions.builder()
                .extensionPath(extension)
                .fallbackPollInterval(Duration.ofSeconds(30))
                .build())) {
                db.notify("multiprocess-listen", "{\"from\":\"parent\"}");
            }
            waitUntil(Duration.ofSeconds(5), () -> Files.isRegularFile(done), "child mmap listener should receive parent notify");
            assertEquals("{\"from\":\"parent\"}", Files.readString(done, StandardCharsets.UTF_8));
            assertEquals(0, child.waitFor());
        } finally {
            child.destroyForcibly();
        }
    }

    @Test
    void pythonStdlibSqliteInteropBothDirections() throws Exception {
        Path dbPath = tmp.resolve("interop.db");
        Path ext = NativeLoader.resolve(OpenOptions.defaults());
        String python = python();
        assumeTrue(python != null, "python unavailable");

        runPython(python, """
            import sqlite3, sys
            db, ext = sys.argv[1], sys.argv[2]
            con = sqlite3.connect(db)
            con.enable_load_extension(True)
            con.execute("SELECT load_extension(?, ?)", (ext, "sqlite3_honkerext_init"))
            con.execute("SELECT honker_bootstrap()")
            con.execute("SELECT honker_enqueue('interop', '{\\"from\\":\\"python\\"}', NULL, NULL, 0, 3, NULL)")
            con.commit()
            """, dbPath, ext);

        try (Database db = Honker.open(dbPath, OpenOptions.defaults())) {
            Job job = db.queue("interop").claimOne("java").orElseThrow();
            assertEquals("{\"from\":\"python\"}", job.payloadJson());
            assertTrue(job.ack());
            db.queue("interop").enqueue("{\"from\":\"java\"}");
        }

        runPython(python, """
            import json, sqlite3, sys
            db, ext = sys.argv[1], sys.argv[2]
            con = sqlite3.connect(db)
            con.enable_load_extension(True)
            con.execute("SELECT load_extension(?, ?)", (ext, "sqlite3_honkerext_init"))
            con.execute("SELECT honker_bootstrap()")
            row = con.execute("SELECT honker_claim_batch('interop', 'python', 1, 300)").fetchone()[0]
            jobs = json.loads(row)
            assert jobs[0]["payload"] == '{\\"from\\":\\"java\\"}', jobs
            con.execute("SELECT honker_ack(?, 'python')", (jobs[0]["id"],))
            con.commit()
            """, dbPath, ext);
    }

    @Test
    void workerWakesForDelayedJobDeadline() throws Exception {
        try (Database db = open()) {
            Queue q = db.queue("delayed");
            CountDownLatch seen = new CountDownLatch(1);
            try (WorkerHandle ignored = q.worker(
                "worker",
                job -> seen.countDown(),
                WorkerOptions.builder()
                    .idlePollInterval(Duration.ofSeconds(30))
                    .defaultRetryDelay(Duration.ZERO)
                    .build()
            )) {
                q.enqueue(
                    "{\"later\":true}",
                    EnqueueOptions.builder().delay(Duration.ofSeconds(1)).build()
                );
                assertTrue(seen.await(3, TimeUnit.SECONDS), "worker should wake at run_at deadline");
            }
        }
    }

    private Database open() {
        return Honker.open(tmp.resolve("app.db"), OpenOptions.builder()
            .fallbackPollInterval(Duration.ofMillis(2))
            .build());
    }

    private Database openWithLongFallback(String file) {
        return Honker.open(tmp.resolve(file), OpenOptions.builder()
            .fallbackPollInterval(Duration.ofSeconds(30))
            .watcherOptions(WatcherOptions.builder()
                .pollInterval(Duration.ofMillis(1))
                .subscriberBufferSize(1024)
            .build())
            .build());
    }

    private Database openWithBackend(String file, WatcherBackend backend) {
        return Honker.open(tmp.resolve(file), OpenOptions.builder()
            .fallbackPollInterval(Duration.ofSeconds(30))
            .watcherOptions(WatcherOptions.builder()
                .backend(backend)
                .pollInterval(Duration.ofMillis(1))
                .subscriberBufferSize(1024)
                .build())
            .build());
    }

    private static String python() {
        Path venv = Path.of(".venv/bin/python").toAbsolutePath().normalize();
        if (java.nio.file.Files.isExecutable(venv)) {
            return venv.toString();
        }
        return "python3";
    }

    private static void runPython(String python, String script, Path db, Path ext) throws Exception {
        Process p = new ProcessBuilder(python, "-c", script, db.toString(), ext.toString())
            .redirectErrorStream(true)
            .start();
        String out = new String(p.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        assertEquals(0, p.waitFor(), out);
    }

    private static Process startChild(String mode, Path db, Path ext, Path ready, Path done) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        return new ProcessBuilder(
            java,
            "-cp",
            System.getProperty("java.class.path"),
            "dev.honker.HonkerJvmChild",
            mode,
            db.toString(),
            ext.toString(),
            ready.toString(),
            done.toString()
        ).redirectErrorStream(true).start();
    }

    private static long count(Database db, String sql) {
        return db.query(sql).get(0).getLong("n");
    }

    private static long updatePollThreadCount() {
        return Thread.getAllStackTraces().keySet().stream()
            .filter(t -> t.getName().equals("honker-update-poll") && t.isAlive())
            .count();
    }

    private static long percentile(List<Long> sorted, double pct) {
        int index = (int) Math.ceil(sorted.size() * pct) - 1;
        return sorted.get(Math.max(0, Math.min(sorted.size() - 1, index)));
    }

    private static void assertWakeWasFast(long startNanos, long fallbackMillis) {
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - startNanos).toMillis();
        assertTrue(elapsedMillis < 1_000,
            "wake took " + elapsedMillis + "ms; this looks like fallback polling near " + fallbackMillis + "ms");
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HonkerException("sleep interrupted", e);
        }
    }

    private static void waitUntil(Duration timeout, BooleanSupplier condition, String message) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail(message);
            }
        }
        fail(message);
    }
}
