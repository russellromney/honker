'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const honker = require('..');
const { createTempDb } = require('./helpers');

function tmpdb() {
  return createTempDb('honker-queue-events-', honker.open.bind(honker));
}

test('queue events are opt-in and follow successful committed transitions', async () => {
  const { path, open, cleanup } = tmpdb();
  let db;
  let feed;
  try {
    db = open(path);
    const queue = db.queue('emails', { maxAttempts: 3 });

    queue.enqueue({ disabled: true });
    feed = db.queueEvents({ fromOffset: 0 });
    assert.deepEqual(feed.readSince(0), []);
    feed.close();

    assert.equal(
      db.configureQueueEvents({ retentionTarget: 100, includePayload: true }),
      true,
    );
    feed = db.queueEvents({ fromOffset: 0 });

    const rolledBack = db.transaction();
    queue.enqueueTx(rolledBack, { rolledBack: true });
    rolledBack.rollback();
    assert.deepEqual(feed.readSince(0), []);

    const first = feed.next();
    const id = queue.enqueue({ to: 'alice@example.com' }, { priority: 7 });
    const enqueued = await first;
    assert.equal(enqueued.done, false);
    assert.equal(enqueued.value.type, 'enqueued');
    assert.equal(enqueued.value.jobId, id);
    assert.deepEqual(enqueued.value.payload, { to: 'alice@example.com' });

    const claimedOnce = queue.claimOne('worker-1');
    assert.equal(claimedOnce.ack(), true);
    assert.deepEqual(
      feed.readSince(enqueued.value.offset).map((event) => event.type),
      ['claimed', 'completed'],
    );
  } finally {
    try { feed?.close(); } catch {}
    cleanup();
  }
});

test('pre-event schemas fail feed construction with the typed disabled error', () => {
  const { path, open, cleanup } = tmpdb();
  let db;
  let feed;
  try {
    db = open(path);
    const legacySchema = db.transaction();
    legacySchema.execute('DROP TABLE _honker_queue_event_config');
    legacySchema.commit();

    assert.throws(
      () => db.queueEvents(),
      (error) => error instanceof honker.QueueEventsDisabledError &&
        error.code === 'HONKER_QUEUE_EVENTS_DISABLED',
    );
    assert.throws(
      () => db.queueEventListener(),
      (error) => error instanceof honker.QueueEventsDisabledError &&
        error.code === 'HONKER_QUEUE_EVENTS_DISABLED',
    );

    // Compatibility remains symmetric: queue mutations still work without
    // the opt-in event schema.
    assert.ok(db.queue('legacy').enqueue({ compatible: true }) > 0);

    // Follow the typed error's recovery instruction. Configuration restores
    // the canonical schema, and subsequent lifecycle events are observable.
    assert.equal(db.configureQueueEvents({ enabled: true }), true);
    feed = db.queueEvents({ fromOffset: 0 });
    const id = db.queue('legacy').enqueue({ emits: true });
    assert.deepEqual(
      feed.readSince(0).map((event) => [event.type, event.jobId]),
      [['enqueued', id]],
    );
  } finally {
    try { feed?.close(); } catch {}
    cleanup();
  }
});

test('queue events cover retry, completion, cancellation, filtering, and retention', () => {
  const { path, open, cleanup } = tmpdb();
  let db;
  let globalFeed;
  let alphaFeed;
  try {
    db = open(path);
    db.configureQueueEvents({ retentionTarget: 100, includePayload: false });

    const queue = db.queue('jobs', { maxAttempts: 3 });
    const id = queue.enqueue({ secret: 'not retained' });
    const first = queue.claimOne('worker-1');
    assert.equal(first.retry(0, 'temporary'), true);
    const second = queue.claimOne('worker-2');
    assert.equal(second.ack(), true);
    assert.equal(second.ack(), false);

    const cancelled = queue.enqueue({ cancel: true });
    assert.equal(queue.cancel(cancelled), true);
    assert.equal(queue.cancel(cancelled), false);

    const failed = queue.enqueue({ fail: true });
    const failedJob = queue.claimOne('worker-fail');
    assert.equal(failedJob.fail('permanent'), true);

    globalFeed = db.queueEvents({ fromOffset: 0 });
    const lifecycle = globalFeed.readSince(0, 100);
    assert.deepEqual(
      lifecycle.filter((event) => event.jobId === id).map((event) => event.type),
      ['enqueued', 'claimed', 'retry_scheduled', 'claimed', 'completed'],
    );
    assert.equal(
      lifecycle.filter((event) => event.jobId === cancelled).at(-1).type,
      'cancelled',
    );
    const failedEvent = lifecycle
      .filter((event) => event.jobId === failed)
      .at(-1);
    assert.equal(failedEvent.type, 'dead_lettered');
    assert.equal(failedEvent.reason, 'explicit_failure');
    assert.equal(failedEvent.error, 'permanent');
    assert.equal(failedEvent.workerId, 'worker-fail');
    assert.equal(failedEvent.attempts, 1);
    assert.ok(lifecycle.every((event) => event.payload === undefined));

    db.configureQueueEvents({ retentionTarget: 3, includePayload: true });
    const alpha = db.queue('alpha');
    const beta = db.queue('beta');
    alpha.enqueue({ sequence: 1 });
    db.stream('app').publish({ unrelated: 1 });
    beta.enqueue({ sequence: 2 });
    db.stream('app').publish({ unrelated: 2 });
    alpha.enqueue({ sequence: 3 });
    db.stream('app').publish({ unrelated: 3 });
    beta.enqueue({ sequence: 4 });
    db.stream('app').publish({ unrelated: 4 });
    alpha.enqueue({ sequence: 5 });

    globalFeed.close();
    globalFeed = db.queueEvents();
    const retained = globalFeed.readSince(globalFeed.lastOffset, 100);
    assert.equal(retained.length, 3);
    assert.deepEqual(retained.map((event) => event.payload.sequence), [3, 4, 5]);

    const staleFeed = db.queueEvents({ fromOffset: 0 });
    assert.throws(
      () => staleFeed.readSince(0, 100),
      (error) => {
        assert.ok(error instanceof honker.QueueEventOffsetExpiredError);
        assert.equal(error.code, 'HONKER_QUEUE_EVENT_OFFSET_EXPIRED');
        assert.equal(error.requestedOffset, 0);
        assert.ok(error.trimmedThroughOffset > 0);
        assert.equal(error.oldestAvailableOffset, retained[0].offset);
        return true;
      },
    );
    staleFeed.close();

    alphaFeed = db.queueEvents({ queue: 'alpha' });
    assert.deepEqual(
      alphaFeed.readSince(alphaFeed.lastOffset, 100).map((event) => event.payload.sequence),
      [3, 5],
    );

    const finalOffset = retained.at(-1).offset;
    db.configureQueueEvents({ enabled: false, retentionTarget: 3 });
    alpha.enqueue({ sequence: 6 });
    assert.deepEqual(globalFeed.readSince(finalOffset, 100), []);
  } finally {
    try { alphaFeed?.close(); } catch {}
    try { globalFeed?.close(); } catch {}
    cleanup();
  }
});

test('queue event retention remains bounded across short-lived connections', () => {
  const { path, open, cleanup } = tmpdb();
  let reader;
  let feed;
  try {
    const configured = open(path);
    configured.configureQueueEvents({ retentionTarget: 20 });
    configured.close();

    // Model request-scoped clients and separate worker processes. Each enqueue
    // uses a fresh native database handle and a fresh Rust config cache.
    for (let sequence = 0; sequence < 45; sequence++) {
      const writer = open(path);
      writer.queue('short-lived-writers').enqueue({ sequence });
      writer.close();
    }

    reader = open(path);
    feed = reader.queueEvents();
    assert.ok(feed.lastOffset > 0);
    const retained = feed.readSince(feed.lastOffset, 100);
    assert.equal(retained.length, 21);
    assert.deepEqual(
      retained.map((event) => event.jobId),
      [...retained.map((event) => event.jobId)].sort((a, b) => a - b),
    );
    assert.throws(
      () => feed.readSince(0, 100),
      (error) => error instanceof honker.QueueEventOffsetExpiredError &&
        error.trimmedThroughOffset === feed.lastOffset,
    );
  } finally {
    try { feed?.close(); } catch {}
    cleanup();
  }
});

test('queue event listener provides live EventEmitter ergonomics over the durable feed', async () => {
  const { path, open, cleanup } = tmpdb();
  let db;
  let listener;
  let replayListener;
  let gapListener;
  try {
    db = open(path);
    assert.throws(
      () => db.queueEventListener(),
      (error) => error instanceof honker.QueueEventsDisabledError &&
        error.code === 'HONKER_QUEUE_EVENTS_DISABLED',
    );

    db.configureQueueEvents({ includePayload: true });
    db.queue('listener').enqueue({ sequence: 1 });
    assert.throws(
      () => db.queueEventListener({ startAt: 'middle' }),
      /startAt must be 'latest' or 'oldest'/,
    );
    assert.throws(
      () => db.queueEventListener({ fromOffset: 0, startAt: 'oldest' }),
      /Specify either fromOffset or startAt/,
    );

    listener = db.queueEventListener({ queue: 'listener' });
    const allLiveEvents = [];
    listener.on('event', (item) => allLiveEvents.push(item));
    const live = new Promise((resolve, reject) => {
      listener.once('enqueued', resolve);
      listener.once('error', reject);
    });
    const liveId = db.queue('listener').enqueue({ sequence: 2 });
    const event = await live;
    assert.equal(event.jobId, liveId);
    assert.equal(event.payload.sequence, 2);
    assert.deepEqual(allLiveEvents, [event]);

    replayListener = db.queueEventListener({ queue: 'listener', startAt: 'oldest' });
    const replayed = new Promise((resolve, reject) => {
      replayListener.once('enqueued', resolve);
      replayListener.once('error', reject);
    });
    assert.equal((await replayed).payload.sequence, 1);

    let closeCount = 0;
    listener.on('close', () => closeCount++);
    listener.close();
    listener.close();
    assert.equal(closeCount, 1);
    replayListener.close();
    db.configureQueueEvents({ retentionTarget: 1, includePayload: false });
    db.queue('listener-gap').enqueue({ sequence: 3 });
    db.queue('listener-gap').enqueue({ sequence: 4 });
    gapListener = db.queueEventListener({ fromOffset: 0 });
    const gap = new Promise((resolve) => gapListener.once('error', resolve));
    const gapError = await gap;
    assert.ok(gapError instanceof honker.QueueEventOffsetExpiredError);
    assert.equal(gapError.requestedOffset, 0);
  } finally {
    try { gapListener?.close(); } catch {}
    try { replayListener?.close(); } catch {}
    try { listener?.close(); } catch {}
    cleanup();
  }
});

test('queue event iterators close on abort, early return, and pending close', async () => {
  const { path, open, cleanup } = tmpdb();
  let db;
  let abortedFeed;
  let breakFeed;
  let closedFeed;
  let errorFeed;
  try {
    db = open(path);
    db.configureQueueEvents();

    const controller = new AbortController();
    abortedFeed = db.queueEvents({ signal: controller.signal, fallbackPollS: null });
    const abortedNext = abortedFeed.next();
    controller.abort();
    assert.deepEqual(await abortedNext, { done: true, value: undefined });
    assert.equal(abortedFeed._updates._closed, true);

    breakFeed = db.queueEvents();
    db.queue('iterator-cleanup').enqueue({ sequence: 1 });
    for await (const event of breakFeed) {
      assert.equal(event.type, 'enqueued');
      break;
    }
    assert.equal(breakFeed._updates._closed, true);

    closedFeed = db.queueEvents({
      fromOffset: breakFeed.lastOffset,
      fallbackPollS: null,
    });
    const closedNext = closedFeed.next();
    closedFeed.close();
    assert.deepEqual(await closedNext, { done: true, value: undefined });
    assert.equal(closedFeed._updates._closed, true);

    errorFeed = db.queueEvents({ fromOffset: breakFeed.lastOffset });
    const dropStream = db.transaction();
    dropStream.execute('DROP TABLE _honker_stream');
    dropStream.commit();
    await assert.rejects(errorFeed.next(), /no such table: _honker_stream/);
    assert.equal(errorFeed._updates._closed, true);
  } finally {
    try { errorFeed?.close(); } catch {}
    try { closedFeed?.close(); } catch {}
    try { breakFeed?.close(); } catch {}
    try { abortedFeed?.close(); } catch {}
    cleanup();
  }
});

test('the internal queue event stream topic is reserved', () => {
  const { path, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(path);
    assert.throws(
      () => db.stream('_honker:queue-events:v1').publish({ forged: true }),
      /reserved for queue lifecycle events/,
    );

    db.configureQueueEvents({ includePayload: true });
    const id = db.queue('reserved-topic').enqueue({ legitimate: true });
    const [event] = db.queueEvents({ queue: 'reserved-topic' }).readSince(0);
    assert.equal(event.jobId, id);
    assert.deepEqual(event.payload, { legitimate: true });
  } finally {
    cleanup();
  }
});
