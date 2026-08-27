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
      db.configureQueueEvents({ maxEvents: 100, includePayload: true }),
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

test('queue events cover retry, completion, cancellation, filtering, and retention', () => {
  const { path, open, cleanup } = tmpdb();
  let db;
  let globalFeed;
  let alphaFeed;
  try {
    db = open(path);
    db.configureQueueEvents({ maxEvents: 100, includePayload: false });

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
    assert.equal(failedEvent.error, 'permanent');
    assert.equal(failedEvent.workerId, 'worker-fail');
    assert.equal(failedEvent.attempts, 1);
    assert.ok(lifecycle.every((event) => event.payload === undefined));

    db.configureQueueEvents({ maxEvents: 3, includePayload: true });
    const alpha = db.queue('alpha');
    const beta = db.queue('beta');
    alpha.enqueue({ sequence: 1 });
    beta.enqueue({ sequence: 2 });
    alpha.enqueue({ sequence: 3 });
    beta.enqueue({ sequence: 4 });
    alpha.enqueue({ sequence: 5 });

    globalFeed.close();
    globalFeed = db.queueEvents({ fromOffset: 0 });
    const retained = globalFeed.readSince(0, 100);
    assert.equal(retained.length, 3);
    assert.deepEqual(retained.map((event) => event.payload.sequence), [3, 4, 5]);

    alphaFeed = db.queueEvents({ queue: 'alpha', fromOffset: 0 });
    assert.deepEqual(
      alphaFeed.readSince(0, 100).map((event) => event.payload.sequence),
      [3, 5],
    );

    const finalOffset = retained.at(-1).offset;
    db.configureQueueEvents({ enabled: false, maxEvents: 3 });
    alpha.enqueue({ sequence: 6 });
    assert.deepEqual(globalFeed.readSince(finalOffset, 100), []);
  } finally {
    try { alphaFeed?.close(); } catch {}
    try { globalFeed?.close(); } catch {}
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
