// End-to-end tests for the shared update wait — UpdateEvents.next()
// and waitForUpdateOrTimeout, which ClaimWaker, Listener, and
// Scheduler all wait through — against the real native binding.
// No mocks: the regression these tests guard is a native thread leak
// (one parked Tokio blocking thread per abandoned next()), which only
// exists behind the real UpdateEvents wrapper. Run with
// `node --test test/api.test.js` after `npm run build`.

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { setTimeout: delay } = require('node:timers/promises');

const lit = require('..');
const { createTempDb } = require('./helpers');

function tmpdb() {
  return createTempDb('honker-node-api-', lit.open.bind(lit));
}

const unhandled = [];
process.on('unhandledRejection', (err) => unhandled.push(err));

test('claim waker reuses one native update wait across poll timeouts', async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  const db = open(dbPath);
  try {
    const q = db.queue('q');
    const waker = q.claimWaker({ idlePollS: 0.001 }); // 1 ms polls
    const controller = new AbortController();
    const abortTimer = setTimeout(() => controller.abort(), 200);

    // Sample the shared native wait for the whole run: on an idle
    // database its identity must never change. A new promise means a
    // new native next() — one more parked OS thread per poll timeout.
    let firstPending = null;
    const samples = [];
    const sampler = setInterval(() => {
      const pending = waker._updates._pending;
      if (!pending) return;
      if (!firstPending) firstPending = pending;
      samples.push(pending === firstPending);
    }, 5);

    let result;
    try {
      result = await waker.next('worker', { signal: controller.signal });
    } finally {
      clearTimeout(abortTimer);
      clearInterval(sampler);
      waker.close();
    }

    assert.equal(result, null, 'abort with no jobs returns null');
    assert.ok(firstPending, 'a native wait was started');
    assert.ok(samples.length > 0, 'sampler observed the native wait');
    assert.ok(
      samples.every(Boolean),
      'poll timeouts must not start additional native waits'
    );
    assert.equal(
      waker._updates._waiters.size,
      0,
      'timed-out polls must unsubscribe from the shared wait'
    );
  } finally {
    cleanup();
  }
});

test('updates wake the shared wait across repeated idle polls', async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  const db = open(dbPath);
  try {
    const q = db.queue('q');
    // 60 s poll interval: only a real update can wake these waits.
    const waker = q.claimWaker({ idlePollS: 60 });
    try {
      for (const n of [1, 2]) {
        const pending = waker.next('worker');
        await delay(50);
        q.enqueue({ n });
        const job = await pending;
        assert.ok(job, `wait ${n} returned a job`);
        assert.equal(job.payload.n, n);
      }
      // After each wake the shared wait settles and is evicted, so the
      // next wait starts a fresh native next().
      assert.equal(waker._updates._pending, null);
    } finally {
      waker.close();
    }
  } finally {
    cleanup();
  }
});

test('close during an event-only wait returns promptly', async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  const db = open(dbPath);
  try {
    const q = db.queue('q');
    const waker = q.claimWaker({ idlePollS: null }); // no poll fallback
    const started = Date.now();
    const pending = waker.next('worker');
    setTimeout(() => waker.close(), 50);
    const result = await pending;
    assert.equal(result, null);
    assert.ok(Date.now() - started < 5000, 'close must unblock the wait');
  } finally {
    cleanup();
  }
});

test('concurrent UpdateEvents.next() calls share one native wait', async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  const db = open(dbPath);
  try {
    const updates = db.updateEvents();
    const first = updates.next();
    const second = updates.next();
    const shared = updates._pending;
    assert.ok(shared, 'a native wait was started');
    assert.equal(updates._waiters.size, 2, 'both callers wait on it');

    db.notify('chan', { wake: true }); // any commit resolves the wait
    await Promise.all([first, second]);
    assert.equal(updates._pending, null, 'settle evicts the shared wait');
    assert.equal(updates._waiters.size, 0, 'settle drains the waiters');
    updates.close();
  } finally {
    cleanup();
  }
});

test('racing UpdateEvents.next() against timeouts starts no extra native waits', async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  const db = open(dbPath);
  try {
    const updates = db.updateEvents();
    const raced = [];
    let firstPending = null;
    for (let i = 0; i < 50; i++) {
      // The timeout always wins on an idle database. Before the wait
      // was shared, each losing race abandoned a native next() — one
      // parked OS thread per iteration (issue #68 through public API).
      const attempt = updates.next();
      raced.push(attempt);
      await Promise.race([attempt, delay(1)]);
      const pending = updates._pending;
      assert.ok(pending, 'the shared native wait survives the race');
      if (!firstPending) firstPending = pending;
      assert.equal(
        pending,
        firstPending,
        `iteration ${i} must reuse the same native wait`
      );
    }
    updates.close();
    // Abandoned race losers settle (here: reject on close) — drain
    // them so nothing surfaces as an unhandled rejection.
    await Promise.all(raced.map((p) => p.catch(() => undefined)));
  } finally {
    cleanup();
  }
});

test('UpdateEvents.next() rejects when closed mid-wait', async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  const db = open(dbPath);
  try {
    const updates = db.updateEvents();
    const pending = updates.next();
    await delay(50); // let the native wait park
    updates.close();
    await assert.rejects(pending);
  } finally {
    cleanup();
  }
});

test('listener wakes on notify after fallback poll timeouts', async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  const db = open(dbPath);
  try {
    const listener = db.listen('chan', { fallbackPollS: 0.005 });
    try {
      const pending = listener.next();
      await delay(60); // several fallback timeouts inside the wait
      db.notify('chan', { hello: 1 });
      const { value, done } = await pending;
      assert.equal(done, false);
      assert.deepEqual(value.payload, { hello: 1 });
    } finally {
      listener.close();
    }
  } finally {
    cleanup();
  }
});

test('no unhandled promise rejections from shared waits', async () => {
  // Last test in the file (node --test runs top-level tests in order):
  // gives any stray rejection from the scenarios above time to surface.
  await delay(100);
  assert.deepEqual(unhandled, []);
});
