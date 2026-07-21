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

test('poll timeouts remove their abort listeners', async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  const db = open(dbPath);
  try {
    const q = db.queue('q');
    const waker = q.claimWaker({ idlePollS: 0.001 });
    const controller = new AbortController();
    const abortTimer = setTimeout(() => controller.abort(), 200);

    // Count listeners on the real AbortSignal. Every poll iteration
    // subscribes to 'abort'; losing the race to the timeout must
    // remove that listener again (issue #67).
    let added = 0;
    let removed = 0;
    const signal = controller.signal;
    const realAdd = signal.addEventListener.bind(signal);
    const realRemove = signal.removeEventListener.bind(signal);
    signal.addEventListener = (...args) => {
      added += 1;
      return realAdd(...args);
    };
    signal.removeEventListener = (...args) => {
      removed += 1;
      return realRemove(...args);
    };

    try {
      await waker.next('worker', { signal });
    } finally {
      clearTimeout(abortTimer);
      waker.close();
    }

    assert.ok(added >= 2, `expected several poll iterations, saw ${added}`);
    // The listener whose abort fires is auto-removed by the platform
    // (once: true) without a removeEventListener call — allow that one.
    assert.ok(
      added - removed <= 1,
      `${added - removed} abort listeners leaked across poll timeouts`
    );
  } finally {
    cleanup();
  }
});

test('watcher death degrades wait loops to poll cadence', {
  // Same trigger as the watcher-death contract tests: atomically
  // replace the db file so the watcher thread panics and dies.
  skip: process.platform === 'win32' ? 'rename-over-open denied on Windows' : false,
}, async () => {
  const fs = require('node:fs');
  const { path: dbPath, open, cleanup } = tmpdb();
  const db = open(dbPath);
  try {
    const tx = db.transaction();
    tx.execute('CREATE TABLE _warm (i INTEGER)');
    tx.commit();
    await delay(100);

    const updates = db.updateEvents();
    const replacement = `${dbPath}.replacement`;
    fs.writeFileSync(replacement, '');
    fs.renameSync(replacement, dbPath);

    // Public contract: next() rejects once the watcher dies (an
    // ordinary wake may come first — keep awaiting until rejection).
    let raised = false;
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline && !raised) {
      const result = await updates.next().then(() => null, (e) => e);
      if (result instanceof Error) raised = true;
    }
    assert.ok(raised, 'next() must reject after watcher death');
    assert.ok(updates._dead, 'the death is recorded on the subscription');

    // The wait loops must fall back to poll cadence. Count real
    // claimOne calls over one abort window: poll cadence (200 ms)
    // means ~3 calls; a hot loop on instantly-rejecting waits means
    // thousands. After the replace the db itself may or may not still
    // answer (the panic message says to reopen) — that's out of scope
    // here, so the loop's own SQL calls are made error-tolerant; only
    // their cadence is under test.
    const q = db.queue('q');
    const tolerant = (fn, fallback) => (...args) => {
      try {
        return fn(...args);
      } catch {
        return fallback;
      }
    };
    q._nextClaimAt = tolerant(q._nextClaimAt.bind(q), null);
    let claims = 0;
    const realClaimOne = q.claimOne.bind(q);
    q.claimOne = tolerant((workerId) => {
      claims += 1;
      return realClaimOne(workerId);
    }, null);
    const waker = q.claimWaker({ idlePollS: 0.2 });
    const controller = new AbortController();
    const abortTimer = setTimeout(() => controller.abort(), 500);
    try {
      const result = await waker.next('worker', { signal: controller.signal });
      assert.equal(result, null);
    } finally {
      clearTimeout(abortTimer);
      waker.close();
    }
    assert.ok(
      claims <= 8,
      `${claims} claimOne calls in 500 ms — a hot loop would make thousands`
    );
  } finally {
    cleanup();
  }
});

test('event-only waker still exits after watcher death', {
  // Regression: a waker with idlePollS null parked before the watcher
  // dies re-waits on the dead path, where an event-only wait could
  // never settle — close() could not wake it (hang). The dead path
  // degrades null timeouts to a 1 s cadence instead.
  skip: process.platform === 'win32' ? 'rename-over-open denied on Windows' : false,
  timeout: 15000,
}, async () => {
  const fs = require('node:fs');
  const { path: dbPath, open, cleanup } = tmpdb();
  const db = open(dbPath);
  try {
    const tx = db.transaction();
    tx.execute('CREATE TABLE _warm (i INTEGER)');
    tx.commit();
    await delay(100);

    const q = db.queue('q');
    // The replace may leave the db itself broken (disk I/O error on
    // later reads) — out of scope here; only the wait cadence is
    // under test, so the loop's SQL calls are made error-tolerant.
    const tolerant = (fn, fallback) => (...args) => {
      try {
        return fn(...args);
      } catch {
        return fallback;
      }
    };
    q._nextClaimAt = tolerant(q._nextClaimAt.bind(q), null);
    q.claimOne = tolerant(q.claimOne.bind(q), null);
    const waker = q.claimWaker({ idlePollS: null }); // event-only, no signal
    const parked = waker.next('worker');
    await delay(100); // waker is parked in its event wait

    const replacement = `${dbPath}.replacement`;
    fs.writeFileSync(replacement, '');
    fs.renameSync(replacement, dbPath);

    // Wait for the death to reach the waker's own subscription.
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline && waker._updates._dead == null) {
      await delay(50);
    }
    assert.ok(waker._updates._dead, 'watcher death reached the waker');

    const started = Date.now();
    waker.close();
    const result = await parked;
    assert.equal(result, null);
    assert.ok(
      Date.now() - started < 5000,
      'close() must unblock an event-only wait on a dead watcher'
    );
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
