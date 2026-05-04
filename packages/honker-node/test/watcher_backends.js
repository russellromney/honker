// Cross-language proof for the experimental watcher backends, Node side.
//
// Mirrors tests/test_watcher_backends.py: opens a Database with each
// backend, drives a workload, verifies updateEvents() fires through the
// public Node API. Builds without the corresponding Cargo feature reject
// explicit `"kernel"` / `"shm"` requests, so when an experimental case
// runs fallback polling cannot make it pass.
//
// A control assertion runs the same workload against the default polling
// backend so a regression in the experimental path stands out from
// test-environment flakiness.

const test = require('node:test');
const assert = require('node:assert/strict');

const lit = require('..');
const { createTempDb } = require('./helpers');

function tmpdb() {
  return createTempDb('honker-node-watchers-', lit.open.bind(lit));
}

function openOrSkipBackend(t, open, dbPath, backend) {
  try {
    return open(dbPath, undefined, backend);
  } catch (err) {
    if ((backend === 'kernel' || backend === 'shm') && /requires the/.test(String(err))) {
      t.skip(String(err));
      return null;
    }
    throw err;
  }
}

async function driveCommitsAndCountWakes(db, n, spacingMs) {
  // Force WAL to exist before subscribing so the kernel-watcher can
  // attach a per-file watch on the -wal at startup.
  {
    const tx = db.transaction();
    tx.execute('CREATE TABLE IF NOT EXISTS t (x INTEGER)');
    tx.commit();
  }
  await new Promise((r) => setTimeout(r, 50));

  const ev = db.updateEvents();
  let counted = 0;
  let stop = false;
  // Track when the consumer has actually entered its first `await ev.next()`
  // so we don't race the first commit against the napi runtime startup.
  let consumerReady;
  const ready = new Promise((r) => { consumerReady = r; });
  const consumer = (async () => {
    let firstIter = true;
    while (!stop) {
      try {
        if (firstIter) {
          // Signal readiness on the same microtask we begin awaiting.
          consumerReady();
          firstIter = false;
        }
        await ev.next();
        counted += 1;
      } catch {
        return;
      }
    }
  })();
  await ready;
  // One more tick so the napi blocking task is actually scheduled.
  await new Promise((r) => setTimeout(r, 20));

  for (let i = 0; i < n; i++) {
    const tx = db.transaction();
    tx.execute('INSERT INTO t (x) VALUES (?)', [i]);
    tx.commit();
    await new Promise((r) => setTimeout(r, spacingMs));
  }

  // Wait long enough for the slowest backend's safety net (500 ms) plus
  // event delivery and the napi-rs Promise round-trip latency.
  await new Promise((r) => setTimeout(r, 800));
  stop = true;
  ev.close();
  try { await consumer; } catch {}
  return counted;
}

for (const backend of [null, 'kernel', 'shm']) {
  const label = backend === null ? 'polling (default)' : backend;
  test(`watcherBackend=${label} detects commits`, async (t) => {
    const { path: dbPath, open, cleanup } = tmpdb();
    let db;
    try {
      db = openOrSkipBackend(t, open, dbPath, backend);
      if (!db) return;
      const n = 4;
      const counted = await driveCommitsAndCountWakes(db, n, 30);
      const minWakes = backend === 'kernel' && process.platform === 'win32' ? 1 : n;
      assert.ok(
        counted >= minWakes,
        `watcherBackend=${label}: only ${counted} wakes for ${n} commits`,
      );
      const maxWakes = backend === 'kernel' ? n * 4 : n + 2;
      assert.ok(
        counted <= maxWakes,
        `watcherBackend=${label}: ${counted} wakes for ${n} commits exceeds bound ${maxWakes}`,
      );
    } finally {
      cleanup();
    }
  });
}

test('unknown watcherBackend throws', () => {
  const { path: dbPath, cleanup } = tmpdb();
  try {
    for (const backend of ['bogus', 'KERNEL', ' polling ']) {
      assert.throws(
        () => lit.open(dbPath, undefined, backend),
        /unknown watcher backend/,
      );
    }
  } finally {
    cleanup();
  }
});

test('polling watcherBackend aliases open', () => {
  for (const backend of [undefined, null, 'poll', 'polling']) {
    const { path: dbPath, cleanup } = tmpdb();
    try {
      const db = lit.open(dbPath, undefined, backend);
      db.queue('alias-check');
      db.close();
    } finally {
      cleanup();
    }
  }
});
