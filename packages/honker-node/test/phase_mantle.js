// Tests for Phase Mantle: schedule lifecycle (pause/resume/list/update)
// and queue cancel/getJob.

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const honker = require('..');
const { createTempDb } = require('./helpers');

function tmpdb() {
  return createTempDb('honker-mantle-', honker.open.bind(honker));
}

// ---------- schedule lifecycle ----------

test('schedule list round-trips all fields', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    const sched = new honker.Scheduler(db);
    sched.add({ name: 'daily-recap', queue: 'emails', cron: '0 9 * * *', payload: { foo: 1 }, priority: 3 });
    sched.add({ name: 'hourly-sync', queue: 'syncs', cron: '@every 1h', payload: null });

    const rows = sched.list();
    const byName = Object.fromEntries(rows.map((r) => [r.name, r]));
    assert.deepEqual(Object.keys(byName).sort(), ['daily-recap', 'hourly-sync']);
    assert.equal(byName['daily-recap'].queue, 'emails');
    assert.equal(byName['daily-recap'].priority, 3);
    assert.deepEqual(JSON.parse(byName['daily-recap'].payload), { foo: 1 });
    assert.equal(byName['daily-recap'].enabled, true);
    assert.ok(byName['daily-recap'].next_fire_at > 0);
  } finally {
    cleanup();
  }
});

test('pause/resume idempotent', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    const sched = new honker.Scheduler(db);
    sched.add({ name: 'a', queue: 'q', cron: '0 9 * * *', payload: null });

    assert.equal(sched.pause('a'), true);
    assert.equal(sched.pause('a'), false); // already paused
    assert.equal(sched.pause('missing'), false);

    const paused = sched.list().find((s) => s.name === 'a');
    assert.equal(paused.enabled, false);

    assert.equal(sched.resume('a'), true);
    assert.equal(sched.resume('a'), false); // already enabled

    const enabled = sched.list().find((s) => s.name === 'a');
    assert.equal(enabled.enabled, true);
  } finally {
    cleanup();
  }
});

test('update mutates fields and recomputes next_fire_at on cron change', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    const sched = new honker.Scheduler(db);
    sched.add({ name: 't', queue: 'q', cron: '0 9 * * *', payload: { v: 1 }, priority: 0 });

    assert.equal(sched.update('t', { payload: { v: 99 }, priority: 5 }), true);
    const row = sched.list().find((s) => s.name === 't');
    assert.deepEqual(JSON.parse(row.payload), { v: 99 });
    assert.equal(row.priority, 5);

    const before = row.next_fire_at;
    assert.equal(sched.update('t', { cron: '*/5 * * * *' }), true);
    const after = sched.list().find((s) => s.name === 't');
    assert.equal(after.cron_expr, '*/5 * * * *');
    assert.notEqual(after.next_fire_at, before);

    assert.equal(sched.update('missing', { payload: {} }), false);
  } finally {
    cleanup();
  }
});

// ---------- queue cancel / getJob ----------

test('queue.getJob returns row, cancel removes pending', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    const q = db.queue('emails');

    const tx = db.transaction();
    const jid = q.enqueueTx(tx, { to: 'alice@example.com' });
    tx.commit();

    const row = q.getJob(jid);
    assert.equal(row.queue, 'emails');
    assert.equal(row.state, 'pending');
    assert.deepEqual(JSON.parse(row.payload), { to: 'alice@example.com' });
    assert.equal(row.id, jid);

    assert.equal(q.cancel(jid), true);
    assert.equal(q.cancel(jid), false); // idempotent
    assert.equal(q.getJob(jid), null);
    assert.equal(q.claimOne('worker-1'), null);
  } finally {
    cleanup();
  }
});

test('cancel of processing job invalidates ack', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    const q = db.queue('emails');

    const tx = db.transaction();
    const jid = q.enqueueTx(tx, { to: 'x' });
    tx.commit();

    const job = q.claimOne('worker-1');
    assert.equal(job.id, jid);

    assert.equal(q.cancel(jid), true);
    // Worker's ack returns false — same as expired claim.
    assert.equal(job.ack(), false);
  } finally {
    cleanup();
  }
});
