// Tests for Phase Mantle: schedule lifecycle (pause/resume/list/update)
// and queue cancel/getJob.

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const honker = require('..');
const { createTempDb, knownBug } = require('./helpers');

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

test('update with no fields is a no-op and returns false', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    const sched = new honker.Scheduler(db);
    sched.add({ name: 't', queue: 'q', cron: '0 9 * * *', payload: { v: 1 } });
    const before = sched.list();
    assert.equal(sched.update('t'), false);
    assert.deepEqual(sched.list(), before);
  } finally {
    cleanup();
  }
});

test('update can write JSON null distinct from omitted payload', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    const sched = new honker.Scheduler(db);
    sched.add({ name: 't', queue: 'q', cron: '0 9 * * *', payload: { v: 1 } });
    // Omitted payload — leaves the row alone.
    sched.update('t', { priority: 7 });
    let row = sched.list().find((s) => s.name === 't');
    assert.deepEqual(JSON.parse(row.payload), { v: 1 });
    assert.equal(row.priority, 7);
    // payload: null — explicitly write JSON null.
    sched.update('t', { payload: null });
    row = sched.list().find((s) => s.name === 't');
    assert.equal(JSON.parse(row.payload), null);
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

test('paused schedule does not emit on tick', async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    const sched = new honker.Scheduler(db);
    // schedule with next_fire_at in the past
    sched.add({ name: 'due', queue: 'emails', cron: '@every 1s', payload: { x: 1 } });
    await new Promise((r) => setTimeout(r, 1100));
    sched.pause('due');

    const future = Math.floor(Date.now() / 1000) + 5;
    const fires = sched.tick(future);
    assert.equal(fires.length, 0, `paused schedule must not emit; got ${JSON.stringify(fires)}`);

    // Resume and tick again — now it fires.
    sched.resume('due');
    const fires2 = sched.tick(future);
    assert.ok(fires2.length >= 1, `resumed schedule should emit; got ${JSON.stringify(fires2)}`);
  } finally {
    cleanup();
  }
});

test('queue.getJob misses after ack (separate from cancel)', () => {
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
    assert.equal(job.ack(), true);
    // After ack the row is gone — get_job misses just like after cancel.
    assert.equal(q.getJob(jid), null);
  } finally {
    cleanup();
  }
});

// ---------- user-facing negatives for the Phase Clemente core changes ----------

test('queue.cancel of another queue\'s job returns false [known bug #186]', async (t) => {
  const { path: dbPath, open, cleanup } = tmpdb();
  try {
    const db = open(dbPath);
    const emails = db.queue('emails');
    const sms = db.queue('sms');
    const smsId = sms.enqueue({ to: '+1555' });
    await knownBug(
      t,
      'Queue.cancel calls honker_cancel(id); core has the queue-scoped honker_cancel(queue, id) since #155 (#134) but the Node binding has not switched (#186)',
      () => {
        assert.equal(emails.cancel(smsId), false);
        assert.notEqual(sms.getJob(smsId), null);
      },
    );
  } finally {
    cleanup();
  }
});

test('maxAttempts below 1 is rejected at enqueue with a clear error', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  try {
    const db = open(dbPath);
    for (const bad of [0, -1]) {
      // Declaring the queue does not validate; the first enqueue does.
      const q = db.queue(`bad${bad}`, { maxAttempts: bad });
      assert.throws(() => q.enqueue({ n: 1 }), /max_attempts must be at least 1/);
      const tx = db.transaction();
      assert.throws(() => q.enqueueTx(tx, { n: 1 }), /max_attempts must be at least 1/);
      tx.rollback();
      assert.equal(
        db.query('SELECT COUNT(*) AS c FROM _honker_live')[0].c,
        0,
        'a rejected enqueue writes nothing',
      );
    }
  } finally {
    cleanup();
  }
});

test('scheduler rejects maxAttempts below 1 and invalid schedules', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  try {
    const db = open(dbPath);
    const sched = new honker.Scheduler(db);
    assert.throws(
      () => sched.add({ name: 'z', queue: 'q', cron: '@every 1s', payload: null, maxAttempts: 0 }),
      /max_attempts must be at least 1/,
    );
    sched.add({ name: 'ok', queue: 'q', cron: '@every 1s', payload: null });
    assert.throws(() => sched.update('ok', { maxAttempts: 0 }), /max_attempts must be at least 1/);
    for (const cron of ['61 * * * *', 'not a schedule', '@every 0s']) {
      assert.throws(
        () => sched.add({ name: `bad-${cron}`, queue: 'q', cron, payload: null }),
        Error,
        `cron ${JSON.stringify(cron)} must be rejected`,
      );
    }
    assert.deepEqual(sched.list().map((s) => s.name), ['ok']);
  } finally {
    cleanup();
  }
});

test('lookups of missing jobs and schedules are misses, not errors', () => {
  // #166 made lookup errors raise; a genuine miss must still be a miss.
  const { path: dbPath, open, cleanup } = tmpdb();
  try {
    const db = open(dbPath);
    const q = db.queue('emails');
    const sched = new honker.Scheduler(db);
    assert.equal(q.getJob(424242), null);
    assert.equal(q.cancel(424242), false);
    assert.equal(db.getResult(424242), null);
    assert.equal(sched.update('missing', { priority: 1 }), false);
    assert.equal(sched.pause('missing'), false);
    assert.equal(sched.resume('missing'), false);
    assert.equal(sched.remove('missing'), 0);
  } finally {
    cleanup();
  }
});

test('getJob shows the scheduled state until a delayed job is due', () => {
  // #180: future run_at is 'scheduled'; a delayed retry goes back to it.
  const { path: dbPath, open, cleanup } = tmpdb();
  try {
    const db = open(dbPath);
    const q = db.queue('later');
    const later = q.enqueue({ n: 1 }, { delay: 60 });
    assert.equal(q.getJob(later).state, 'scheduled');
    assert.equal(q.claimOne('w'), null);

    const now = q.enqueue({ n: 2 });
    assert.equal(q.getJob(now).state, 'pending');
    const job = q.claimOne('w');
    assert.equal(job.id, now);
    assert.equal(job.retry(60, 'later'), true);
    assert.equal(q.getJob(now).state, 'scheduled');
    assert.equal(job.retry(0), false, 'the claim is gone');

    assert.equal(q.cancel(later), true, 'cancel accepts scheduled rows');
    assert.equal(q.getJob(later), null);
  } finally {
    cleanup();
  }
});
