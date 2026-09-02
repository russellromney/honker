'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const honker = require('..');

function runNode(script, dbPath) {
  const result = spawnSync(process.execPath, ['-e', script, dbPath], {
    cwd: path.join(__dirname, '..'),
    encoding: 'utf8',
  });
  assert.equal(result.status, 0, result.stderr || result.stdout);
  return result.stdout.trim();
}

test('typed job details survive a producer-to-consumer process journey', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'honker-typed-jobs-e2e-'));
  const dbPath = path.join(dir, 'app.db');
  let db;
  try {
    const producerOutput = runNode(`
      const honker = require('.');
      const db = honker.open(process.argv[1]);
      const queue = db.queue('emails', { maxAttempts: 5 });
      const id = queue.enqueue(
        { recipient: 'alice@example.com', template: 'welcome' },
        { priority: 7, expires: 600 },
      );
      console.log(id);
      db.close();
    `, dbPath);
    const jobId = Number(producerOutput);
    assert.ok(jobId > 0);

    db = honker.open(dbPath);
    const snapshot = db.queue('emails').getJob(jobId);
    assert.deepEqual(snapshot.payload, {
      recipient: 'alice@example.com',
      template: 'welcome',
    });
    assert.equal(snapshot.state, 'pending');
    assert.equal(snapshot.priority, 7);
    assert.equal(snapshot.maxAttempts, 5);
    assert.ok(snapshot.runAt > 0);
    assert.ok(snapshot.createdAt > 0);
    assert.ok(snapshot.expiresAt >= snapshot.createdAt + 599);
    db.close();
    db = null;

    // A distinctive visibility timeout so claimExpiresAt cannot be confused
    // with any other timestamp on the job: runAt and createdAt sit at ~+0 and
    // expiresAt at ~+600, while this claim's deadline lands at ~+77.
    const visibilityTimeoutS = 77;
    const consumerOutput = runNode(`
      const honker = require('.');
      const db = honker.open(process.argv[1]);
      const job = db.queue('emails', { visibilityTimeoutS: ${visibilityTimeoutS} })
        .claimOne('worker-e2e');
      console.log(JSON.stringify({
        fields: Object.keys(job).filter((k) => !k.startsWith('_')),
        id: job.id,
        queue: job.queue,
        payload: job.payload,
        state: job.state,
        priority: job.priority,
        runAt: job.runAt,
        workerId: job.workerId,
        claimExpiresAt: job.claimExpiresAt,
        attempts: job.attempts,
        maxAttempts: job.maxAttempts,
        createdAt: job.createdAt,
        expiresAt: job.expiresAt,
        acked: job.ack(),
      }));
      db.close();
    `, dbPath);
    const claimed = JSON.parse(consumerOutput);
    assert.equal(claimed.id, jobId);
    assert.equal(claimed.queue, 'emails');
    assert.deepEqual(claimed.payload, snapshot.payload);
    assert.equal(claimed.state, 'processing');
    assert.equal(claimed.priority, 7);
    assert.equal(claimed.workerId, 'worker-e2e');
    assert.equal(claimed.attempts, 1);
    assert.equal(claimed.maxAttempts, 5);
    assert.equal(claimed.runAt, snapshot.runAt);
    assert.equal(claimed.createdAt, snapshot.createdAt);
    assert.equal(claimed.expiresAt, snapshot.expiresAt);
    assert.equal(claimed.acked, true);

    // The claim lease. Nothing else pins this field, and every other timestamp
    // on the job is far outside this window, so a mapping that returned runAt,
    // createdAt, or expiresAt here fails.
    assert.equal(typeof claimed.claimExpiresAt, 'number');
    assert.ok(
      claimed.claimExpiresAt >= claimed.createdAt + visibilityTimeoutS,
      `claimExpiresAt ${claimed.claimExpiresAt} is before createdAt + ${visibilityTimeoutS}`,
    );
    assert.ok(
      claimed.claimExpiresAt <= claimed.createdAt + visibilityTimeoutS + 120,
      `claimExpiresAt ${claimed.claimExpiresAt} is far past createdAt + ${visibilityTimeoutS}`,
    );

    // A claimed Job and a JobSnapshot must expose the same twelve fields.
    // Without this, a field added to one shape and not the other ships silently.
    const JOB_FIELDS = [
      'attempts', 'claimExpiresAt', 'createdAt', 'expiresAt', 'id',
      'maxAttempts', 'payload', 'priority', 'queue', 'runAt', 'state',
      'workerId',
    ];
    assert.deepEqual(claimed.fields.slice().sort(), JOB_FIELDS);
    assert.deepEqual(Object.keys(snapshot).sort(), JOB_FIELDS);

    db = honker.open(dbPath);
    assert.equal(db.queue('emails').getJob(jobId), null);
  } finally {
    try { db?.close(); } catch {}
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('getJob is scoped to its own queue', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'honker-getjob-scope-'));
  const dbPath = path.join(dir, 'app.db');
  let db;
  try {
    db = honker.open(dbPath);
    const emails = db.queue('emails');
    const sms = db.queue('sms');

    // Two queues with incompatible payload contracts. Job ids are globally
    // unique, so a raw id lookup would happily hand an SMS row to `emails`.
    const smsId = sms.enqueue({ phone: '+15550100', body: 'hello' });

    assert.equal(emails.getJob(smsId), null);

    const owned = sms.getJob(smsId);
    assert.deepEqual(owned.payload, { phone: '+15550100', body: 'hello' });
    assert.equal(owned.queue, 'sms');
    assert.equal(owned.id, smsId);
  } finally {
    try { db?.close(); } catch {}
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('a claimed job reports its own run_at, not its created_at', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'honker-runat-backdated-'));
  const dbPath = path.join(dir, 'app.db');
  let db;
  try {
    db = honker.open(dbPath);
    const q = db.queue('backdated');

    // Back-date run_at so it differs from created_at while the job stays
    // claimable. A *delay* would push run_at into the future, which makes the
    // job unclaimable — which is why every other test here compares two values
    // that are equal to the second and cannot tell the two fields apart.
    const runAt = Math.floor(Date.now() / 1000) - 100;
    const id = q.enqueue({ hello: 'backdated' }, { runAt });

    const snapshot = q.getJob(id);
    assert.equal(snapshot.runAt, runAt);
    assert.notEqual(snapshot.runAt, snapshot.createdAt);

    const job = q.claimOne('worker-backdated');
    assert.equal(job.id, id);
    assert.equal(job.runAt, runAt);
    assert.notEqual(job.runAt, job.createdAt);
    assert.equal(job.createdAt, snapshot.createdAt);
  } finally {
    try { db?.close(); } catch {}
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('a JSON null payload comes back as null, not as the string "null"', () => {
  // `Job<T>.payload` and `JobSnapshot<T>.payload` are declared as `T`, but the
  // generic is an unchecked assertion: honker never inspects payload shape. A
  // JSON null is the one violation reachable from any producer with no raw SQL
  // at all, so pin what a reader actually gets. The TSDoc on
  // `JobSnapshot.payload` documents this; this test keeps that doc honest.
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'honker-null-payload-'));
  const dbPath = path.join(dir, 'app.db');
  let db;
  try {
    db = honker.open(dbPath);
    const q = db.queue('nullable');
    const id = q.enqueue(null);

    const snapshot = q.getJob(id);
    assert.equal(snapshot.payload, null);

    const job = q.claimOne('worker-null');
    assert.equal(job.id, id);
    assert.equal(job.payload, null);
    assert.equal(job.ack(), true);
  } finally {
    try { db?.close(); } catch {}
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
