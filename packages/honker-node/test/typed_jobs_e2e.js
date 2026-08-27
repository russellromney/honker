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

    const consumerOutput = runNode(`
      const honker = require('.');
      const db = honker.open(process.argv[1]);
      const job = db.queue('emails').claimOne('worker-e2e');
      console.log(JSON.stringify({
        id: job.id,
        payload: job.payload,
        state: job.state,
        priority: job.priority,
        runAt: job.runAt,
        workerId: job.workerId,
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

    db = honker.open(dbPath);
    assert.equal(db.queue('emails').getJob(jobId), null);
  } finally {
    try { db?.close(); } catch {}
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
