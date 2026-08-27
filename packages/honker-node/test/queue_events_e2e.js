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

test('queue event feed observes a real producer and workers across processes', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'honker-queue-events-e2e-'));
  const dbPath = path.join(dir, 'app.db');
  let db;
  let feed;
  try {
    db = honker.open(dbPath);
    db.configureQueueEvents({ maxEvents: 100, includePayload: true });

    const rolledBack = db.transaction();
    db.queue('emails').enqueueTx(rolledBack, { rolledBack: true });
    rolledBack.rollback();

    feed = db.queueEvents({ queue: 'emails', fromOffset: 0, fallbackPollS: null });
    const liveEvent = feed.next();
    const jobId = Number(runNode(`
      const honker = require('.');
      const db = honker.open(process.argv[1]);
      const id = db.queue('emails', { maxAttempts: 3 }).enqueue({
        recipient: 'alice@example.com',
        template: 'welcome',
      });
      console.log(id);
      db.close();
    `, dbPath));

    const first = await liveEvent;
    assert.equal(first.done, false);
    assert.equal(first.value.type, 'enqueued');
    assert.equal(first.value.jobId, jobId);
    assert.deepEqual(first.value.payload, {
      recipient: 'alice@example.com',
      template: 'welcome',
    });

    runNode(`
      const honker = require('.');
      const db = honker.open(process.argv[1]);
      const job = db.queue('emails', { maxAttempts: 3 }).claimOne('worker-1');
      if (!job || !job.retry(0, 'temporary')) process.exit(2);
      db.close();
    `, dbPath);
    runNode(`
      const honker = require('.');
      const db = honker.open(process.argv[1]);
      const job = db.queue('emails', { maxAttempts: 3 }).claimOne('worker-2');
      if (!job || !job.ack()) process.exit(2);
      db.close();
    `, dbPath);

    const replayed = feed.readSince(first.value.offset, 100);
    assert.deepEqual(
      replayed.filter((event) => event.jobId === jobId).map((event) => event.type),
      ['claimed', 'retry_scheduled', 'claimed', 'completed'],
    );
    assert.ok(replayed.every((event) => event.offset > first.value.offset));
    assert.ok(
      [first.value, ...replayed].every((event) => event.payload?.rolledBack !== true),
    );
  } finally {
    try { feed?.close(); } catch {}
    try { db?.close(); } catch {}
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
