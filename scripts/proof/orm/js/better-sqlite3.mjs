// better-sqlite3, exactly as guides/orm/javascript.mdx shows it.
// docs:start example
import assert from 'node:assert/strict';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import { extensionPath, extensionInfo } from '@russellthehippo/honker-node/extension';
import { run } from './surface.mjs';

const info = extensionInfo();
assert.equal(info.entrypoint, 'sqlite3_honkerext_init');
assert.match(
  info.path,
  new RegExp(`honker-ext-${process.env.HONKER_PLATFORM}`),
  `expected the packaged extension, resolved ${info.path}`,
);

const dbPath = process.env.HONKER_TEST_DB;
if (!dbPath) throw new Error('HONKER_TEST_DB is required');
fs.rmSync(dbPath, { force: true });

const db = new Database(dbPath);
db.loadExtension(extensionPath());
db.prepare('SELECT honker_bootstrap()').run();

await run((sql, args) => {
  const row = db.prepare(sql).get(...args);
  return row ? Object.values(row)[0] : null;
}, 'b3');

db.exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)');
const committed = db.transaction(() => {
  db.prepare('INSERT INTO orders (id, user_id) VALUES (?, ?)').run(42, 1);
  return db
    .prepare('SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL) AS id')
    .get('b3_atomic', JSON.stringify({ order_id: 42 }), 0, 3).id;
})();
assert.equal(db.prepare('SELECT COUNT(*) AS n FROM orders WHERE id = 42').get().n, 1);
assert.match(db.prepare('SELECT honker_get_job(?) AS job').get(committed).job, /order_id/);

let rolled;
try {
  db.transaction(() => {
    db.prepare('INSERT INTO orders (id, user_id) VALUES (?, ?)').run(43, 1);
    rolled = db
      .prepare('SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL) AS id')
      .get('b3_atomic', JSON.stringify({ order_id: 43 }), 0, 3).id;
    throw new Error('rollback');
  })();
} catch (err) {
  if (err.message !== 'rollback') throw err;
}
assert.equal(db.prepare('SELECT COUNT(*) AS n FROM orders WHERE id = 43').get().n, 0);
assert.equal(db.prepare('SELECT honker_get_job(?) AS job').get(rolled).job, '');

db.close();
console.log('PASS better-sqlite3');
// docs:end example

// Not part of the docs example: a write lock held past busy_timeout.
//
// Another process holds BEGIN EXCLUSIVE while this connection calls
// honker_enqueue, honker_claim_batch, honker_ack and
// honker_scheduler_tick. Each waits out better-sqlite3's default 5 s
// timeout and throws a SqliteError. The error comes from inside the
// honker_* function, so err.code is SQLite's code: SQLITE_BUSY since
// #184 (it was SQLITE_ERROR before, with the same message). Nothing is
// half-done, and the same call succeeds once the lock is gone.
{
  const { spawn } = await import('node:child_process');
  const { createRequire } = await import('node:module');
  const betterSqlite = createRequire(import.meta.url).resolve('better-sqlite3');

  const busyPath = `${dbPath}.busy.db`;
  for (const suffix of ['', '-wal', '-shm']) fs.rmSync(busyPath + suffix, { force: true });
  const user = new Database(busyPath); // default timeout: 5000 ms
  user.loadExtension(extensionPath());
  user.pragma('journal_mode = WAL');
  const scalar = (sql, ...args) => Object.values(user.prepare(sql).get(...args))[0];
  scalar('SELECT honker_bootstrap()');
  const claimId = scalar("SELECT honker_enqueue('busy-claim', '{}', NULL, NULL, 0, 3, NULL)");
  const ackId = scalar("SELECT honker_enqueue('busy-ack', '{}', NULL, NULL, 0, 3, NULL)");
  assert.equal(JSON.parse(scalar("SELECT honker_claim_batch('busy-ack', 'w-ack', 1, 300)"))[0].id, ackId);
  scalar("SELECT honker_scheduler_register('busy-tick', 'busy-tick', '@every 1s', '{}', 0, NULL, 3)");

  const calls = {
    enqueue: () => scalar("SELECT honker_enqueue('busy-enqueue', ?, NULL, NULL, 0, 3, NULL)", '{"k":1}'),
    claim: () => JSON.parse(scalar("SELECT honker_claim_batch('busy-claim', 'w-claim', 1, 300)"))[0]?.id ?? null,
    ack: () => scalar("SELECT honker_ack(?, 'w-ack')", ackId),
    tick: () => JSON.parse(scalar('SELECT honker_scheduler_tick(unixepoch())')),
  };
  const jobs = (queue) => user.prepare(
    'SELECT id, state, worker_id, attempts FROM _honker_live WHERE queue = ? ' +
    "UNION ALL SELECT id, 'dead', NULL, attempts FROM _honker_dead WHERE queue = ? ORDER BY id",
  ).all(queue, queue);
  const state = () => ({
    enqueue: jobs('busy-enqueue'),
    claim: jobs('busy-claim'),
    ack: jobs('busy-ack'),
    tick: jobs('busy-tick'),
    nextFireAt: scalar("SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name = 'busy-tick'"),
  });
  const before = state();

  const holder = spawn(process.execPath, ['-e', `
    const Database = require(process.argv[1]);
    const db = new Database(process.argv[2]);
    db.exec('BEGIN EXCLUSIVE');
    console.log('LOCKED');
    process.stdin.once('data', () => { db.exec('ROLLBACK'); db.close(); process.exit(0); });
  `, betterSqlite, busyPath], { stdio: ['pipe', 'pipe', 'inherit'] });
  const holderExit = new Promise((resolve) => holder.on('exit', resolve));
  await new Promise((resolve, reject) => {
    holder.stdout.once('data', (d) => (String(d).includes('LOCKED') ? resolve() : reject(new Error(String(d)))));
    holder.once('exit', (code) => reject(new Error(`lock holder exited ${code}`)));
  });

  const seen = {};
  try {
    for (const [op, call] of Object.entries(calls)) {
      const started = Date.now();
      try {
        seen[op] = { unexpected: call() };
      } catch (err) {
        seen[op] = { class: err.constructor.name, code: err.code, message: err.message };
        assert.ok(Date.now() - started > 4000, `${op} gave up after ${Date.now() - started} ms`);
      }
      assert.equal(user.inTransaction, false, `${op} left a transaction open`);
    }
    console.log('better-sqlite3 under a held write lock: ' + JSON.stringify(seen));
    const expected = { class: 'SqliteError', code: 'SQLITE_BUSY', message: 'database is locked' };
    assert.deepEqual(seen, Object.fromEntries(Object.keys(calls).map((op) => [op, expected])));
    // Nothing half-done while the lock is still held.
    assert.deepEqual(state(), before);
  } finally {
    holder.stdin.write('\n');
    assert.equal(await holderExit, 0);
  }

  const results = Object.fromEntries(Object.entries(calls).map(([op, call]) => [op, call()]));
  const after = state();
  assert.deepEqual(after.enqueue.map((j) => j.id), [results.enqueue]);
  assert.equal(results.claim, claimId);
  assert.deepEqual(after.claim, [{ id: claimId, state: 'processing', worker_id: 'w-claim', attempts: 1 }]);
  assert.equal(results.ack, 1);
  assert.deepEqual(after.ack, []);
  assert.ok(results.tick.length >= 1);
  assert.equal(after.tick.length, results.tick.length);
  assert.ok(after.nextFireAt > before.nextFireAt);
  user.close();
  console.log('PASS better-sqlite3 busy');
}
