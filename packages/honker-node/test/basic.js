// Smoke test for the Node binding. Run with `node --test test/`.
const test = require('node:test');
const assert = require('node:assert/strict');

const lit = require('..');
const { createTempDb } = require('./helpers');

// `createTempDb` wraps every Database/UpdateEvents/etc. it hands out
// so `cleanup()` can close them before unlinking the tempdir. Required
// on Windows: SQLite holds the file handle, and `rmSync` hits EBUSY
// until every Connection has been dropped. The wrapper makes the
// "always close before unlink" rule structural rather than something
// each test has to remember.
function tmpdb() {
  return createTempDb('honker-node-', lit.open.bind(lit));
}

test('open / transaction / commit', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    const tx = db.transaction();
    tx.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT)');
    tx.execute('INSERT INTO t (payload) VALUES (?)', ['hello']);
    tx.commit();
    const rows = db.query('SELECT id, payload FROM t ORDER BY id');
    assert.equal(rows.length, 1);
    assert.equal(rows[0].payload, 'hello');
  } finally {
    db?.close();
    cleanup();
  }
});

test('rollback drops writes', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    {
      const tx = db.transaction();
      tx.execute('CREATE TABLE t (v INTEGER)');
      tx.commit();
    }
    {
      const tx = db.transaction();
      tx.execute('INSERT INTO t (v) VALUES (1)');
      tx.rollback();
    }
    const rows = db.query('SELECT COUNT(*) AS c FROM t');
    assert.equal(rows[0].c, 0);
  } finally {
    db?.close();
    cleanup();
  }
});

test('notify inserts into _honker_notifications and rollback drops it', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);

    // Committed notify. payload is any JSON-serializable value —
    // stringified inside the binding, matches Python's json.dumps.
    {
      const tx = db.transaction();
      const id = tx.notify('orders', { id: 42 });
      assert.ok(id > 0);
      tx.commit();
    }
    // Rolled-back notify
    {
      const tx = db.transaction();
      tx.notify('orders', { id: 99 });
      tx.rollback();
    }
    const rows = db.query(
      "SELECT channel, payload FROM _honker_notifications WHERE channel='orders' ORDER BY id"
    );
    assert.equal(rows.length, 1);
    assert.equal(rows[0].channel, 'orders');
    assert.deepEqual(JSON.parse(rows[0].payload), { id: 42 });
  } finally {
    db?.close();
    cleanup();
  }
});

test('notify payload round-trips common JSON shapes', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    const cases = [
      { id: 42, name: 'alice' },  // object
      [1, 2, 3],                   // array
      'hello',                     // string
      42,                          // integer
      3.14,                        // float
      null,                        // null
      true,                        // bool
    ];
    {
      const tx = db.transaction();
      for (const p of cases) tx.notify('rt', p);
      tx.commit();
    }
    const rows = db.query(
      "SELECT payload FROM _honker_notifications WHERE channel='rt' ORDER BY id"
    );
    const decoded = rows.map((r) => JSON.parse(r.payload));
    assert.deepEqual(decoded, cases);
  } finally {
    db?.close();
    cleanup();
  }
});

test('updateEvents fires on commit', async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    // Force WAL file to exist by doing a first commit.
    {
      const tx = db.transaction();
      tx.execute('CREATE TABLE t (n INTEGER)');
      tx.commit();
    }
    const ev = db.updateEvents();
    // Fire a commit after a short delay; updateEvents.next() should resolve.
    setTimeout(() => {
      const tx = db.transaction();
      tx.execute('INSERT INTO t (n) VALUES (1)');
      tx.commit();
    }, 50);
    const t0 = performance.now();
    await ev.next();
    const dt = performance.now() - t0;
    assert.ok(dt < 500, `WAL wake took ${dt.toFixed(1)}ms, expected < 500`);
    ev.close();
  } finally {
    db?.close();
    cleanup();
  }
});

test('custom watcherPollIntervalMs still fires on commit', async () => {
  const { path: dbPath, cleanup } = tmpdb();
  let db;
  try {
    db = lit.open(dbPath, { watcherPollIntervalMs: 25 });
    {
      const tx = db.transaction();
      tx.execute('CREATE TABLE t (n INTEGER)');
      tx.commit();
    }
    const ev = db.updateEvents();
    setTimeout(() => {
      const tx = db.transaction();
      tx.execute('INSERT INTO t (n) VALUES (1)');
      tx.commit();
    }, 50);
    await Promise.race([
      ev.next(),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('custom watcher interval did not observe commit')), 1000)
      ),
    ]);
    ev.close();
  } finally {
    db?.close();
    cleanup();
  }
});

test('updateEvents dropped without close() still releases the watcher thread', async () => {
  // Create+drop many UpdateEvents instances without calling .close() on
  // any of them. Dropping must cascade to the core UpdateWatcher's Drop,
  // which signals stop to the update watcher thread. Without Drop semantics,
  // 100 abandoned UpdateEvents = 100 stuck threads.
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    // Force the WAL to exist.
    {
      const tx = db.transaction();
      tx.execute('CREATE TABLE t (n INTEGER)');
      tx.commit();
    }

    for (let i = 0; i < 100; i++) {
      const ev = db.updateEvents();
      // Let the thread spawn; don't call close(). Abandon and move on.
      void ev;
    }

    // Give GC a chance and the stop flags a tick to propagate.
    if (global.gc) global.gc();
    await new Promise((r) => setTimeout(r, 100));

    // Sanity: a fresh updateEvents still wakes on commit. If prior watchers
    // somehow deadlocked the poll infrastructure this would time out.
    const ev = db.updateEvents();
    setTimeout(() => {
      const tx = db.transaction();
      tx.execute('INSERT INTO t (n) VALUES (1)');
      tx.commit();
    }, 50);
    const t0 = performance.now();
    await ev.next();
    const dt = performance.now() - t0;
    assert.ok(dt < 1000, `post-churn WAL wake took ${dt.toFixed(1)}ms`);
    ev.close();
  } finally {
    db?.close();
    cleanup();
  }
});

test('pruneNotifications by max_keep', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    for (let i = 0; i < 50; i++) {
      const tx = db.transaction();
      tx.notify('ch', `n${i}`);
      tx.commit();
    }
    const before = db.query('SELECT COUNT(*) AS c FROM _honker_notifications');
    assert.equal(before[0].c, 50);
    const deleted = db.pruneNotifications(null, 5);
    assert.equal(deleted, 45);
    const after = db.query('SELECT COUNT(*) AS c FROM _honker_notifications');
    assert.equal(after[0].c, 5);
  } finally {
    db?.close();
    cleanup();
  }
});

test('pruneNotifications maxKeep is gap-safe and validates bounds', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    {
      const tx = db.transaction();
      tx.execute(
        "INSERT INTO _honker_notifications " +
        "(id, channel, payload, created_at) VALUES " +
        "(90, 'ch', '\"old\"', unixepoch()), " +
        "(100, 'ch', '\"new\"', unixepoch())"
      );
      tx.commit();
    }

    assert.equal(db.pruneNotifications(null, 5), 0);
    assert.equal(db.pruneNotifications(null, 2), 0);
    assert.equal(db.pruneNotifications(null, 1), 1);
    assert.deepEqual(
      db.query('SELECT id FROM _honker_notifications ORDER BY id')
        .map((row) => row.id),
      [100]
    );
    assert.throws(
      () => db.pruneNotifications(null, -1),
      /maxKeep must not be negative/
    );
    assert.equal(db.pruneNotifications(null, 0), 1);

    for (const payload of ['fresh-1', 'fresh-2', 'fresh-3', 'old-newest']) {
      const tx = db.transaction();
      tx.notify('ch', payload);
      tx.commit();
    }
    {
      const tx = db.transaction();
      tx.execute(
        "UPDATE _honker_notifications SET created_at=0 " +
        "WHERE payload='\"old-newest\"'"
      );
      tx.commit();
    }
    assert.equal(db.pruneNotifications(1, 2), 3);
    assert.deepEqual(
      db.query('SELECT payload FROM _honker_notifications ORDER BY id')
        .map((row) => JSON.parse(row.payload)),
      ['fresh-3']
    );
  } finally {
    db?.close();
    cleanup();
  }
});

// ---------------------------------------------------------------------
// A write lock held past busy_timeout: what the user sees.
//
// Another process holds BEGIN EXCLUSIVE (node:sqlite) while separate
// processes call enqueue, claimOne, job.ack() and scheduler.tick().
// Each waits out the binding's 5 s busy_timeout and throws. Nothing is
// half-done, and the same call succeeds once the lock is gone.
//
// The napi binding is not changed by #184: every call runs in
// db.transaction(), whose BEGIN IMMEDIATE hits the lock before any
// honker_* function runs, and napi_err() maps the error to a plain
// Error with code 'GenericFailure' and SQLite's message. No SQLite code
// is exposed. Pinned as-is so a binding PR that adds a typed busy error
// has to change it on purpose.
// ---------------------------------------------------------------------

const { spawn } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const BUSY_PRELUDE = `
const fs = require('node:fs');
const out = (line) => fs.writeSync(1, line + '\\n');
function waitFile(p) {
  const deadline = Date.now() + 60000;
  while (!fs.existsSync(p)) {
    if (Date.now() > deadline) throw new Error('timed out waiting for ' + p);
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 20);
  }
}
`;

const BUSY_HOLDER = `${BUSY_PRELUDE}
const { DatabaseSync } = require('node:sqlite');
const [dbPath, release] = process.argv.slice(1);
const db = new DatabaseSync(dbPath);
db.exec('BEGIN EXCLUSIVE');
out('LOCKED');
waitFile(release);
db.exec('ROLLBACK');
db.close();
`;

const BUSY_CALLER = `${BUSY_PRELUDE}
const honker = require(${JSON.stringify(path.resolve(__dirname, '..'))});
const [dbPath, op, go, retry] = process.argv.slice(1);
const db = honker.open(dbPath);
const q = db.queue('busy-' + op);
const job = op === 'ack' ? q.claimOne('w-ack') : null;
const sched = db.scheduler();
const call = () => {
  if (op === 'enqueue') return q.enqueue({ k: 'under-lock' });
  if (op === 'claim') return q.claimOne('w-claim')?.id ?? null;
  if (op === 'ack') return job.ack();
  if (op === 'tick') return sched.tick();
  throw new Error('unknown op ' + op);
};
out('READY');
waitFile(go);
const started = Date.now();
try {
  out('UNEXPECTED ' + JSON.stringify(call()));
  process.exit(0);
} catch (err) {
  out('ERROR ' + JSON.stringify({
    class: err.constructor.name,
    message: err.message,
    code: err.code ?? null,
    waitedMs: Date.now() - started,
  }));
}
waitFile(retry);
out('RESULT ' + JSON.stringify(call()));
db.close();
`;

function busyChild(code, args) {
  const proc = spawn(process.execPath, ['-e', code, ...args], { stdio: ['ignore', 'pipe', 'pipe'] });
  const lines = [];
  let buf = '';
  let stderr = '';
  proc.stdout.on('data', (d) => {
    buf += d;
    let i;
    while ((i = buf.indexOf('\n')) >= 0) {
      lines.push(buf.slice(0, i));
      buf = buf.slice(i + 1);
    }
  });
  proc.stderr.on('data', (d) => { stderr += d; });
  const exited = new Promise((resolve) => proc.on('exit', resolve));
  return {
    proc,
    exited,
    async line(prefix, timeoutMs = 30000) {
      const deadline = Date.now() + timeoutMs;
      while (Date.now() < deadline) {
        const i = lines.findIndex((l) => l.startsWith(prefix));
        if (i >= 0) return lines.splice(i, 1)[0].slice(prefix.length).trim();
        if (proc.exitCode !== null) break;
        await new Promise((r) => setTimeout(r, 20));
      }
      throw new Error(`no ${prefix} line; lines=${JSON.stringify(lines)} stderr=${stderr}`);
    },
  };
}

function busyState(db) {
  const jobs = (queue) => db.query(
    'SELECT id, state, worker_id, attempts FROM _honker_live WHERE queue = ? ' +
    "UNION ALL SELECT id, 'dead', NULL, attempts FROM _honker_dead WHERE queue = ? ORDER BY id",
    [queue, queue],
  );
  return {
    enqueue: jobs('busy-enqueue'),
    claim: jobs('busy-claim'),
    ack: jobs('busy-ack'),
    tick: jobs('busy-tick'),
    nextFireAt: db.query("SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name = 'busy-tick'")[0].next_fire_at,
  };
}

test('calls under a write lock held past busy_timeout throw, change nothing, and succeed after', { timeout: 60000 }, async () => {
  const { path: dbPath, dir, open, cleanup } = tmpdb();
  const ops = ['enqueue', 'claim', 'ack', 'tick'];
  const children = [];
  try {
    const db = open(dbPath);
    const claimId = db.queue('busy-claim').enqueue({ k: 'claim-me' });
    const ackId = db.queue('busy-ack').enqueue({ k: 'ack-me' });
    db.queue('busy-enqueue');
    db.scheduler().add({ name: 'busy-tick', queue: 'busy-tick', schedule: '@every 1s', payload: {} });
    const [go, retry, release] = ['go', 'retry', 'release'].map((n) => path.join(dir, n));

    const callers = Object.fromEntries(ops.map((op) => {
      const c = busyChild(BUSY_CALLER, [dbPath, op, go, retry]);
      children.push(c);
      return [op, c];
    }));
    for (const c of Object.values(callers)) await c.line('READY');
    const before = busyState(db);
    assert.deepEqual(before.ack, [{ id: ackId, state: 'processing', worker_id: 'w-ack', attempts: 1 }]);

    const holder = busyChild(BUSY_HOLDER, [dbPath, release]);
    children.push(holder);
    await holder.line('LOCKED');
    fs.writeFileSync(go, '');
    const seen = {};
    const waited = {};
    for (const op of ops) {
      const err = JSON.parse(await callers[op].line('ERROR '));
      waited[op] = err.waitedMs;
      delete err.waitedMs;
      seen[op] = err;
    }
    console.log('napi: ' + JSON.stringify(seen));
    const expected = { class: 'Error', message: 'database is locked', code: 'GenericFailure' };
    assert.deepEqual(seen, Object.fromEntries(ops.map((op) => [op, expected])));
    assert.ok(Object.values(waited).every((ms) => ms > 4000), `gave up before busy_timeout: ${JSON.stringify(waited)}`);
    // Nothing half-done while the lock is still held.
    assert.deepEqual(busyState(db), before);

    fs.writeFileSync(release, '');
    assert.equal(await holder.exited, 0);
    fs.writeFileSync(retry, '');
    const results = {};
    for (const op of ops) results[op] = JSON.parse(await callers[op].line('RESULT '));
    for (const c of Object.values(callers)) assert.equal(await c.exited, 0);

    const after = busyState(db);
    assert.deepEqual(after.enqueue.map((j) => j.id), [results.enqueue]);
    assert.equal(results.claim, claimId);
    assert.deepEqual(after.claim, [{ id: claimId, state: 'processing', worker_id: 'w-claim', attempts: 1 }]);
    assert.equal(results.ack, true);
    assert.deepEqual(after.ack, []);
    assert.ok(results.tick.length >= 1);
    assert.equal(after.tick.length, results.tick.length);
    assert.ok(after.nextFireAt > before.nextFireAt);
  } finally {
    for (const c of children) if (c.proc.exitCode === null) c.proc.kill();
    cleanup();
  }
});
