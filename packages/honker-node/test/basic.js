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
// JSON payload contract (#152), from the user's side. The typed APIs
// JSON.stringify for you; raw SQL inside a transaction and saveResult
// take text, and that text must be JSON. Readers, workers and
// schedulers run in separate processes on one database file.
// ---------------------------------------------------------------------

const { spawn } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const { setTimeout: delay } = require('node:timers/promises');

const NOT_JSON = /honker: payload must be valid JSON/;
const UNDECODABLE = /honker: payload is valid JSON but cannot be decoded/;
const RAW_ENQUEUE = 'SELECT honker_enqueue(?, ?, NULL, NULL, 0, 3, NULL)';
const REPO_ROOT = path.resolve(__dirname, '..', '..', '..');

const JSON_CHILD = `
'use strict';
const honker = require(${JSON.stringify(path.resolve(__dirname, '..'))});
const cfg = JSON.parse(process.argv[1]);
const db = honker.open(cfg.db);
const out = (tag, obj) => console.log(tag + ' ' + JSON.stringify(obj ?? {}));
(async () => {
  if (cfg.role === 'worker') {
    const q = db.queue(cfg.queue);
    const it = q.claim('json-worker', { idlePollS: 30 });
    out('READY');
    for await (const job of it) {
      out('JOB', { payload: job.payload, ok: job.ack() });
      break;
    }
  } else if (cfg.role === 'listen') {
    const l = db.listen(cfg.channel);
    out('READY');
    const { value } = await l.next();
    out('NOTE', { payload: value.payload });
    l.close();
  } else if (cfg.role === 'subscribe') {
    const sub = db.stream(cfg.stream).subscribe('json-reader');
    out('READY');
    const { value } = await sub.next();
    out('EVENT', { offset: value.offset, payload: value.payload });
    sub.close();
  } else if (cfg.role === 'scheduler') {
    const ac = new AbortController();
    setTimeout(() => ac.abort(), cfg.runMs);
    out('READY');
    await db.scheduler().run(cfg.owner, ac.signal);
    out('STOPPED');
  }
  db.close();
  // Exit once stdout is flushed. A closed claim iterator or listener can
  // still hold a pending idle wait that keeps the event loop alive.
  process.stdout.write('', () => process.exit(0));
})().catch((err) => {
  console.error(err && err.stack ? err.stack : String(err));
  process.exit(1);
});
`;

class JsonChild {
  constructor(cfg) {
    this.proc = spawn(process.execPath, ['-e', JSON_CHILD, JSON.stringify(cfg)], {
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    this.lines = [];
    this.stderr = '';
    this.exitCode = null;
    this._waiters = [];
    let buf = '';
    this.proc.stdout.on('data', (chunk) => {
      buf += chunk.toString('utf8');
      let nl;
      while ((nl = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, nl).replace(/\r$/, '');
        buf = buf.slice(nl + 1);
        const sp = line.indexOf(' ');
        this.lines.push({ tag: line.slice(0, sp), body: JSON.parse(line.slice(sp + 1)) });
        this._wake();
      }
    });
    this.proc.stderr.on('data', (chunk) => {
      this.stderr += chunk.toString('utf8');
    });
    this.exited = new Promise((resolve) => {
      this.proc.on('exit', (code) => {
        this.exitCode = code;
        this._wake();
        resolve(code);
      });
    });
  }

  _wake() {
    for (const w of this._waiters.splice(0)) w();
  }

  async next(tag, timeoutMs = 10000) {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const hit = this.lines.find((l) => l.tag === tag);
      if (hit) return hit.body;
      if (this.exitCode !== null) {
        throw new Error(`child exited ${this.exitCode} before ${tag}: ${this.stderr}`);
      }
      const left = deadline - Date.now();
      if (left <= 0) throw new Error(`timeout waiting for ${tag}: ${this.stderr}`);
      await new Promise((resolve) => {
        const t = setTimeout(resolve, left);
        this._waiters.push(() => {
          clearTimeout(t);
          resolve();
        });
      });
    }
  }

  async kill() {
    if (this.exitCode === null) this.proc.kill('SIGKILL');
    await this.exited;
  }
}

async function withJsonChildren(fn) {
  const children = [];
  try {
    await fn((cfg) => {
      const c = new JsonChild(cfg);
      children.push(c);
      return c;
    });
  } finally {
    await Promise.all(children.map((c) => c.kill()));
  }
}

// The app's transaction, two ways: honker's own Transaction, and a
// connection the app owns with the extension loaded (node:sqlite here,
// standing in for better-sqlite3/Drizzle/Kysely).
function honkerTxApp(open, dbPath) {
  const db = open(dbPath);
  const q = db.queue('emails');
  const setup = db.transaction();
  setup.execute('CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, email TEXT)');
  setup.commit();
  return {
    run(body, commit) {
      const tx = db.transaction();
      try {
        body((sql, args) => tx.query(sql, args), (payload) => q.enqueueTx(tx, payload));
      } catch (err) {
        tx.rollback();
        throw err;
      }
      if (commit) tx.commit();
      else tx.rollback();
    },
    query: (sql) => db.query(sql).map((r) => Object.values(r)),
  };
}

function repoExtensionPath() {
  const name =
    process.platform === 'win32'
      ? 'honker_ext.dll'
      : process.platform === 'darwin'
        ? 'libhonker_ext.dylib'
        : 'libhonker_ext.so';
  const p = path.join(REPO_ROOT, 'target', 'release', name);
  return fs.existsSync(p) ? p : null;
}

function nodeSqliteApp(dbPath) {
  let DatabaseSync;
  try {
    ({ DatabaseSync } = require('node:sqlite'));
  } catch {
    return { skip: 'node:sqlite is not available in this Node' };
  }
  // The repo build, not the published honker-ext package npm ci
  // installed: the contract under test is this checkout's.
  const ext = repoExtensionPath();
  if (!ext) return { skip: 'target/release honker extension not built' };
  const conn = new DatabaseSync(dbPath, { allowExtension: true });
  if (typeof conn.loadExtension !== 'function') {
    conn.close();
    return { skip: 'node:sqlite cannot load extensions in this Node' };
  }
  conn.loadExtension(ext);
  conn.exec('PRAGMA busy_timeout = 5000');
  conn.prepare('SELECT honker_bootstrap()').get();
  conn.exec('CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, email TEXT)');
  return {
    conn,
    run(body, commit) {
      conn.exec('BEGIN IMMEDIATE');
      try {
        body(
          (sql, args) => conn.prepare(sql).all(...args),
          (payload) => conn.prepare(RAW_ENQUEUE).get('emails', JSON.stringify(payload)),
        );
      } catch (err) {
        conn.exec('ROLLBACK');
        throw err;
      }
      conn.exec(commit ? 'COMMIT' : 'ROLLBACK');
    },
    query: (sql) => conn.prepare(sql).all().map((r) => Object.values(r)),
  };
}

async function c8BadJsonScenario(dbPath, app) {
  await withJsonChildren(async (add) => {
    const worker = add({ db: dbPath, role: 'worker', queue: 'emails' });
    await worker.next('READY');

    const writeThenBadEnqueue = (orderId) => (execute) => {
      execute('INSERT INTO orders (id, email) VALUES (?, ?)', [orderId, `o${orderId}@example.com`]);
      assert.throws(() => execute(RAW_ENQUEUE, ['emails', 'not json']), NOT_JSON);
    };

    // The app catches the error and commits: its write survives, no job.
    app.run(writeThenBadEnqueue(1), true);
    assert.deepEqual(app.query('SELECT id FROM orders ORDER BY id'), [[1]]);
    assert.deepEqual(app.query('SELECT COUNT(*) FROM _honker_live'), [[0]]);

    // The app rolls back instead: neither exists.
    app.run(writeThenBadEnqueue(2), false);
    assert.deepEqual(app.query('SELECT id FROM orders ORDER BY id'), [[1]]);
    assert.deepEqual(app.query('SELECT COUNT(*) FROM _honker_live'), [[0]]);

    // A valid enqueue in the same transaction as a rejected one.
    app.run((execute, enqueue) => {
      writeThenBadEnqueue(3)(execute);
      enqueue({ order_id: 3 });
    }, true);
    const job = await worker.next('JOB');
    assert.deepEqual(job, { payload: { order_id: 3 }, ok: true });
    assert.equal(await worker.exited, 0, worker.stderr);
    assert.deepEqual(app.query('SELECT id FROM orders ORDER BY id'), [[1], [3]]);
    assert.deepEqual(app.query('SELECT COUNT(*) FROM _honker_live'), [[0]]);
    assert.deepEqual(app.query('SELECT COUNT(*) FROM _honker_dead'), [[0]]);
  });
}

test('C8: non-JSON raw enqueue in a honker transaction throws; the app write commits alone', async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  try {
    await c8BadJsonScenario(dbPath, honkerTxApp(open, dbPath));
  } finally {
    cleanup();
  }
});

test('C8: non-JSON raw enqueue on an app-owned connection with the extension throws; the app write commits alone', async (t) => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let app;
  try {
    open(dbPath); // create the file in WAL, the way an app would first open it
    app = nodeSqliteApp(dbPath);
    if (app.skip) {
      t.skip(app.skip);
      return;
    }
    await c8BadJsonScenario(dbPath, app);
  } finally {
    app?.conn?.close();
    cleanup();
  }
});

test('saveResult takes JSON text: raw text throws, JSON text round-trips', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  try {
    const db = open(dbPath);
    assert.throws(() => db.saveResult(1, 'done', 60), NOT_JSON);
    assert.equal(db.getResult(1), null);
    for (const [id, text] of [
      [2, '"done"'],
      [3, '{"sum":3}'],
      [4, '[1,"two"]'],
      [5, '42'],
      [6, 'null'],
    ]) {
      db.saveResult(id, text, 60);
      assert.equal(db.getResult(id), text);
    }
    assert.throws(() => db.saveResult(7, '"\\ud800"', 60), UNDECODABLE);
  } finally {
    cleanup();
  }
});

test('typed enqueue / publish reject what JSON.stringify emits but cannot be decoded', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  try {
    const db = open(dbPath);
    let deep = 1;
    for (let i = 0; i < 200; i++) deep = [deep];
    assert.throws(() => db.queue('typed').enqueue({ s: '\ud800' }), UNDECODABLE);
    assert.throws(() => db.queue('typed').enqueue(deep), UNDECODABLE);
    assert.throws(() => db.stream('typed').publish({ s: '\ud800' }), UNDECODABLE);
    assert.throws(() => db.stream('typed').publish(deep), UNDECODABLE);
    assert.deepEqual(
      db.query(
        'SELECT (SELECT COUNT(*) FROM _honker_live) AS live, (SELECT COUNT(*) FROM _honker_stream) AS events',
      ),
      [{ live: 0, events: 0 }],
    );
  } finally {
    cleanup();
  }
});

test('non-JSON notify and stream publish throw; the listener and subscriber processes see only the good one', async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  try {
    const db = open(dbPath);
    await withJsonChildren(async (add) => {
      const listener = add({ db: dbPath, role: 'listen', channel: 'orders' });
      const reader = add({ db: dbPath, role: 'subscribe', stream: 'orders-stream' });
      await listener.next('READY');
      await reader.next('READY');

      const inTx = (fn) => {
        const tx = db.transaction();
        try {
          fn(tx);
          tx.commit();
        } catch (err) {
          tx.rollback();
          throw err;
        }
      };
      assert.throws(() => inTx((tx) => tx.query('SELECT notify(?, ?)', ['orders', 'not json'])), NOT_JSON);
      assert.throws(
        () => inTx((tx) => tx.query('SELECT honker_stream_publish(?, NULL, ?)', ['orders-stream', 'not json'])),
        NOT_JSON,
      );
      assert.throws(() => db.stream('orders-stream').publish({ s: '\ud800' }), UNDECODABLE);

      db.notify('orders', { order_id: 1 });
      db.stream('orders-stream').publish({ order_id: 1 });
      assert.deepEqual(await listener.next('NOTE'), { payload: { order_id: 1 } });
      assert.deepEqual(await reader.next('EVENT'), { offset: 1, payload: { order_id: 1 } });
      assert.equal(await listener.exited, 0, listener.stderr);
      assert.equal(await reader.exited, 0, reader.stderr);
    });
  } finally {
    cleanup();
  }
});

test('legacy bad schedules are dead-lettered while two scheduler processes keep firing the good one', async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  try {
    const db = open(dbPath);
    const sched = db.scheduler();
    sched.add({ name: 'good', queue: 'sched-good', schedule: '@every 1s', payload: { ok: true } });
    const first = db.query("SELECT next_fire_at AS n FROM _honker_scheduler_tasks WHERE name = 'good'")[0].n;
    // Rows an older build or raw SQL could leave behind.
    const tx = db.transaction();
    for (const [name, queue, payload, maxAttempts] of [
      ['bad-payload', 'sched-bad', 'not json', 3],
      ['zero-attempts', 'sched-zero', '{"ok":true}', 0],
    ]) {
      tx.execute(
        'INSERT INTO _honker_scheduler_tasks (name, queue, cron_expr, payload, priority, next_fire_at, enabled, max_attempts)' +
          " VALUES (?, ?, '@every 1s', ?, 0, ?, 1, ?)",
        [name, queue, payload, first, maxAttempts],
      );
    }
    tx.commit();

    const snapshot = () => {
      const row = db.query(`SELECT
        (SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name = 'good') AS goodNext,
        (SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name = 'bad-payload') AS badNext,
        (SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name = 'zero-attempts') AS zeroNext,
        (SELECT COUNT(*) FROM _honker_live WHERE queue = 'sched-good') AS goodJobs,
        (SELECT COUNT(*) FROM _honker_live WHERE queue = 'sched-bad') AS badJobs,
        (SELECT COUNT(*) FROM _honker_live WHERE queue = 'sched-zero') AS zeroJobs,
        (SELECT json_group_array(payload) FROM (SELECT payload FROM _honker_live WHERE queue = 'sched-bad' ORDER BY id)) AS badPayloads,
        (SELECT json_group_array(json_object('run_at', run_at, 'error', last_error)) FROM (
           SELECT run_at, last_error FROM _honker_dead WHERE queue = 'sched-bad' ORDER BY run_at, id)) AS badDead,
        (SELECT json_group_array(json_object('run_at', run_at, 'error', last_error)) FROM (
           SELECT run_at, last_error FROM _honker_dead WHERE queue = 'sched-zero' ORDER BY run_at, id)) AS zeroDead`)[0];
      for (const k of ['badPayloads', 'badDead', 'zeroDead']) row[k] = JSON.parse(row[k]);
      return row;
    };

    await withJsonChildren(async (add) => {
      const procs = [1, 2].map((i) => add({ db: dbPath, role: 'scheduler', owner: `sched-${i}`, runMs: 12000 }));
      for (const p of procs) await p.next('READY');
      await delay(5000);

      const snap = snapshot();
      assert.deepEqual(procs.map((p) => p.exitCode), [null, null], procs.map((p) => p.stderr).join('\n'));
      const boundaries = snap.goodNext - first;
      assert.ok(boundaries >= 3, JSON.stringify(snap));
      assert.equal(snap.badNext, snap.goodNext);
      assert.equal(snap.zeroNext, snap.goodNext);
      assert.equal(snap.goodJobs, boundaries, 'the good schedule fires exactly once per boundary');
      assert.equal(snap.badJobs, 0);
      assert.equal(snap.zeroJobs, 0);
      const expected = Array.from({ length: boundaries }, (_, i) => first + i);
      assert.deepEqual(snap.badDead.map((d) => d.run_at), expected);
      assert.deepEqual(snap.zeroDead.map((d) => d.run_at), expected);
      for (const d of snap.badDead) {
        assert.match(d.error, NOT_JSON);
        assert.match(d.error, /schedule "bad-payload"/);
      }
      for (const d of snap.zeroDead) {
        assert.match(d.error, /max_attempts must be at least 1/);
        assert.match(d.error, /schedule "zero-attempts"/);
      }

      // Fix both through the public API while the schedulers run.
      assert.equal(sched.update('bad-payload', { payload: { fixed: true } }), true);
      assert.equal(sched.update('zero-attempts', { maxAttempts: 3 }), true);
      await delay(2500);

      const after = snapshot();
      assert.deepEqual(procs.map((p) => p.exitCode), [null, null], procs.map((p) => p.stderr).join('\n'));
      const total = after.goodNext - first;
      assert.equal(after.goodJobs, total);
      assert.ok(after.badJobs >= 1 && after.zeroJobs >= 1, JSON.stringify(after));
      assert.deepEqual([...new Set(after.badPayloads)], ['{"fixed":true}']);
      // Every boundary is accounted for once: a job or a dead row.
      assert.equal(after.badJobs + after.badDead.length, total);
      assert.equal(after.zeroJobs + after.zeroDead.length, total);

      for (const p of procs) {
        await p.next('STOPPED', 12000);
        assert.equal(await p.exited, 0, p.stderr);
      }
    });
  } finally {
    cleanup();
  }
});
