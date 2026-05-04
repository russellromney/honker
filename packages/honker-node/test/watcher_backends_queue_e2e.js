// Cross-process queue worker proof for watcher backends, Node side.
//
// Mirrors tests/test_watcher_backends_queue_e2e.py. Workers use
// q.claim(..., { idlePollS: null }) so the high-level fallback poll
// cannot make an experimental backend pass. The outer Promise timeout
// bounds the test; it is not a production wake source.

'use strict';

const { spawn, spawnSync } = require('node:child_process');
const path = require('node:path');
const test = require('node:test');
const assert = require('node:assert/strict');

const honker = require('..');
const { createTempDb } = require('./helpers');

const BACKENDS = [null, 'kernel', 'shm'];
const REQUIRE_HONKER = path.resolve(__dirname, '..');

function tmpdb() {
  return createTempDb('honker-node-queue-watchers-', honker.open.bind(honker));
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

function spawnNode(script) {
  return spawn(process.execPath, ['-e', script], {
    stdio: ['ignore', 'pipe', 'pipe'],
  });
}

function waitForLine(proc, predicate, timeoutMs) {
  let buf = '';
  const lines = [];
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`timeout waiting for child line after ${timeoutMs}ms`));
    }, timeoutMs);

    function check(line) {
      if (predicate(line)) {
        clearTimeout(timer);
        resolve(line);
        return true;
      }
      return false;
    }

    proc.stdout.on('data', (chunk) => {
      buf += chunk.toString('utf8');
      let nl;
      while ((nl = buf.indexOf('\n')) >= 0) {
        let line = buf.slice(0, nl);
        buf = buf.slice(nl + 1);
        if (line.endsWith('\r')) line = line.slice(0, -1);
        lines.push(line);
        if (check(line)) return;
      }
    });

    proc.once('exit', () => {
      const existing = lines.find(predicate);
      if (existing) {
        clearTimeout(timer);
        resolve(existing);
      }
    });
    proc.once('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });
}

async function waitWorker(proc, workerId, timeoutMs = 20000) {
  const stderr = [];
  proc.stderr.on('data', (chunk) => stderr.push(chunk.toString('utf8')));
  const line = await waitForLine(proc, (l) => l.startsWith('RESULT '), timeoutMs);
  const result = JSON.parse(line.slice('RESULT '.length));
  const exited = proc.exitCode ?? await Promise.race([
    new Promise((resolve) => proc.once('exit', (code) => resolve(code))),
    new Promise((resolve) => setTimeout(() => resolve(undefined), 1000)),
  ]);
  if (exited === undefined) {
    if (proc.kill()) {
      await new Promise((resolve) => proc.once('exit', resolve));
    }
  } else if (exited !== 0) {
    throw new Error(`worker ${workerId} exited ${exited}: ${stderr.join('')}`);
  }
  return result;
}

function workerScript(dbPath, workerId, backend, idleExitMs = 2000) {
  return `
    'use strict';
    const honker = require(${JSON.stringify(REQUIRE_HONKER)});
    const db = honker.open(${JSON.stringify(dbPath)}, undefined, ${JSON.stringify(backend)});
    const q = db.queue('shared');
    const waker = q.claimWaker({ idlePollS: null });
    let next = waker.next(${JSON.stringify(workerId)});
    console.log('READY');

    (async () => {
      const processed = [];
      while (true) {
        const result = await Promise.race([
          next,
          new Promise((resolve) => setTimeout(() => resolve({ timeout: true }), ${idleExitMs})),
        ]);
        if (result.timeout || result.done) break;
        if (!result) break;
        processed.push(result.payload.i);
        result.ack();
        next = waker.next(${JSON.stringify(workerId)});
      }
      waker.close();
      db.close();
      console.log('RESULT ' + JSON.stringify(processed));
    })().catch((err) => {
      console.error(err && err.stack || err);
      process.exit(1);
    });
  `;
}

async function spawnWorker(dbPath, workerId, backend) {
  const proc = spawnNode(workerScript(dbPath, workerId, backend));
  await waitForLine(proc, (line) => line === 'READY', 5000);
  return proc;
}

function runWriter(dbPath, n, offset = 0) {
  const script = `
    'use strict';
    const honker = require(${JSON.stringify(REQUIRE_HONKER)});
    const db = honker.open(${JSON.stringify(dbPath)});
    const q = db.queue('shared');
    for (let i = ${offset}; i < ${offset} + ${n}; i += 1) {
      q.enqueue({ i });
    }
    db.close();
  `;
  const res = spawnSync(process.execPath, ['-e', script], {
    encoding: 'utf8',
    timeout: 15000,
  });
  assert.equal(
    res.status,
    0,
    `writer failed: stdout=${res.stdout} stderr=${res.stderr}`,
  );
}

for (const backend of BACKENDS) {
  const label = backend === null ? 'polling (default)' : backend;

  test(`watcherBackend=${label} queue 1 writer / 1 worker`, async (t) => {
    const { path: dbPath, open, cleanup } = tmpdb();
    let worker;
    try {
      const db = openOrSkipBackend(t, open, dbPath, backend);
      if (!db) return;
      db.queue('shared');
      db.close();

      const n = 25;
      worker = await spawnWorker(dbPath, 'w1', backend);
      runWriter(dbPath, n);
      const processed = await waitWorker(worker, 'w1');
      assert.deepEqual(processed.sort((a, b) => a - b), Array.from({ length: n }, (_, i) => i));
    } finally {
      worker?.kill();
      cleanup();
    }
  });

  test(`watcherBackend=${label} queue 1 writer / many workers`, async (t) => {
    const { path: dbPath, open, cleanup } = tmpdb();
    const workers = [];
    try {
      const db = openOrSkipBackend(t, open, dbPath, backend);
      if (!db) return;
      db.queue('shared');
      db.close();

      const n = 60;
      for (let i = 0; i < 3; i += 1) {
        workers.push(await spawnWorker(dbPath, `w${i}`, backend));
      }
      runWriter(dbPath, n);
      const results = await Promise.all(workers.map((w, i) => waitWorker(w, `w${i}`)));
      const combined = results.flat().sort((a, b) => a - b);
      assert.deepEqual(combined, Array.from({ length: n }, (_, i) => i));
      for (let i = 0; i < results.length; i += 1) {
        for (let j = i + 1; j < results.length; j += 1) {
          const overlap = results[i].filter((value) => results[j].includes(value));
          assert.deepEqual(overlap, [], `workers w${i} and w${j} double-claimed`);
        }
      }
    } finally {
      for (const worker of workers) worker.kill();
      cleanup();
    }
  });

  test(`watcherBackend=${label} queue many writers / 1 worker`, async (t) => {
    const { path: dbPath, open, cleanup } = tmpdb();
    let worker;
    try {
      const db = openOrSkipBackend(t, open, dbPath, backend);
      if (!db) return;
      db.queue('shared');
      db.close();

      const writers = 3;
      const perWriter = 20;
      worker = await spawnWorker(dbPath, 'solo', backend);
      for (let i = 0; i < writers; i += 1) {
        runWriter(dbPath, perWriter, i * perWriter);
      }
      const processed = await waitWorker(worker, 'solo');
      assert.deepEqual(
        processed.sort((a, b) => a - b),
        Array.from({ length: writers * perWriter }, (_, i) => i),
      );
    } finally {
      worker?.kill();
      cleanup();
    }
  });
}
