// Worker-loop scenarios through the public Node API, one OS process per
// worker, on a file-backed WAL database with the real clock.
//
// Mirrors the C1-C9 scenarios in tests/test_real_e2e_scenarios.py so
// both bindings answer the same user questions: does a stale owner stay
// out, does cancel reach a running job, does a worker survive a long
// write lock, does a lease hold under heartbeats, does the scheduler
// fire once per boundary across processes. Known binding bugs are
// pinned with knownBug(): the test passes while the bug reproduces and
// fails with XPASS once it is fixed.

'use strict';

const { spawn } = require('node:child_process');
const path = require('node:path');
const test = require('node:test');
const assert = require('node:assert/strict');
const { setTimeout: delay } = require('node:timers/promises');

const honker = require('..');
const { createTempDb, knownBug } = require('./helpers');

const REQUIRE_HONKER = path.resolve(__dirname, '..');
const SKIP_POSIX = process.platform === 'win32' ? 'uses SIGKILL/SIGTERM' : false;

function tmpdb() {
  return createTempDb('honker-node-scenarios-', honker.open.bind(honker));
}

// One child script, role picked by cfg.role. Children print one line per
// event: `<TAG> <json>`.
const CHILD = `
'use strict';
const honker = require(${JSON.stringify(REQUIRE_HONKER)});
const cfg = JSON.parse(process.argv[1]);
const db = honker.open(cfg.db);
const out = (tag, obj) => console.log(tag + ' ' + JSON.stringify(obj || {}));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const ac = new AbortController();
process.on('SIGTERM', () => ac.abort());
const vis = cfg.vis || 30;
const q = db.queue(cfg.queue, { visibilityTimeoutS: vis });

async function run(p, job) {
  const end = Date.now() + ((p && p.sleep_s) || 0) * 1000;
  while (Date.now() < end) {
    await sleep(Math.min(250, Math.max(0, end - Date.now())));
    if (p.heartbeat) out('HB', { key: p.key, ok: job.heartbeat(vis) });
  }
  if (p && p.fail) throw new Error('handler failed');
}

function key(p) {
  return p && typeof p === 'object' ? p.key : null;
}

async function claimLoop(slot) {
  const workerId = cfg.workerId + (cfg.loops > 1 ? '-' + slot : '');
  for await (const job of q.claim(workerId, { signal: ac.signal })) {
    out('START', { key: key(job.payload), id: job.id, attempts: job.attempts, type: typeof job.payload });
    try {
      await run(job.payload, job);
    } catch (err) {
      out('RETRY', { key: key(job.payload), ok: job.retry(1, err.message) });
      continue;
    }
    out('END', { key: key(job.payload), ack: job.ack() });
  }
}

async function main() {
  if (cfg.role === 'worker') {
    let loops;
    if (cfg.mode === 'outbox') {
      const outbox = new honker.Outbox(db, cfg.queue, async (p, job) => {
        out('START', { key: key(p), id: job.id, attempts: job.attempts, type: typeof p });
        await run(p, job);
        out('END', { key: key(p) });
      }, { visibilityTimeoutS: vis, baseBackoffS: 1 });
      loops = [outbox.runWorker(cfg.workerId, { signal: ac.signal })];
    } else {
      loops = Array.from({ length: cfg.loops || 1 }, (_, i) => claimLoop(i));
    }
    out('READY');
    if (cfg.cpuAfterS) {
      await sleep(1000);
      const before = process.cpuUsage();
      await sleep(cfg.cpuAfterS * 1000);
      const used = process.cpuUsage(before);
      out('CPU', { seconds: (used.user + used.system) / 1e6 });
    }
    await Promise.all(loops);
  } else if (cfg.role === 'lifecycle') {
    // Claim one job, report it, then run whatever op the parent sends.
    let job = null;
    while (!job) {
      job = q.claimOne(cfg.workerId);
      if (!job) await sleep(50);
    }
    out('CLAIMED', { id: job.id, attempts: job.attempts, claimExpiresAt: job.claimExpiresAt });
    const op = await new Promise((resolve) => {
      let buf = '';
      process.stdin.on('data', (c) => {
        buf += c;
        if (buf.includes('\\n')) resolve(buf.trim());
      });
    });
    const ops = {
      ack: () => job.ack(),
      retry: () => job.retry(0, 'stale retry'),
      fail: () => job.fail('stale fail'),
      heartbeat: () => job.heartbeat(60),
    };
    out('RESULT', { ok: ops[op]() });
    process.exit(0);
  } else if (cfg.role === 'scheduler') {
    out('READY');
    await db.scheduler().run(cfg.owner, ac.signal);
  }
  db.close();
}

main().catch((err) => {
  out('DIED', { message: String(err && err.message || err) });
  process.exit(1);
});
`;

class Child {
  constructor(cfg) {
    this.lines = [];
    this.stderr = '';
    this.waiters = new Set();
    this.exitCode = null;
    this.proc = spawn(process.execPath, ['-e', CHILD, JSON.stringify(cfg)], {
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let buf = '';
    this.proc.stdout.on('data', (chunk) => {
      buf += chunk.toString('utf8');
      let nl;
      while ((nl = buf.indexOf('\n')) >= 0) {
        const raw = buf.slice(0, nl).replace(/\r$/, '');
        buf = buf.slice(nl + 1);
        const sp = raw.indexOf(' ');
        const ev = { tag: raw.slice(0, sp), ...JSON.parse(raw.slice(sp + 1)) };
        this.lines.push(ev);
        for (const w of this.waiters) w();
      }
    });
    this.proc.stderr.on('data', (c) => {
      this.stderr += c.toString('utf8');
    });
    this.exited = new Promise((resolve) => {
      this.proc.once('exit', (code, signal) => {
        this.exitCode = code ?? signal;
        for (const w of this.waiters) w();
        resolve(this.exitCode);
      });
    });
  }

  events(tag, k) {
    return this.lines.filter((e) => e.tag === tag && (k === undefined || e.key === k));
  }

  // Resolves with the first event matching `pred`. A timeout or the
  // child exiting first is an AssertionError, so knownBug() can tell a
  // reproduced bug from an unrelated crash.
  waitFor(pred, timeoutMs, what) {
    return new Promise((resolve, reject) => {
      let timer;
      const check = () => {
        const hit = this.lines.find(pred);
        if (hit) {
          done();
          resolve(hit);
        } else if (this.exitCode !== null) {
          done();
          reject(new assert.AssertionError({
            message: `child exited ${this.exitCode} before ${what}: ` +
              `${JSON.stringify(this.events('DIED'))} ${this.stderr.slice(-500)}`,
          }));
        }
      };
      const done = () => {
        clearTimeout(timer);
        this.waiters.delete(check);
      };
      timer = setTimeout(() => {
        done();
        reject(new assert.AssertionError({
          message: `timed out after ${timeoutMs} ms waiting for ${what}; ` +
            `events=${JSON.stringify(this.lines.slice(-10))} ${this.stderr.slice(-500)}`,
        }));
      }, timeoutMs);
      this.waiters.add(check);
      check();
    });
  }

  send(line) {
    this.proc.stdin.write(line + '\n');
  }

  async stop() {
    if (this.exitCode === null) this.proc.kill('SIGTERM');
    const code = await Promise.race([this.exited, delay(5000).then(() => 'timeout')]);
    if (code === 'timeout') {
      this.proc.kill('SIGKILL');
      await this.exited;
    }
    return code;
  }

  async kill() {
    if (this.exitCode === null) this.proc.kill('SIGKILL');
    await this.exited;
  }
}

async function startWorker(dbPath, cfg) {
  const child = new Child({ db: dbPath, role: 'worker', workerId: 'w1', ...cfg });
  await child.waitFor((e) => e.tag === 'READY', 10000, 'READY');
  return child;
}

async function withChildren(fn) {
  const children = [];
  try {
    await fn((child) => {
      children.push(child);
      return child;
    });
  } finally {
    await Promise.all(children.map((c) => c.kill()));
  }
}

// ---------------------------------------------------------------------
// C1: a stale owner cannot touch a job that was reclaimed.
// ---------------------------------------------------------------------

async function staleOwnerScenario(dbPath, open, op, reclaimAs) {
  const db = open(dbPath);
  const q = db.queue('c1', { visibilityTimeoutS: 1 });
  const jid = q.enqueue({ key: 'job' });
  // The reclaiming handle holds its own claim for 30 s.
  const reclaimer = db.queue('c1', { visibilityTimeoutS: 30 });
  const stale = new Child({ db: dbPath, role: 'lifecycle', queue: 'c1', vis: 1, workerId: 'w1' });
  try {
    const claimed = await stale.waitFor((e) => e.tag === 'CLAIMED', 10000, 'CLAIMED');
    assert.equal(claimed.id, jid);
    // Lease expiry is strict at one-second resolution.
    await delay(Math.max(0, (claimed.claimExpiresAt + 1.05) * 1000 - Date.now()));
    const fresh = reclaimer.claimOne(reclaimAs);
    assert.ok(fresh, 'the lapsed lease must be reclaimable');
    assert.equal(fresh.attempts, 2);

    stale.send(op);
    const result = await stale.waitFor((e) => e.tag === 'RESULT', 10000, 'RESULT');
    const row = q.getJob(jid);
    assert.equal(result.ok, false, `stale ${op} must return false`);
    assert.equal(row && row.state, 'processing', `stale ${op} must leave attempt 2 alone`);
    assert.equal(row.attempts, 2);
    assert.equal(fresh.ack(), true, 'the new owner still owns the job');
  } finally {
    await stale.kill();
  }
}

for (const op of ['ack', 'retry', 'fail', 'heartbeat']) {
  test(`C1: stale owner's ${op} after another worker reclaimed the job returns false`, { skip: SKIP_POSIX }, async () => {
    const { path: dbPath, open, cleanup } = tmpdb();
    try {
      await staleOwnerScenario(dbPath, open, op, 'w2');
    } finally {
      cleanup();
    }
  });

  test(`C1: stale owner's ${op} with the same worker id cannot touch the new attempt [known bug #176]`, { skip: SKIP_POSIX }, async (t) => {
    const { path: dbPath, open, cleanup } = tmpdb();
    try {
      await knownBug(
        t,
        '#176: unfenced ack/retry/fail/heartbeat check worker_id plus an unexpired lease, not the attempt; core fencing landed in #179, the Node binding does not use it yet (#124)',
        () => staleOwnerScenario(dbPath, open, op, 'w1'),
      );
    } finally {
      cleanup();
    }
  });
}

// ---------------------------------------------------------------------
// C2: cancelling a running job is dropped and the worker keeps going.
// ---------------------------------------------------------------------

async function cancelInFlightScenario(dbPath, open, mode, add) {
  const db = open(dbPath);
  const q = mode === 'outbox'
    ? new honker.Outbox(db, 'c2', () => {}).queue
    : db.queue('c2');
  const worker = add(await startWorker(dbPath, { queue: 'c2', mode }));
  const slow = q.enqueue({ key: 'slow', sleep_s: 1.5 });
  await worker.waitFor((e) => e.tag === 'START' && e.key === 'slow', 10000, 'slow START');
  assert.equal(q.cancel(slow), true);
  const next = q.enqueue({ key: 'next' });
  await worker.waitFor((e) => e.tag === 'END' && e.key === 'next', 10000, 'the next job to finish');
  assert.equal(q.getJob(next), null);
  assert.equal(q.getJob(slow), null);
  assert.equal(worker.events('START', 'slow').length, 1, 'a cancelled job must not run again');
  assert.equal(worker.exitCode, null, 'the worker must still be running');
  return worker;
}

test('C2: cancel of an in-flight job is dropped and a claim loop keeps working', { skip: SKIP_POSIX }, async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  try {
    await withChildren(async (add) => {
      const worker = await cancelInFlightScenario(dbPath, open, 'claim', add);
      assert.deepEqual(worker.events('END', 'slow').map((e) => e.ack), [false]);
    });
  } finally {
    cleanup();
  }
});

test('C2: cancel of an in-flight job does not stop Outbox.runWorker [known bug #186]', { skip: SKIP_POSIX }, async (t) => {
  const { path: dbPath, open, cleanup } = tmpdb();
  try {
    await withChildren(async (add) => {
      await knownBug(
        t,
        'Outbox.runWorker throws when ack returns false, then retry also returns false and the loop exits with "outbox retry failed"; one cancelled or reclaimed job stops the worker (#186)',
        () => cancelInFlightScenario(dbPath, open, 'outbox', add),
      );
    });
  } finally {
    cleanup();
  }
});

// ---------------------------------------------------------------------
// C3: a writer lock held by another process.
// ---------------------------------------------------------------------

async function writeLockScenario(dbPath, open, holdS, add) {
  const db = open(dbPath);
  const q = db.queue('c3');
  const setup = db.transaction();
  setup.execute('CREATE TABLE IF NOT EXISTS app_lock (n INTEGER)');
  setup.commit();
  const worker = add(await startWorker(dbPath, { queue: 'c3' }));
  // Due in 2 s, so the worker's claim lands inside the lock window.
  const jid = q.enqueue({ key: 'late' }, { delay: 2 });
  const tx = db.transaction();
  tx.execute('INSERT INTO app_lock VALUES (1)');
  await delay(holdS * 1000);
  tx.commit();
  await worker.waitFor((e) => e.tag === 'END' && e.key === 'late', 15000, 'the job after the lock');
  assert.equal(q.getJob(jid), null);
  assert.equal(worker.exitCode, null, 'the worker must still be running');
}

test('C3: a worker waits out a 3 s write lock and then runs the job', { skip: SKIP_POSIX }, async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  try {
    await withChildren((add) => writeLockScenario(dbPath, open, 3, add));
  } finally {
    cleanup();
  }
});

test('C3: a claim loop survives a write lock longer than busy_timeout [known bug #186]', { skip: SKIP_POSIX, timeout: 40000 }, async (t) => {
  const { path: dbPath, open, cleanup } = tmpdb();
  try {
    await withChildren(async (add) => {
      await knownBug(
        t,
        'ClaimWaker.next lets SQLITE_BUSY from claimOne escape after busy_timeout (5 s), so the q.claim() iterator throws and the worker loop ends; #186, see also #184',
        () => writeLockScenario(dbPath, open, 7, add),
      );
    });
  } finally {
    cleanup();
  }
});

// ---------------------------------------------------------------------
// C4: leases and heartbeats for a handler longer than its lease.
// ---------------------------------------------------------------------

for (const heartbeat of [true, false]) {
  test(`C4: a handler twice its 2 s lease ${heartbeat ? 'with' : 'without'} heartbeats`, { skip: SKIP_POSIX, timeout: 30000 }, async () => {
    const { path: dbPath, open, cleanup } = tmpdb();
    try {
      await withChildren(async (add) => {
        const db = open(dbPath);
        const q = db.queue('c4', { maxAttempts: 2 });
        const a = add(await startWorker(dbPath, { queue: 'c4', vis: 2, workerId: 'a' }));
        const b = add(await startWorker(dbPath, { queue: 'c4', vis: 2, workerId: 'b' }));
        const jid = q.enqueue({ key: 'long', sleep_s: 4.5, heartbeat });
        const ends = () => [...a.events('END', 'long'), ...b.events('END', 'long')];
        const deadline = Date.now() + 20000;
        while (ends().length < (heartbeat ? 1 : 2) && Date.now() < deadline) await delay(100);
        await delay(heartbeat ? 500 : 2500); // let any extra run show up
        const starts = [...a.events('START', 'long'), ...b.events('START', 'long')];
        assert.equal(q.getJob(jid), null);
        const dead = db.query('SELECT last_error FROM _honker_dead');
        if (heartbeat) {
          assert.equal(starts.length, 1, 'heartbeats keep a single owner');
          assert.deepEqual(ends().map((e) => e.ack), [true]);
          assert.deepEqual(dead, []);
        } else {
          // Without heartbeats the lease lapses and the other worker
          // runs the job again. Neither overrunning run may ack, and
          // once attempts are spent the job is dead-lettered.
          assert.deepEqual(starts.map((s) => s.attempts).sort(), [1, 2]);
          assert.deepEqual(ends().map((e) => e.ack), [false, false]);
          assert.deepEqual(dead, [{ last_error: 'max attempts exceeded' }]);
        }
      });
    } finally {
      cleanup();
    }
  });
}

// ---------------------------------------------------------------------
// C6: idle workers cost almost nothing.
// ---------------------------------------------------------------------

test('C6: an idle worker with four claim loops uses almost no CPU', { skip: SKIP_POSIX }, async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  try {
    await withChildren(async (add) => {
      open(dbPath).queue('c6').enqueue({ key: 'warm' });
      const w = add(await startWorker(dbPath, { queue: 'c6', loops: 4, cpuAfterS: 5 }));
      const cpu = await w.waitFor((e) => e.tag === 'CPU', 15000, 'CPU report');
      assert.ok(cpu.seconds < 0.5, `idle worker used ${cpu.seconds} s CPU in 5 s`);
    });
  } finally {
    cleanup();
  }
});

// ---------------------------------------------------------------------
// C7: two scheduler processes fire each boundary once.
// ---------------------------------------------------------------------

test('C7: two scheduler processes fire each @every 1s boundary once', { skip: SKIP_POSIX, timeout: 30000 }, async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  try {
    await withChildren(async (add) => {
      const db = open(dbPath);
      const sched = db.scheduler();
      sched.add({ name: 'beat', queue: 'c7', cron: '@every 1s', payload: { beat: true } });
      const nextFire = () => db.query("SELECT next_fire_at AS n FROM _honker_scheduler_tasks WHERE name='beat'")[0].n;
      const first = nextFire();
      const s1 = add(new Child({ db: dbPath, role: 'scheduler', queue: 'c7', owner: 's1' }));
      const s2 = add(new Child({ db: dbPath, role: 'scheduler', queue: 'c7', owner: 's2' }));
      await Promise.all([s1, s2].map((s) => s.waitFor((e) => e.tag === 'READY', 10000, 'READY')));
      await delay(8000);
      assert.equal(await s1.stop(), 0);
      assert.equal(await s2.stop(), 0);
      const fired = nextFire() - first;
      const jobs = db.query("SELECT COUNT(*) AS c FROM _honker_live WHERE queue='c7'")[0].c;
      assert.ok(fired >= 6, `only ${fired} boundaries in 8 s`);
      assert.equal(jobs, fired, 'one job per boundary, never two');
    });
  } finally {
    cleanup();
  }
});

// ---------------------------------------------------------------------
// C8 / C9: what reaches a worker.
// ---------------------------------------------------------------------

test('C8: a rolled-back enqueueTx never reaches a worker', { skip: SKIP_POSIX }, async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  try {
    await withChildren(async (add) => {
      const db = open(dbPath);
      const q = db.queue('c8');
      const w = add(await startWorker(dbPath, { queue: 'c8' }));
      const tx = db.transaction();
      q.enqueueTx(tx, { key: 'ghost' });
      tx.rollback();
      q.enqueue({ key: 'real' });
      await w.waitFor((e) => e.tag === 'END' && e.key === 'real', 10000, 'real END');
      await delay(500);
      assert.deepEqual(w.events('START').map((e) => e.key), ['real']);
    });
  } finally {
    cleanup();
  }
});

test('C9: a legacy non-JSON row fails alone; jobs behind it still run [known bug: decoded as raw text]', { skip: SKIP_POSIX }, async (t) => {
  const { path: dbPath, open, cleanup } = tmpdb();
  try {
    await withChildren(async (add) => {
      const db = open(dbPath);
      const q = db.queue('c9');
      // Written straight to the table, as a row from before #153 would be.
      const tx = db.transaction();
      tx.execute("INSERT INTO _honker_live (queue, payload, priority) VALUES ('c9', 'not json', 10)");
      tx.commit();
      q.enqueue({ key: 'good-0' });
      q.enqueue({ key: 'good-1' });
      const w = add(await startWorker(dbPath, { queue: 'c9' }));
      await w.waitFor((e) => e.tag === 'END' && e.key === 'good-1', 10000, 'good-1 END');
      await w.waitFor((e) => e.tag === 'END' && e.key === 'good-0', 10000, 'good-0 END');
      assert.equal(w.exitCode, null);
      await knownBug(
        t,
        '#152 read side: Job.payload falls back to the raw string when JSON.parse fails, so the handler gets "not json" typed as the job payload instead of the job failing loudly',
        () => {
          assert.deepEqual(
            w.events('START').filter((e) => e.type !== 'object').map((e) => e.type),
            [],
            'a handler must never see an undecodable payload',
          );
        },
      );
    });
  } finally {
    cleanup();
  }
});

// ---------------------------------------------------------------------
// #177 / #180: an expired job held by a dead worker never runs again.
// ---------------------------------------------------------------------

test('an expired job held by a killed worker is dead-lettered, not rerun', { skip: SKIP_POSIX, timeout: 30000 }, async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  try {
    await withChildren(async (add) => {
      const db = open(dbPath);
      const q = db.queue('expiring');
      const jid = q.enqueue({ key: 'expiring' }, { expires: 3 });
      // The lease (5 s) outlives the job's expiry (3 s): once the lease
      // lapses the job must be dead-lettered, not handed to the rescuer.
      const crashed = add(new Child({ db: dbPath, role: 'lifecycle', queue: 'expiring', vis: 5, workerId: 'crashy' }));
      assert.equal((await crashed.waitFor((e) => e.tag === 'CLAIMED', 10000, 'CLAIMED')).id, jid);
      await crashed.kill();
      q.enqueue({ key: 'keeper' }, { delay: 8 });
      const rescuer = add(await startWorker(dbPath, { queue: 'expiring', workerId: 'rescuer' }));
      await rescuer.waitFor((e) => e.tag === 'END' && e.key === 'keeper', 15000, 'keeper END');
      assert.deepEqual(rescuer.events('START').map((e) => e.key), ['keeper']);
      assert.deepEqual(
        db.query('SELECT id, last_error FROM _honker_dead'),
        [{ id: jid, last_error: 'expired' }],
      );
    });
  } finally {
    cleanup();
  }
});
