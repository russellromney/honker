'use strict';

const { EventEmitter } = require('node:events');
const { setTimeout: delay } = require('node:timers/promises');

function scalar(rows) {
  if (!Array.isArray(rows) || rows.length === 0) return null;
  const row = rows[0];
  if (!row || typeof row !== 'object') return null;
  const keys = Object.keys(row);
  return keys.length === 0 ? null : row[keys[0]];
}

function parseJson(text) {
  if (text == null) return null;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function jsonText(value) {
  return JSON.stringify(value);
}

function nowUnix() {
  return Math.floor(Date.now() / 1000);
}

function monotonicMs() {
  return performance.now();
}

function aborted(signal) {
  return Boolean(signal?.aborted);
}

function abortPromise(signal) {
  // Every caller must cancel() once its race settles: an { once: true }
  // listener is only removed by the platform when abort actually fires,
  // so without cancel() each poll timeout would leak one listener on
  // the signal until it aborts (issue #67).
  if (!signal) return { promise: new Promise(() => {}), cancel() {} };
  if (signal.aborted) return { promise: Promise.resolve(), cancel() {} };
  let onAbort;
  const promise = new Promise((resolve) => {
    onAbort = resolve;
    signal.addEventListener('abort', resolve, { once: true });
  });
  return {
    promise,
    cancel() {
      signal.removeEventListener('abort', onAbort);
    },
  };
}

async function waitForUpdateOrTimeout(updateEvents, signal, timeoutMs) {
  if (aborted(signal)) return;
  // A dead watcher never fires again (UpdateEvents._settle records
  // native-wait failures): skip the event wait and degrade to plain
  // poll cadence instead of hot-looping on instantly-rejecting waits.
  const wait = updateEvents._dead == null ? updateEvents._subscribe() : null;
  // An event-only wait (timeoutMs == null) on a dead watcher could
  // never settle — not even close() would wake it, since there is no
  // parked waiter left to reject. Degrade it to a 1 s cadence so the
  // wait loops keep re-checking their closed/abort flags.
  const effectiveMs = wait == null && timeoutMs == null ? 1000 : timeoutMs;
  const onAbort = abortPromise(signal);
  try {
    // Waiter promises reject on watcher death / close(); internal wait
    // loops treat that as an ordinary wake (they re-check their closed
    // flags and exit), so swallow it here.
    const racers = [];
    if (wait) racers.push(wait.promise.catch(() => undefined));
    if (effectiveMs != null) racers.push(delay(Math.max(0, effectiveMs)));
    racers.push(onAbort.promise);
    await Promise.race(racers);
  } finally {
    if (wait) wait.cancel();
    onAbort.cancel();
  }
}

function unwrapTx(tx) {
  return tx instanceof Transaction ? tx._tx : tx;
}

class CheckpointMigrationError extends Error {
  constructor(stream, consumer, offset) {
    super(
      `Cannot automatically migrate the Node 0.4.6 checkpoint for stream ` +
        `${JSON.stringify(stream)} and consumer ${JSON.stringify(consumer)}: ` +
        `legacy offset ${offset} is not a retained event in that stream. ` +
        `Call saveOffset(consumer, 0) to reset it, or explicitly save a known offset.`,
    );
    this.name = 'CheckpointMigrationError';
    this.code = 'HONKER_CHECKPOINT_MIGRATION_UNVERIFIABLE';
    this.stream = stream;
    this.consumer = consumer;
    this.offset = offset;
  }
}

class QueueEventOffsetExpiredError extends Error {
  constructor(requestedOffset, trimmedThroughOffset, oldestAvailableOffset) {
    super(
      `Queue-event offset ${requestedOffset} is no longer retained; ` +
        `events through offset ${trimmedThroughOffset} were trimmed. ` +
        `Start without fromOffset to consume from the oldest retained event.`,
    );
    this.name = 'QueueEventOffsetExpiredError';
    this.code = 'HONKER_QUEUE_EVENT_OFFSET_EXPIRED';
    this.requestedOffset = requestedOffset;
    this.trimmedThroughOffset = trimmedThroughOffset;
    this.oldestAvailableOffset = oldestAvailableOffset;
  }
}

class QueueEventsDisabledError extends Error {
  constructor() {
    super(
      'Queue events are disabled; call configureQueueEvents({ enabled: true }) first.',
    );
    this.name = 'QueueEventsDisabledError';
    this.code = 'HONKER_QUEUE_EVENTS_DISABLED';
  }
}

class Transaction {
  constructor(tx) {
    this._tx = tx;
  }

  raw() {
    return this._tx;
  }

  execute(sql, params) {
    return this._tx.execute(sql, params);
  }

  query(sql, params) {
    return this._tx.query(sql, params);
  }

  notify(channel, payload) {
    return this._tx.notify(channel, payload);
  }

  commit() {
    this._tx.commit();
  }

  rollback() {
    this._tx.rollback();
  }
}

class UpdateEvents {
  constructor(ev) {
    this._ev = ev;
    this._closed = false;
    // One pending native wait shared by every concurrent waiter. The
    // native next() parks a Tokio blocking-pool thread on recv() until
    // the next commit or close(), and it cannot be cancelled — so every
    // abandoned next() (Promise.race losers, in particular) would pin
    // one more OS thread for the life of an idle database. Sharing the
    // wait means N concurrent next() calls cost one thread total.
    //
    // While no commit arrives, the single pending wait holds its one
    // thread even between waker.next() calls (e.g. during job
    // processing). That residual thread is deliberate: the native wait
    // has no cancellation, and the alternative is a thread per wait.
    this._pending = null;
    this._waiters = new Set();
    // Set when the native wait fails (watcher death — close() is
    // checked first and takes precedence). The subscription can never
    // fire again: the shared watcher's sender is gone, and a fresh
    // native next() would reject instantly. Internal wait loops check
    // this to degrade to poll cadence instead of hot-looping.
    this._dead = null;
  }

  raw() {
    return this._ev;
  }

  // Internal: subscribe to the shared native wait. The promise
  // RESOLVES on the next update and REJECTS when the native wait fails
  // (watcher death, close()) — same contract as awaiting next().
  // Callers that race the returned promise MUST cancel() when their
  // race settles another way — otherwise each abandoned race leaves
  // its waiter in the set until the wait settles (bounded by poll
  // rate × idle time; waitForUpdateOrTimeout cancels, keeping the set
  // at one entry per in-flight wait).
  _subscribe() {
    if (this._closed) {
      return { promise: Promise.resolve(), cancel() {} };
    }
    if (this._dead != null) {
      return { promise: Promise.reject(this._dead), cancel() {} };
    }
    if (!this._pending) {
      const pending = this._ev.next().then(
        () => this._settle(pending, null),
        (err) => this._settle(pending, err),
      );
      this._pending = pending;
    }
    let waiter;
    const promise = new Promise((resolve, reject) => {
      waiter = { resolve, reject };
    });
    this._waiters.add(waiter);
    return {
      promise,
      cancel: () => {
        this._waiters.delete(waiter);
      },
    };
  }

  _settle(pending, err) {
    if (this._pending !== pending) return;
    this._pending = null;
    if (err != null) this._dead = err;
    const waiters = [...this._waiters];
    this._waiters.clear();
    for (const w of waiters) {
      if (err == null) w.resolve();
      else w.reject(err);
    }
  }

  /** Wait for the next database update. Resolves on the next commit;
   *  rejects when the watcher dies or close() cuts the subscription.
   *  Concurrent calls share one native wait and settle together — a
   *  wake is a "go re-read state" hint, not a per-caller event — so
   *  racing next() against your own timeout never starts extra native
   *  waits. */
  async next() {
    await this._subscribe().promise;
  }

  close() {
    if (this._closed) return;
    this._closed = true;
    this._ev.close();
  }
}

class Lock {
  constructor(db, name, owner) {
    this._db = db;
    this.name = name;
    this.owner = owner;
  }

  release() {
    return (
      this._db._callScalar('SELECT honker_lock_release(?, ?)', [
        this.name,
        this.owner,
      ]) === 1
    );
  }

  heartbeat(ttlS) {
    // honker_lock_acquire uses INSERT OR IGNORE and does not refresh
    // expires_at for an existing owner. Use honker_lock_renew.
    return (
      this._db._callScalar('SELECT honker_lock_renew(?, ?, ?)', [
        this.name,
        this.owner,
        ttlS,
      ]) === 1
    );
  }
}

class Job {
  constructor(queue, row) {
    this._queue = queue;
    this.id = row.id;
    this.queue = row.queue;
    this.payload = parseJson(row.payload);
    this.state = row.state;
    this.priority = row.priority;
    this.runAt = row.run_at;
    this.workerId = row.worker_id;
    this.attempts = row.attempts;
    this.claimExpiresAt = row.claim_expires_at ?? null;
    this.maxAttempts = row.max_attempts;
    this.createdAt = row.created_at;
    this.expiresAt = row.expires_at ?? null;
  }

  ack() {
    return this._queue._ack(this.id, this.workerId);
  }

  retry(delayS = 60, error = '') {
    return this._queue._retry(this.id, this.workerId, delayS, error);
  }

  fail(error = '') {
    return this._queue._fail(this.id, this.workerId, error);
  }

  heartbeat(extendS) {
    return this._queue._heartbeat(this.id, this.workerId, extendS);
  }
}

// How long the claim waker waits before retrying a deadline it reached but
// could not claim. Short enough that a contended claim recovers promptly,
// long enough not to spin on the writer lock.
const DEADLINE_RETRY_MS = 50;

class ClaimWaker {
  constructor(queue, { idlePollS = 5 } = {}) {
    this._queue = queue;
    this._idlePollMs = idlePollS == null ? null : Math.max(0, idlePollS * 1000);
    this._updates = queue._db.updateEvents();
    this._closed = false;
  }

  async next(workerId, opts = {}) {
    if (this._closed || aborted(opts.signal)) return null;

    let job = this._queue.claimOne(workerId);
    if (job) return job;

    // True once we have waited on a run_at/claim_expires_at deadline that
    // has since passed. next_claim_at only reports deadlines with
    // run_at > unixepoch(), so it returns 0 the moment one arrives — and
    // without this, a claim that came back empty right then (a writer lock
    // held past busy_timeout, a racing worker) would fall through to
    // idlePollS and park for the whole interval on a queue that has work
    // ready. Retry briefly instead; a genuinely empty queue never sets it.
    let deadlinePassed = false;

    while (!this._closed && !aborted(opts.signal)) {
      const nextClaimAt = this._queue._nextClaimAt();
      let waitMs = this._idlePollMs;
      if (nextClaimAt && nextClaimAt > 0) {
        const untilDeadline = Math.max(0, nextClaimAt * 1000 - Date.now());
        waitMs = waitMs == null ? untilDeadline : Math.min(waitMs, untilDeadline);
        deadlinePassed = true;
      } else if (deadlinePassed) {
        waitMs = waitMs == null ? DEADLINE_RETRY_MS : Math.min(waitMs, DEADLINE_RETRY_MS);
      }
      await waitForUpdateOrTimeout(this._updates, opts.signal, waitMs);
      if (this._closed || aborted(opts.signal)) return null;
      job = this._queue.claimOne(workerId);
      if (job) return job;
    }

    return null;
  }

  close() {
    if (this._closed) return;
    this._closed = true;
    this._updates.close();
  }
}

class Queue {
  constructor(db, name, { visibilityTimeoutS = 300, maxAttempts = 3 } = {}) {
    this._db = db;
    this.name = name;
    this.visibilityTimeoutS = visibilityTimeoutS;
    this.maxAttempts = maxAttempts;
  }

  enqueue(payload, opts = {}) {
    if (opts.tx) return this.enqueueTx(opts.tx, payload, opts);
    return this._db._callScalar('SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?) AS id', [
      this.name,
      jsonText(payload),
      opts.runAt ?? null,
      opts.delay ?? null,
      opts.priority ?? 0,
      this.maxAttempts,
      opts.expires ?? null,
    ]);
  }

  enqueueTx(tx, payload, opts = {}) {
    return scalar(
      unwrapTx(tx).query('SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?) AS id', [
        this.name,
        jsonText(payload),
        opts.runAt ?? null,
        opts.delay ?? null,
        opts.priority ?? 0,
        this.maxAttempts,
        opts.expires ?? null,
      ]),
    );
  }

  claimBatch(workerId, n) {
    const rowsJson = this._db._callScalar('SELECT honker_claim_batch(?, ?, ?, ?)', [
      this.name,
      workerId,
      n,
      this.visibilityTimeoutS,
    ]);
    return JSON.parse(rowsJson).map((row) => new Job(this, row));
  }

  claimOne(workerId) {
    return this.claimBatch(workerId, 1)[0] ?? null;
  }

  async *claim(workerId, opts = {}) {
    const waker = this.claimWaker(opts);
    try {
      while (true) {
        const job = await waker.next(workerId, opts);
        if (!job) return;
        yield job;
      }
    } finally {
      waker.close();
    }
  }

  ackBatch(ids, workerId) {
    return this._db._callScalar('SELECT honker_ack_batch(?, ?)', [jsonText(ids), workerId]);
  }

  sweepExpired() {
    return this._db._callScalar('SELECT honker_sweep_expired(?)', [this.name]);
  }

  claimWaker(opts = {}) {
    return new ClaimWaker(this, opts);
  }

  _nextClaimAt() {
    return this._db._callScalar('SELECT honker_queue_next_claim_at(?)', [this.name]);
  }

  _ack(jobId, workerId) {
    return this._db._callScalar('SELECT honker_ack(?, ?)', [jobId, workerId]) === 1;
  }

  _retry(jobId, workerId, delayS, error) {
    return (
      this._db._callScalar('SELECT honker_retry(?, ?, ?, ?)', [
        jobId,
        workerId,
        delayS,
        error,
      ]) === 1
    );
  }

  _fail(jobId, workerId, error) {
    return (
      this._db._callScalar('SELECT honker_fail(?, ?, ?)', [jobId, workerId, error]) ===
      1
    );
  }

  _heartbeat(jobId, workerId, extendS) {
    return (
      this._db._callScalar('SELECT honker_heartbeat(?, ?, ?)', [
        jobId,
        workerId,
        extendS,
      ]) === 1
    );
  }

  /** Delete a pending or processing job by id. Returns true iff a row
   *  was removed. Idempotent on missing.
   *
   *  IMPORTANT: cancel does NOT interrupt a worker that's currently
   *  running the handler for this job. The worker keeps executing
   *  until its handler returns (or it dies). What cancel does is
   *  invalidate the worker's claim — its next ack()/heartbeat() call
   *  returns false, same shape as an expired claim. If you need the
   *  handler to actually stop, build that signal in your app (check a
   *  flag periodically, etc.); honker doesn't propagate cancellation
   *  to running handlers. */
  cancel(jobId) {
    return this._db._callScalar('SELECT honker_cancel(?)', [jobId]) > 0;
  }

  /** Read a single job row by id. Returns the row object or null if
   *  the job has been ack'd, dead'd, or never existed. */
  getJob(jobId) {
    const raw = this._db._callScalar('SELECT honker_get_job(?)', [jobId]);
    if (!raw) return null;
    const row = JSON.parse(raw);
    return {
      id: row.id,
      queue: row.queue,
      payload: parseJson(row.payload),
      state: row.state,
      priority: row.priority,
      runAt: row.run_at,
      workerId: row.worker_id ?? null,
      claimExpiresAt: row.claim_expires_at ?? null,
      attempts: row.attempts,
      maxAttempts: row.max_attempts,
      createdAt: row.created_at,
      expiresAt: row.expires_at ?? null,
    };
  }
}

class Outbox {
  constructor(db, name, delivery, opts = {}) {
    if (typeof delivery !== 'function') {
      throw new TypeError('delivery must be a function');
    }
    this._db = db;
    this.name = name;
    this.delivery = delivery;
    this.maxAttempts = opts.maxAttempts ?? 5;
    this.baseBackoffS = opts.baseBackoffS ?? 5;
    this.queue = db.queue(`_outbox:${name}`, {
      visibilityTimeoutS: opts.visibilityTimeoutS ?? 60,
      maxAttempts: this.maxAttempts,
    });
  }

  enqueue(payload, opts = {}) {
    return this.queue.enqueue(payload, opts);
  }

  enqueueTx(tx, payload, opts = {}) {
    return this.queue.enqueueTx(tx, payload, opts);
  }

  async runWorker(workerId, opts = {}) {
    const waker = this.queue.claimWaker(opts);
    try {
      while (!aborted(opts.signal)) {
        const job = await waker.next(workerId, opts);
        if (!job) return;
        try {
          await this.delivery(job.payload, job);
          if (!job.ack()) {
            throw new Error(`outbox ack failed for job ${job.id}`);
          }
        } catch (err) {
          if (aborted(opts.signal)) throw err;
          const delayS = this._retryDelay(job.attempts);
          const message = err && err.stack ? err.stack : String(err);
          if (!job.retry(delayS, message)) {
            throw new Error(`outbox retry failed for job ${job.id}: ${message}`);
          }
        }
      }
    } finally {
      waker.close();
    }
  }

  _retryDelay(attempts) {
    if (this.baseBackoffS <= 0) return 0;
    return Math.ceil(this.baseBackoffS * (2 ** Math.max(0, attempts - 1)));
  }
}

class StreamEvent {
  constructor(row) {
    this.offset = row.offset;
    this.topic = row.topic;
    this.key = row.key ?? null;
    this.payload = parseJson(row.payload);
    this.createdAt = row.created_at ?? null;
  }
}

class StreamSubscription {
  constructor(stream, consumer, { saveEveryN = 1000, saveEveryS = 1.0 } = {}) {
    this._stream = stream;
    this._consumer = consumer;
    this._saveEveryN = Math.max(0, saveEveryN);
    this._saveEveryMs = Math.max(0, saveEveryS * 1000);
    this._updates = stream._db.updateEvents();
    this._closed = false;
    this._pending = [];
    this._deliveredSinceSave = 0;
    this._lastSavedOffset = stream.getOffset(consumer);
    this._lastSeenOffset = this._lastSavedOffset;
    this._lastSaveAt = Date.now();
  }

  [Symbol.asyncIterator]() {
    return this;
  }

  _maybeSaveOffset() {
    if (this._lastSeenOffset <= this._lastSavedOffset) return;
    const hitCount =
      this._saveEveryN > 0 && this._deliveredSinceSave >= this._saveEveryN;
    const hitTime =
      this._saveEveryMs > 0 && Date.now() - this._lastSaveAt >= this._saveEveryMs;
    if (!hitCount && !hitTime) return;
    this._stream.saveOffset(this._consumer, this._lastSeenOffset);
    this._lastSavedOffset = this._lastSeenOffset;
    this._deliveredSinceSave = 0;
    this._lastSaveAt = Date.now();
  }

  _loadPending() {
    const rows = this._stream.readSince(this._lastSeenOffset, 256);
    this._pending.push(...rows);
  }

  async next() {
    while (!this._closed) {
      if (this._pending.length === 0) this._loadPending();
      if (this._pending.length > 0) {
        const event = this._pending.shift();
        this._lastSeenOffset = event.offset;
        this._deliveredSinceSave += 1;
        this._maybeSaveOffset();
        return { done: false, value: event };
      }
      // 1 s fallback poll: while the watcher is live the event wait
      // wins every race, so this costs nothing; when the watcher is
      // dead it bounds the loop to poll cadence instead of spinning
      // on instantly-rejecting waits.
      await waitForUpdateOrTimeout(this._updates, null, 1000);
    }
    return { done: true, value: undefined };
  }

  close() {
    if (this._closed) return;
    this._closed = true;
    if (this._lastSeenOffset > this._lastSavedOffset) {
      this._stream.saveOffset(this._consumer, this._lastSeenOffset);
      this._lastSavedOffset = this._lastSeenOffset;
    }
    this._updates.close();
  }
}

class Stream {
  constructor(db, name) {
    this._db = db;
    this.name = name;
  }

  publish(payload) {
    return this._db._callScalar('SELECT honker_stream_publish(?, NULL, ?)', [
      this.name,
      jsonText(payload),
    ]);
  }

  publishWithKey(key, payload) {
    return this._db._callScalar('SELECT honker_stream_publish(?, ?, ?)', [
      this.name,
      key,
      jsonText(payload),
    ]);
  }

  publishTx(tx, payload) {
    return scalar(
      unwrapTx(tx).query('SELECT honker_stream_publish(?, NULL, ?)', [
        this.name,
        jsonText(payload),
      ]),
    );
  }

  readSince(offset, limit) {
    const rowsJson = this._db._callScalar('SELECT honker_stream_read_since(?, ?, ?)', [
      this.name,
      offset,
      limit,
    ]);
    return JSON.parse(rowsJson).map((row) => new StreamEvent(row));
  }

  readFromConsumer(consumer, limit) {
    return this.readSince(this.getOffset(consumer), limit);
  }

  // Node 0.4.6 persisted (stream, consumer) into the SQL ABI's
  // (consumer, topic) key. Prefer the canonical row. When only the old
  // row exists, its saved offset identifies which stream produced it:
  // a normal checkpoint is the offset of an event in this stream. That
  // lets the common upgrade path migrate automatically without guessing
  // about a legitimate checkpoint for the reversed name pair.
  _resolveCheckpointTx(tx, consumer, { allowUnverifiable = false } = {}) {
    const canonicalOffset = scalar(
      tx.query(
        'SELECT offset FROM _honker_stream_consumers WHERE name = ? AND topic = ?',
        [consumer, this.name],
      ),
    );
    if (canonicalOffset != null) return canonicalOffset;

    const legacyOffset = scalar(
      tx.query(
        'SELECT offset FROM _honker_stream_consumers WHERE name = ? AND topic = ?',
        [this.name, consumer],
      ),
    );
    if (legacyOffset == null) return 0;

    if (legacyOffset !== 0) {
      const eventTopic = scalar(
        tx.query('SELECT topic FROM _honker_stream WHERE offset = ?', [legacyOffset]),
      );
      if (eventTopic !== this.name) {
        // Reads must not guess: adopting this row could skip events for
        // the requested stream. An explicit save is different. It gives
        // us the caller's intended canonical progress, so leave the
        // ambiguous legacy row untouched and let the canonical upsert
        // below establish the new source of truth.
        if (allowUnverifiable) return 0;
        throw new CheckpointMigrationError(this.name, consumer, legacyOffset);
      }
    }

    tx.execute(
      'INSERT INTO _honker_stream_consumers (name, topic, offset) VALUES (?, ?, ?) ' +
        'ON CONFLICT(name, topic) DO NOTHING',
      [consumer, this.name, legacyOffset],
    );
    return legacyOffset;
  }

  saveOffset(consumer, offset) {
    const tx = this._db.transaction();
    try {
      this._resolveCheckpointTx(unwrapTx(tx), consumer, { allowUnverifiable: true });
      const changed =
        scalar(
          unwrapTx(tx).query('SELECT honker_stream_save_offset(?, ?, ?)', [
            consumer,
            this.name,
            offset,
          ]),
        ) === 1;
      tx.commit();
      return changed;
    } catch (err) {
      try {
        tx.rollback();
      } catch {}
      throw err;
    }
  }

  saveOffsetTx(tx, consumer, offset) {
    const rawTx = unwrapTx(tx);
    this._resolveCheckpointTx(rawTx, consumer, { allowUnverifiable: true });
    return (
      scalar(
        rawTx.query('SELECT honker_stream_save_offset(?, ?, ?)', [
          consumer,
          this.name,
          offset,
        ]),
      ) === 1
    );
  }

  getOffset(consumer) {
    const tx = this._db.transaction();
    try {
      const offset = this._resolveCheckpointTx(unwrapTx(tx), consumer);
      tx.commit();
      return offset;
    } catch (err) {
      try {
        tx.rollback();
      } catch {}
      throw err;
    }
  }

  subscribe(consumer, opts = {}) {
    return new StreamSubscription(this, consumer, opts);
  }
}

class QueueEvent {
  constructor(row) {
    this.version = row.version;
    this.offset = row.offset;
    this.type = row.type;
    this.queue = row.queue;
    this.jobId = row.job_id;
    this.occurredAt = row.occurred_at;
    this.attempts = row.attempts;
    this.workerId = row.worker_id ?? null;
    this.runAt = row.run_at ?? null;
    this.reason = row.reason ?? null;
    this.error = row.error ?? null;
    if (Object.prototype.hasOwnProperty.call(row, 'payload')) {
      this.payload = row.payload;
    }
  }
}

class QueueEvents {
  constructor(db, opts = {}) {
    const {
      queue = null,
      fallbackPollS = 1,
      signal = null,
    } = opts;
    this._db = db;
    this.queue = queue;
    this._fallbackPollMs =
      fallbackPollS == null ? null : Math.max(0, fallbackPollS * 1000);
    this._signal = signal;
    // Subscribe before the first read so a commit cannot land in the
    // snapshot/listen gap and leave this iterator parked until fallback.
    this._updates = db.updateEvents();
    let status;
    try {
      status = db._queueEventsStatus();
    } catch (error) {
      this._updates.close();
      throw error;
    }
    this._closed = false;
    this._pending = [];
    this._lastSeen = Object.prototype.hasOwnProperty.call(opts, 'fromOffset')
      ? opts.fromOffset
      : status.trimmedThroughOffset;
  }

  [Symbol.asyncIterator]() {
    return this;
  }

  get lastOffset() {
    return this._lastSeen;
  }

  readSince(offset, limit = 256) {
    let rowsJson;
    try {
      rowsJson = this._db._callScalar(
        'SELECT honker_queue_events_read_since(?, ?, ?)',
        [offset, this.queue, limit],
      );
    } catch (error) {
      if (!String(error?.message).includes('HONKER_QUEUE_EVENT_OFFSET_EXPIRED')) {
        throw error;
      }
      const status = this._db._queueEventsStatus();
      throw new QueueEventOffsetExpiredError(
        offset,
        status.trimmedThroughOffset,
        status.oldestOffset,
      );
    }
    return JSON.parse(rowsJson || '[]').map((row) => new QueueEvent(row));
  }

  _loadPending() {
    this._pending.push(...this.readSince(this._lastSeen));
  }

  async next() {
    try {
      while (!this._closed && !aborted(this._signal)) {
        if (this._pending.length === 0) this._loadPending();
        if (this._pending.length > 0) {
          const event = this._pending.shift();
          this._lastSeen = event.offset;
          return { done: false, value: event };
        }
        await waitForUpdateOrTimeout(
          this._updates,
          this._signal,
          this._fallbackPollMs,
        );
      }
    } catch (error) {
      this.close();
      throw error;
    }
    this.close();
    return { done: true, value: undefined };
  }

  async return() {
    this.close();
    return { done: true, value: undefined };
  }

  close() {
    if (this._closed) return;
    this._closed = true;
    this._updates.close();
  }
}

class QueueEventListener extends EventEmitter {
  constructor(db, opts = {}) {
    super();
    const status = db._queueEventsStatus();
    if (!status.enabled) throw new QueueEventsDisabledError();
    if (
      Object.prototype.hasOwnProperty.call(opts, 'fromOffset') &&
      Object.prototype.hasOwnProperty.call(opts, 'startAt')
    ) {
      throw new TypeError('Specify either fromOffset or startAt, not both');
    }
    const { startAt = 'latest', ...feedOpts } = opts;
    if (!Object.prototype.hasOwnProperty.call(opts, 'fromOffset')) {
      if (startAt === 'latest') {
        feedOpts.fromOffset = status.latestOffset ?? status.trimmedThroughOffset;
      } else if (startAt !== 'oldest') {
        throw new TypeError("startAt must be 'latest' or 'oldest'");
      }
    }
    this._feed = new QueueEvents(db, feedOpts);
    this._closed = false;
    queueMicrotask(() => this._pump());
  }

  get lastOffset() {
    return this._feed.lastOffset;
  }

  async _pump() {
    try {
      for await (const event of this._feed) {
        this.emit(event.type, event);
        this.emit('event', event);
      }
    } catch (error) {
      if (!this._closed) this.emit('error', error);
    } finally {
      this.close();
    }
  }

  close() {
    if (this._closed) return;
    this._closed = true;
    this._feed.close();
    this.emit('close');
  }
}

class Listener {
  constructor(db, channel, { fallbackPollS = 15 } = {}) {
    this._db = db;
    this.channel = channel;
    this._fallbackPollMs =
      fallbackPollS == null ? null : Math.max(0, fallbackPollS * 1000);
    this._updates = db.updateEvents();
    this._closed = false;
    this._pending = [];
    this._lastSeen = scalar(db.query('SELECT COALESCE(MAX(id), 0) FROM _honker_notifications')) ?? 0;
  }

  [Symbol.asyncIterator]() {
    return this;
  }

  _loadPending() {
    const rows = this._db.query(
      'SELECT id, channel, payload, created_at FROM _honker_notifications WHERE id > ? ORDER BY id',
      [this._lastSeen],
    );
    for (const row of rows) {
      this._lastSeen = row.id;
      if (row.channel === this.channel) {
        this._pending.push({
          id: row.id,
          channel: row.channel,
          payload: parseJson(row.payload),
          createdAt: row.created_at ?? null,
        });
      }
    }
  }

  async next() {
    while (!this._closed) {
      if (this._pending.length === 0) this._loadPending();
      if (this._pending.length > 0) {
        return { done: false, value: this._pending.shift() };
      }
      await waitForUpdateOrTimeout(this._updates, null, this._fallbackPollMs);
    }
    return { done: true, value: undefined };
  }

  close() {
    if (this._closed) return;
    this._closed = true;
    this._updates.close();
  }
}

class Scheduler {
  constructor(db) {
    this._db = db;
  }

  add({ name, queue, schedule = null, cron = null, payload, priority = 0, expiresS = null, maxAttempts = 3 }) {
    const expr = schedule ?? cron;
    if (!expr) throw new Error('must provide schedule or cron');
    this._db._callScalar('SELECT honker_scheduler_register(?, ?, ?, ?, ?, ?, ?)', [
      name,
      queue,
      expr,
      jsonText(payload),
      priority,
      expiresS,
      maxAttempts,
    ]);
  }

  remove(name) {
    return this._db._callScalar('SELECT honker_scheduler_unregister(?)', [name]);
  }

  pause(name) {
    return this._db._callScalar('SELECT honker_scheduler_pause(?)', [name]) > 0;
  }

  resume(name) {
    return this._db._callScalar('SELECT honker_scheduler_resume(?)', [name]) > 0;
  }

  list() {
    const raw = this._db._callScalar('SELECT honker_scheduler_list()');
    return JSON.parse(raw || '[]');
  }

  update(name, opts = {}) {
    // Detect "field present" via `in` so null and undefined are
    // distinguishable: { payload: null } writes JSON null, omitting
    // the key leaves payload alone. Same shape as the Python binding's
    // _UNSET sentinel — keeps the two bindings consistent on this
    // semantic edge.
    const has = (k) => Object.prototype.hasOwnProperty.call(opts, k);
    const expr = has('schedule') ? opts.schedule : has('cron') ? opts.cron : null;
    const cronArg = expr === undefined ? null : expr;
    const payloadArg = has('payload') ? jsonText(opts.payload) : null;
    const priorityArg = has('priority') ? opts.priority : null;
    const touchExpires = has('expiresS') ? 1 : 0;
    const expiresArg = has('expiresS') ? opts.expiresS : null;
    const touchMaxAttempts = has('maxAttempts') ? 1 : 0;
    const maxAttemptsArg = has('maxAttempts') ? opts.maxAttempts : null;
    const n = this._db._callScalar(
      'SELECT honker_scheduler_update(?, ?, ?, ?, ?, ?, ?, ?)',
      [name, cronArg, payloadArg, priorityArg, expiresArg, touchExpires, maxAttemptsArg, touchMaxAttempts],
    );
    return n > 0;
  }

  tick(now = nowUnix()) {
    const rowsJson = this._db._callScalar('SELECT honker_scheduler_tick(?)', [now]);
    return JSON.parse(rowsJson);
  }

  soonest() {
    return this._db._callScalar('SELECT honker_scheduler_soonest()');
  }

  async run(owner, signal) {
    const updates = this._db.updateEvents();
    try {
      while (!aborted(signal)) {
        const lock = this._db.tryLock('honker-scheduler', owner, 60);
        if (!lock) {
          await waitForUpdateOrTimeout(updates, signal, 5000);
          continue;
        }
        try {
          await this._leaderLoop(lock, signal, updates);
        } finally {
          try {
            lock.release();
          } catch {}
        }
      }
    } finally {
      updates.close();
    }
  }

  async _leaderLoop(lock, signal, updates) {
    const heartbeatMs = 20_000;
    let lastHeartbeat = monotonicMs();
    while (!aborted(signal)) {
      if (!lock.heartbeat(60)) return;
      lastHeartbeat = monotonicMs();
      this.tick();

      let waitMs = Math.max(0, heartbeatMs - (monotonicMs() - lastHeartbeat));
      const nextFire = this.soonest();
      if (nextFire && nextFire > 0) {
        waitMs = Math.min(waitMs, Math.max(0, nextFire * 1000 - Date.now()));
      }
      await waitForUpdateOrTimeout(updates, signal, waitMs);
    }
  }
}

class Database {
  constructor(db) {
    this._db = db;
  }

  raw() {
    return this._db;
  }

  transaction() {
    return new Transaction(this._db.transaction());
  }

  query(sql, params) {
    return this._db.query(sql, params);
  }

  _callRows(sql, params) {
    const tx = this.transaction();
    try {
      const rows = tx.query(sql, params);
      tx.commit();
      return rows;
    } catch (err) {
      try {
        tx.rollback();
      } catch {}
      throw err;
    }
  }

  _callScalar(sql, params) {
    return scalar(this._callRows(sql, params));
  }

  updateEvents() {
    return new UpdateEvents(this._db.updateEvents());
  }

  close() {
    this._db.close();
  }

  pruneNotifications(olderThanS, maxKeep) {
    return this._db.pruneNotifications(olderThanS, maxKeep);
  }

  notify(channel, payload) {
    const tx = this.transaction();
    try {
      const id = tx.notify(channel, payload);
      tx.commit();
      return id;
    } catch (err) {
      try {
        tx.rollback();
      } catch {}
      throw err;
    }
  }

  notifyTx(tx, channel, payload) {
    return unwrapTx(tx).notify(channel, payload);
  }

  configureQueueEvents({ enabled = true, retentionTarget = 10_000, includePayload = false } = {}) {
    return (
      this._callScalar('SELECT honker_queue_events_configure(?, ?, ?)', [
        enabled ? 1 : 0,
        retentionTarget,
        includePayload ? 1 : 0,
      ]) === 1
    );
  }

  _queueEventsStatus() {
    const raw = this._callScalar('SELECT honker_queue_events_status()');
    const row = JSON.parse(raw || '{}');
    return {
      enabled: Boolean(row.enabled),
      retentionTarget: row.retention_target ?? 10_000,
      includePayload: Boolean(row.include_payload),
      trimmedThroughOffset: row.trimmed_through_offset ?? 0,
      oldestOffset: row.oldest_offset ?? null,
      latestOffset: row.latest_offset ?? null,
    };
  }

  queueEvents(opts = {}) {
    return new QueueEvents(this, opts);
  }

  queueEventListener(opts = {}) {
    return new QueueEventListener(this, opts);
  }

  queue(name, opts = {}) {
    return new Queue(this, name, opts);
  }

  outbox(name, delivery, opts = {}) {
    return new Outbox(this, name, delivery, opts);
  }

  stream(name) {
    return new Stream(this, name);
  }

  listen(channel, opts = {}) {
    return new Listener(this, channel, opts);
  }

  scheduler() {
    return new Scheduler(this);
  }

  tryLock(name, owner, ttlS) {
    const ok = this._callScalar('SELECT honker_lock_acquire(?, ?, ?)', [
      name,
      owner,
      ttlS,
    ]);
    return ok === 1 ? new Lock(this, name, owner) : null;
  }

  tryRateLimit(name, limit, per) {
    return this._callScalar('SELECT honker_rate_limit_try(?, ?, ?)', [
      name,
      limit,
      per,
    ]) === 1;
  }

  sweepRateLimits(olderThanS) {
    return this._callScalar('SELECT honker_rate_limit_sweep(?)', [olderThanS]);
  }

  saveResult(jobId, value, ttlS) {
    this._callScalar('SELECT honker_result_save(?, ?, ?)', [jobId, value, ttlS]);
  }

  getResult(jobId) {
    return this._callScalar('SELECT honker_result_get(?)', [jobId]);
  }

  sweepResults() {
    return this._callScalar('SELECT honker_result_sweep()');
  }
}

module.exports = function buildApi(nativeBinding) {
  function open(path, maxReaders, watcherBackend, watcherPollIntervalMs) {
    if (maxReaders && typeof maxReaders === 'object') {
      const opts = maxReaders;
      return new Database(nativeBinding.open(
        path,
        opts.maxReaders,
        opts.watcherBackend,
        opts.watcherPollIntervalMs,
      ));
    }
    return new Database(nativeBinding.open(
      path,
      maxReaders,
      watcherBackend,
      watcherPollIntervalMs,
    ));
  }

  return {
    open,
    Database,
    Transaction,
    UpdateEvents,
    Queue,
    Outbox,
    Job,
    ClaimWaker,
    Stream,
    StreamEvent,
    StreamSubscription,
    QueueEvent,
    QueueEvents,
    QueueEventListener,
    QueueEventOffsetExpiredError,
    QueueEventsDisabledError,
    CheckpointMigrationError,
    Listener,
    Scheduler,
    Lock,
    native: nativeBinding,
    NativeDatabase: nativeBinding.Database,
    NativeTransaction: nativeBinding.Transaction,
    NativeUpdateEvents: nativeBinding.UpdateEvents,
  };
};
