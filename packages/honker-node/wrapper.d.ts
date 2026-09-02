export type JsonPrimitive = string | number | boolean | null
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue }

export interface Notification {
  id: number
  channel: string
  payload: JsonValue
  createdAt?: number | null
}

export interface ScheduledFire {
  name: string
  queue: string
  fire_at: number
  job_id: number
}

export interface ScheduleRow {
  name: string
  queue: string
  cron_expr: string
  payload: string
  priority: number
  expires_s: number | null
  next_fire_at: number
  enabled: boolean
  max_attempts: number
}

export interface StreamEvent {
  offset: number
  topic: string
  key: string | null
  payload: JsonValue
  createdAt: number | null
}

export class CheckpointMigrationError extends Error {
  readonly code: 'HONKER_CHECKPOINT_MIGRATION_UNVERIFIABLE'
  readonly stream: string
  readonly consumer: string
  readonly offset: number
}

export interface QueueOptions {
  visibilityTimeoutS?: number
  maxAttempts?: number
}

export interface EnqueueOptions {
  tx?: Transaction | any
  runAt?: number | null
  delay?: number | null
  priority?: number
  expires?: number | null
}

export interface SchedulerAddOptions {
  name: string
  queue: string
  schedule?: string | null
  cron?: string | null
  payload: JsonValue
  priority?: number
  expiresS?: number | null
  maxAttempts?: number
}

export interface SchedulerUpdateOptions {
  schedule?: string | null
  cron?: string | null
  payload?: JsonValue
  priority?: number | null
  expiresS?: number | null
  maxAttempts?: number | null
}

export class Transaction {
  raw(): any
  execute(sql: string, params?: JsonValue[] | null): number
  query(sql: string, params?: JsonValue[] | null): Array<Record<string, any>>
  notify(channel: string, payload: JsonValue): number
  commit(): void
  rollback(): void
}

export class UpdateEvents {
  raw(): any
  /** Wait for the next database update. Resolves on the next commit;
   *  rejects when the watcher dies or close() cuts the subscription.
   *  Concurrent calls share one native wait and settle together, so
   *  racing next() against your own timeout never starts extra native
   *  waits. */
  next(): Promise<void>
  close(): void
}

export class Lock {
  readonly name: string
  readonly owner: string
  release(): boolean
  heartbeat(ttlS: number): boolean
}

/**
 * The states a live job can be in. Acked and dead-lettered jobs leave the live
 * table, so they are never observed here.
 */
export type JobState = 'pending' | 'processing'

/**
 * A read-only view of one live job, as returned by {@link Queue.getJob}.
 *
 * Every timestamp is **Unix epoch seconds**, not milliseconds. Use
 * `new Date(job.createdAt * 1000)` to get a JS `Date`.
 *
 * A claimed {@link Job} carries the same twelve fields plus its completion
 * methods, so a `Job<T>` can be passed anywhere a `JobSnapshot<T>` is expected.
 */
export interface JobSnapshot<TPayload = JsonValue> {
  /** Job id. Globally unique across every queue in the database. */
  readonly id: number
  /** Name of the queue that owns this job. */
  readonly queue: string
  /**
   * The stored payload, JSON-decoded.
   *
   * `TPayload` is an **unchecked assertion**, not a validated schema. Honker
   * stores the payload as text and never inspects its shape, so whatever can
   * write to this queue — another process, another language binding, or a raw
   * `honker_enqueue` through your own SQLite connection — decides what comes
   * back. A producer may write JSON `null`, which arrives here as `null`
   * despite the declared type. Validate at the boundary when the producer is
   * not entirely under your control.
   */
  readonly payload: TPayload
  /** `'pending'` (waiting to run) or `'processing'` (claimed by a worker). */
  readonly state: JobState
  /** Higher runs first. Defaults to 0. */
  readonly priority: number
  /** Earliest time the job may be claimed, in Unix epoch seconds. */
  readonly runAt: number
  /** Worker holding the current claim, or null while the job is pending. */
  readonly workerId: string | null
  /**
   * When the current claim lapses and the job becomes reclaimable, in Unix
   * epoch seconds. Null while the job is pending.
   */
  readonly claimExpiresAt: number | null
  /** Claims made so far. The job is dead-lettered once this reaches `maxAttempts`. */
  readonly attempts: number
  /** Claim budget, fixed at enqueue time from the queue's `maxAttempts`. */
  readonly maxAttempts: number
  /** When the job was enqueued, in Unix epoch seconds. */
  readonly createdAt: number
  /**
   * When the job stops being claimable regardless of state, in Unix epoch
   * seconds, or null if it never expires. Set from `EnqueueOptions.expires`.
   */
  readonly expiresAt: number | null
}

/**
 * A job this worker currently holds a claim on, returned by
 * {@link Queue.claimOne}, {@link Queue.claimBatch}, {@link Queue.claim}, and
 * {@link ClaimWaker.next}.
 *
 * Structurally a {@link JobSnapshot} — the same twelve fields, the same
 * Unix-epoch-second timestamps — narrowed by what holding a claim guarantees:
 * `state` is always `'processing'`, and `workerId` and `claimExpiresAt` are
 * never null.
 *
 * Call exactly one of `ack`, `retry`, or `fail` when you are done. Each returns
 * false when the claim is no longer yours, which happens if it lapsed or the
 * job was cancelled.
 */
export class Job<TPayload = JsonValue> implements JobSnapshot<TPayload> {
  /** Job id. Globally unique across every queue in the database. */
  readonly id: number
  /** Name of the queue that owns this job. */
  readonly queue: string
  /**
   * The stored payload, JSON-decoded. `TPayload` is an unchecked assertion —
   * see {@link JobSnapshot.payload}.
   */
  readonly payload: TPayload
  /** Always `'processing'`: holding a claim is what makes this a `Job`. */
  readonly state: 'processing'
  /** Higher runs first. Defaults to 0. */
  readonly priority: number
  /** Earliest time the job could be claimed, in Unix epoch seconds. */
  readonly runAt: number
  /** The worker id this job was claimed with. Never null on a claimed job. */
  readonly workerId: string
  /**
   * When this claim lapses, in Unix epoch seconds. Never null on a claimed
   * job. Past it another worker may reclaim the job and `ack()` returns false.
   * Push it out with `heartbeat()`.
   */
  readonly claimExpiresAt: number
  /** Claims made so far, including this one, so always at least 1. */
  readonly attempts: number
  /** Claim budget, fixed at enqueue time from the queue's `maxAttempts`. */
  readonly maxAttempts: number
  /** When the job was enqueued, in Unix epoch seconds. */
  readonly createdAt: number
  /**
   * When the job stops being claimable regardless of state, in Unix epoch
   * seconds, or null if it never expires.
   */
  readonly expiresAt: number | null
  /** Mark the job done and remove it. False if the claim is no longer ours. */
  ack(): boolean
  /** Return the job to the queue, optionally after `delayS` seconds. */
  retry(delayS?: number, error?: string): boolean
  /** Dead-letter the job now, without spending its remaining attempts. */
  fail(error?: string): boolean
  /** Push `claimExpiresAt` out by `extendS` seconds. False if the claim lapsed. */
  heartbeat(extendS: number): boolean
}

export class ClaimWaker<TPayload = JsonValue> {
  next(workerId: string, opts?: { signal?: AbortSignal }): Promise<Job<TPayload> | null>
  close(): void
}

export class StreamSubscription implements AsyncIterableIterator<StreamEvent> {
  next(): Promise<IteratorResult<StreamEvent>>
  [Symbol.asyncIterator](): AsyncIterableIterator<StreamEvent>
  close(): void
}

export class Stream {
  publish(payload: JsonValue): number
  publishWithKey(key: string, payload: JsonValue): number
  publishTx(tx: Transaction | any, payload: JsonValue, key?: string | null): number
  readSince(offset: number, limit: number): StreamEvent[]
  readFromConsumer(consumer: string, limit: number): StreamEvent[]
  saveOffset(consumer: string, offset: number): boolean
  saveOffsetTx(tx: Transaction | any, consumer: string, offset: number): boolean
  getOffset(consumer: string): number
  subscribe(
    consumer: string,
    opts?: { saveEveryN?: number; saveEveryS?: number; signal?: AbortSignal }
  ): StreamSubscription
}

export class Listener implements AsyncIterableIterator<Notification> {
  next(): Promise<IteratorResult<Notification>>
  [Symbol.asyncIterator](): AsyncIterableIterator<Notification>
  close(): void
}

export class Scheduler {
  add(opts: SchedulerAddOptions): number | null
  remove(name: string): number
  pause(name: string): boolean
  resume(name: string): boolean
  list(): ScheduleRow[]
  update(name: string, opts?: SchedulerUpdateOptions): boolean
  tick(now?: number): ScheduledFire[]
  soonest(): number | null
  run(owner: string, signal?: AbortSignal): Promise<void>
}

export class Queue<TPayload = JsonValue> {
  readonly name: string
  readonly visibilityTimeoutS: number
  readonly maxAttempts: number
  enqueue(payload: TPayload, opts?: EnqueueOptions): number
  enqueueTx(tx: Transaction | any, payload: TPayload, opts?: EnqueueOptions): number
  claimBatch(workerId: string, n: number): Job<TPayload>[]
  claimOne(workerId: string): Job<TPayload> | null
  claim(workerId: string, opts?: { idlePollS?: number | null, signal?: AbortSignal }): AsyncIterableIterator<Job<TPayload>>
  ackBatch(ids: number[], workerId: string): number
  sweepExpired(): number
  claimWaker(opts?: { idlePollS?: number | null }): ClaimWaker<TPayload>
  /**
   * Delete a pending or processing job by id. Returns true iff a row was
   * removed. Idempotent on a missing job.
   *
   * NOT queue-scoped: job ids are globally unique, and `cancel` will remove a
   * job belonging to any queue, not just this one. This is deliberately
   * asymmetric with {@link Queue.getJob}, which does scope to its own queue.
   * A read-then-check in JavaScript would not be atomic — a worker can claim
   * or complete the job between the check and the delete — so correct scoping
   * needs the filter pushed into the core. Pass only ids you know this queue
   * owns.
   *
   * Cancel does NOT interrupt a worker that is currently running the handler
   * for this job. The worker keeps executing until its handler returns. What
   * cancel does is invalidate the worker's claim: its next `ack()` or
   * `heartbeat()` returns false, the same shape as an expired claim. If you
   * need the handler to actually stop, build that signal into your app;
   * honker does not propagate cancellation to running handlers.
   */
  cancel(jobId: number): boolean
  /**
   * Read a single job by id, as a {@link JobSnapshot}.
   *
   * Returns null if the job has been ack'd, dead'd, never existed, or belongs
   * to a different queue. The lookup is scoped to this queue: because job ids
   * are globally unique, an unscoped lookup could return a row whose payload
   * does not match `TPayload`.
   *
   * Two breaking changes landed here together, both since 0.5.1:
   *
   * 1. The return value is now the decoded camelCase `JobSnapshot` instead of
   *    the SQL ABI's raw row. `row.run_at` is now `snapshot.runAt`, and
   *    `payload` is already JSON-decoded — do not `JSON.parse` it again.
   * 2. The lookup is queue-scoped. Code that used any queue handle as a global
   *    by-id lookup now gets null for jobs owned by other queues. There is no
   *    unscoped replacement yet; until one lands (see honker issue #134), reach
   *    for the SQL function directly on your own connection:
   *    `SELECT honker_get_job(?)`, which still returns the raw snake_case row.
   */
  getJob(jobId: number): JobSnapshot<TPayload> | null
}

export interface OutboxOptions {
  visibilityTimeoutS?: number
  maxAttempts?: number
  baseBackoffS?: number
}

export class Outbox<TPayload = JsonValue> {
  readonly name: string
  readonly queue: Queue<TPayload>
  readonly maxAttempts: number
  readonly baseBackoffS: number
  enqueue(payload: TPayload, opts?: EnqueueOptions): number
  enqueueTx(tx: Transaction | any, payload: TPayload, opts?: EnqueueOptions): number
  runWorker(workerId: string, opts?: { idlePollS?: number | null, signal?: AbortSignal }): Promise<void>
}

export class Database {
  raw(): any
  transaction(): Transaction
  query(sql: string, params?: JsonValue[] | null): Array<Record<string, any>>
  updateEvents(): UpdateEvents
  close(): void
  pruneNotifications(olderThanS?: number | null, maxKeep?: number | null): number
  notify(channel: string, payload: JsonValue): number
  notifyTx(tx: Transaction | any, channel: string, payload: JsonValue): number
  queue<TPayload = JsonValue>(name: string, opts?: QueueOptions): Queue<TPayload>
  outbox<TPayload = JsonValue>(name: string, delivery: (payload: TPayload, job: Job<TPayload>) => any | Promise<any>, opts?: OutboxOptions): Outbox<TPayload>
  stream(name: string): Stream
  listen(channel: string, opts?: { fallbackPollS?: number | null }): Listener
  scheduler(): Scheduler
  tryLock(name: string, owner: string, ttlS: number): Lock | null
  tryRateLimit(name: string, limit: number, per: number): boolean
  sweepRateLimits(olderThanS: number): number
  saveResult(jobId: number, value: string, ttlS: number): void
  getResult(jobId: number): string | null
  sweepResults(): number
}

export interface OpenOptions {
  maxReaders?: number | null
  watcherBackend?: string | null
  watcherPollIntervalMs?: number | null
}

export function open(path: string, options?: OpenOptions): Database
export function open(path: string, maxReaders?: number | null, watcherBackend?: string | null, watcherPollIntervalMs?: number | null): Database

export const native: any
export const NativeDatabase: any
export const NativeTransaction: any
export const NativeUpdateEvents: any
