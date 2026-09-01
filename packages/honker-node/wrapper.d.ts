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

/**
 * Queue lifecycle event names. `error` and `close` are reserved by
 * QueueEventListener's EventEmitter contract and must not become lifecycle
 * event types.
 */
export type QueueEventType =
  | 'enqueued'
  | 'claimed'
  | 'completed'
  | 'retry_scheduled'
  | 'dead_lettered'
  | 'cancelled'

export type QueueEventReason =
  | 'explicit_failure'
  | 'attempts_exhausted'
  | 'job_expired'

export class QueueEvent<TPayload = JsonValue> {
  readonly version: 1
  readonly offset: number
  readonly type: QueueEventType
  readonly queue: string
  readonly jobId: number
  readonly occurredAt: number
  readonly attempts: number
  readonly workerId: string | null
  readonly runAt: number | null
  readonly reason: QueueEventReason | null
  readonly error: string | null
  readonly payload?: TPayload
}

export interface QueueEventsOptions {
  queue?: string | null
  fromOffset?: number
  fallbackPollS?: number | null
  signal?: AbortSignal | null
}

export interface QueueEventsConfig {
  enabled?: boolean
  /**
   * Approximate number of events to retain. Counts events, not bytes, and has
   * no time-based expiry: an event stays until `retentionTarget` newer ones
   * push it out. Budget for it — with `includePayload` on, the feed costs
   * roughly `retentionTarget` x payload size on top of the queue, in the same
   * SQLite file. 1 to 1,000,000; defaults to 10,000.
   */
  retentionTarget?: number
  /**
   * Store a copy of each job's payload on its events. Feed-wide: enabling it
   * to inspect one queue captures payloads for every queue. The copy outlives
   * the job row, which is deleted on completion. Defaults to false.
   */
  includePayload?: boolean
}

export interface QueueEventListenerOptions extends Omit<QueueEventsOptions, 'fromOffset'> {
  fromOffset?: number
  startAt?: 'latest' | 'oldest'
}

export class QueueEventOffsetExpiredError extends Error {
  readonly code: 'HONKER_QUEUE_EVENT_OFFSET_EXPIRED'
  readonly requestedOffset: number
  readonly trimmedThroughOffset: number
  readonly oldestAvailableOffset: number | null
}

export class QueueEventsDisabledError extends Error {
  readonly code: 'HONKER_QUEUE_EVENTS_DISABLED'
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

export type JobState = 'pending' | 'processing'

export interface JobSnapshot<TPayload = JsonValue> {
  readonly id: number
  readonly queue: string
  readonly payload: TPayload
  readonly state: JobState
  readonly priority: number
  readonly runAt: number
  readonly workerId: string | null
  readonly claimExpiresAt: number | null
  readonly attempts: number
  readonly maxAttempts: number
  readonly createdAt: number
  readonly expiresAt: number | null
}

export class Job<TPayload = JsonValue> {
  readonly id: number
  readonly queue: string
  readonly payload: TPayload
  readonly state: 'processing'
  readonly priority: number
  readonly runAt: number
  readonly workerId: string
  readonly attempts: number
  readonly claimExpiresAt: number | null
  readonly maxAttempts: number
  readonly createdAt: number
  readonly expiresAt: number | null
  ack(): boolean
  retry(delayS?: number, error?: string): boolean
  fail(error?: string): boolean
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

export class QueueEvents<TPayload = JsonValue>
  implements AsyncIterableIterator<QueueEvent<TPayload>> {
  readonly queue: string | null
  readonly lastOffset: number
  readSince(offset: number, limit?: number): QueueEvent<TPayload>[]
  next(): Promise<IteratorResult<QueueEvent<TPayload>>>
  return(): Promise<IteratorResult<QueueEvent<TPayload>>>
  [Symbol.asyncIterator](): AsyncIterableIterator<QueueEvent<TPayload>>
  close(): void
}

export class QueueEventListener<TPayload = JsonValue> {
  readonly lastOffset: number
  on(type: QueueEventType, listener: (event: QueueEvent<TPayload>) => void): this
  on(type: 'event', listener: (event: QueueEvent<TPayload>) => void): this
  on(type: 'error', listener: (error: Error) => void): this
  on(type: 'close', listener: () => void): this
  once(type: QueueEventType, listener: (event: QueueEvent<TPayload>) => void): this
  once(type: 'event', listener: (event: QueueEvent<TPayload>) => void): this
  once(type: 'error', listener: (error: Error) => void): this
  once(type: 'close', listener: () => void): this
  off(type: QueueEventType, listener: (event: QueueEvent<TPayload>) => void): this
  off(type: 'event', listener: (event: QueueEvent<TPayload>) => void): this
  off(type: 'error', listener: (error: Error) => void): this
  off(type: 'close', listener: () => void): this
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
  cancel(jobId: number): boolean
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
  configureQueueEvents(opts?: QueueEventsConfig): boolean
  queueEvents<TPayload = JsonValue>(opts?: QueueEventsOptions): QueueEvents<TPayload>
  queueEventListener<TPayload = JsonValue>(opts?: QueueEventListenerOptions): QueueEventListener<TPayload>
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
