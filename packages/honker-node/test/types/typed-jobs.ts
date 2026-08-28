import { open, type Job, type JobSnapshot, type QueueEventReason } from '../..'

interface EmailPayload {
  recipient: string
  template: 'welcome' | 'receipt'
  variables: Record<string, string>
}

const db = open('typed-jobs.db')
const queue = db.queue<EmailPayload>('emails')

queue.enqueue({
  recipient: 'alice@example.com',
  template: 'welcome',
  variables: { firstName: 'Alice' },
})

const pending: JobSnapshot<EmailPayload> | null = queue.getJob(1)
if (pending) {
  pending.payload.recipient.toUpperCase()
  pending.state satisfies 'pending' | 'processing'
  pending.runAt.toFixed(0)
}

const claimed: Job<EmailPayload> | null = queue.claimOne('worker-1')
if (claimed) {
  claimed.payload.template satisfies 'welcome' | 'receipt'
  claimed.state satisfies 'processing'
  claimed.priority.toFixed(0)
}

const waker = queue.claimWaker()
const nextJob: Promise<Job<EmailPayload> | null> = waker.next('worker-2')
void nextJob

const outbox = db.outbox<EmailPayload>('email-delivery', async (payload, job) => {
  payload.variables.firstName.toUpperCase()
  job.payload.recipient.toUpperCase()
})
outbox.enqueue({
  recipient: 'bob@example.com',
  template: 'receipt',
  variables: {},
})

db.configureQueueEvents({ retentionTarget: 10_000, includePayload: true })
const queueEvents = db.queueEvents<EmailPayload>({ queue: 'emails', fromOffset: 0 })
const queueEventListener = db.queueEventListener<EmailPayload>({
  queue: 'emails',
  startAt: 'latest',
})
queueEventListener.on('enqueued', (event) => {
  event.payload?.recipient.toUpperCase()
  event.reason satisfies QueueEventReason | null
})
const retained = queueEvents.readSince(0)
for (const event of retained) {
  event.type satisfies
    | 'enqueued'
    | 'claimed'
    | 'completed'
    | 'retry_scheduled'
    | 'dead_lettered'
    | 'cancelled'
  event.payload?.recipient.toUpperCase()
  event.offset.toFixed(0)
}

// @ts-expect-error recipient is required by the queue payload contract
queue.enqueue({ template: 'welcome', variables: {} })

waker.close()
void queueEvents.return()
queueEvents.close()
queueEventListener.close()
db.close()
