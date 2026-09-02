import { open, type Job, type JobSnapshot } from '../..'

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

// A pending snapshot has no claim, so the compiler must force a null check on
// the two claim-only fields. Without this the `Job` narrowing below would be
// vacuous — both types would just be nullable everywhere.
if (pending) {
  // @ts-expect-error workerId is null while a job is pending
  pending.workerId.toUpperCase()
  // @ts-expect-error claimExpiresAt is null while a job is pending
  pending.claimExpiresAt.toFixed(0)
}

const claimed: Job<EmailPayload> | null = queue.claimOne('worker-1')
if (claimed) {
  claimed.payload.template satisfies 'welcome' | 'receipt'
  claimed.state satisfies 'processing'
  claimed.priority.toFixed(0)
  // Holding a claim is what makes these two non-null. No check needed.
  claimed.workerId.toUpperCase()
  claimed.claimExpiresAt.toFixed(0)
  claimed.expiresAt satisfies number | null
}

// A claimed job is a snapshot you hold a claim on: every reader that takes a
// JobSnapshot must accept a Job. `implements JobSnapshot<TPayload>` on the
// class declaration enforces the other direction, so a field added to one and
// not the other fails this compile.
function describe<T>(job: JobSnapshot<T>): number {
  return job.createdAt
}
if (claimed) {
  describe<EmailPayload>(claimed)
}

// The payload generic must survive the async iterator too, which is the shape
// a real worker loop uses.
async function work(signal: AbortSignal): Promise<void> {
  for await (const job of queue.claim('worker-3', { signal })) {
    job.payload.variables.firstName?.toUpperCase()
    job.ack()
  }
}
void work

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

// @ts-expect-error recipient is required by the queue payload contract
queue.enqueue({ template: 'welcome', variables: {} })

waker.close()
db.close()
