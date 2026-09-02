// Compile-time proof of the typed queue surface. Never executed: tsc
// checks it, `bun test` does not run it. Honker performs no runtime
// payload validation, so these types are a contract between callers.
import { open, type Job, type JobSnapshot } from "../../src/index.ts";

interface EmailPayload {
  recipient: string;
  template: "welcome" | "receipt";
  variables: Record<string, string>;
}

const db = open("typed-jobs.db", "libhonker_ext.so");
const queue = db.queue<EmailPayload>("emails");

queue.enqueue({
  recipient: "alice@example.com",
  template: "welcome",
  variables: { firstName: "Alice" },
});

// A snapshot's payload is raw JSON text in this binding, not a decoded
// TPayload — the encoding is deliberately left as it was.
const pending: JobSnapshot | null = queue.getJob(1);
if (pending) {
  const decoded = JSON.parse(pending.payload) as EmailPayload;
  decoded.recipient.toUpperCase();
  pending.state satisfies "pending" | "processing";
  pending.runAt.toFixed(0);
  pending.maxAttempts.toFixed(0);
  pending.createdAt.toFixed(0);
  pending.workerId satisfies string | null;
  pending.claimExpiresAt satisfies number | null;
  pending.expiresAt satisfies number | null;
}

const claimed: Job<EmailPayload> | null = queue.claimOne("worker-1");
if (claimed) {
  claimed.payload.template satisfies "welcome" | "receipt";
  claimed.state satisfies "processing";
  claimed.priority.toFixed(0);
  claimed.workerId.toUpperCase();
  claimed.claimExpiresAt.toFixed(0);
  claimed.ack();
  // @ts-expect-error the payload contract has no `subject`. Without a
  // negative check like this one, widening Job<TPayload>.payload to `any`
  // erases the whole generic and every assertion above still compiles.
  claimed.payload.subject;
}

const waker = queue.claimWaker();
const nextJob: Promise<Job<EmailPayload> | null> = waker.next("worker-2");
void nextJob;

const outbox = db.outbox<EmailPayload>("email-delivery", async (payload, job) => {
  payload.variables.firstName.toUpperCase();
  job.payload.recipient.toUpperCase();
});
outbox.enqueue({
  recipient: "bob@example.com",
  template: "receipt",
  variables: {},
});

// @ts-expect-error recipient is required by the queue payload contract
queue.enqueue({ template: "welcome", variables: {} });

waker.close();
db.close();
