import { test, expect, describe } from "bun:test";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { open, type Database } from "../src/index.ts";

const REPO_ROOT = resolve(import.meta.dir, "..", "..", "..");
const EXT_CANDIDATES = [
  "target/debug/libhonker_ext.dylib",
  "target/debug/libhonker_ext.so",
  "target/debug/libhonker_extension.dylib",
  "target/debug/libhonker_extension.so",
  "target/release/libhonker_ext.dylib",
  "target/release/libhonker_ext.so",
  "target/release/libhonker_extension.dylib",
  "target/release/libhonker_extension.so",
];

function findExtension(): string | null {
  const fromEnv = process.env.HONKER_EXT_PATH;
  if (fromEnv && existsSync(fromEnv)) return fromEnv;
  for (const rel of EXT_CANDIDATES) {
    const p = join(REPO_ROOT, rel);
    if (existsSync(p)) return p;
  }
  return null;
}

const extPath = findExtension();
if (!extPath && process.env.CI) {
  throw new Error("HONKER_EXT_PATH not found in CI; Bun tests must run for real");
}
const maybe = extPath ? describe : describe.skip;

interface EmailPayload {
  recipient: string;
  template: "welcome" | "receipt";
}

/** Read the clock the core stamps rows with, not the JS clock. */
function now(db: Database): number {
  return db.raw.query<{ v: number }, []>("SELECT unixepoch() AS v").get()!.v;
}

function withDb(fn: (db: Database) => Promise<void> | void): () => Promise<void> {
  return async () => {
    const dir = mkdtempSync(join(tmpdir(), "honker-bun-typed-jobs-"));
    const dbPath = join(dir, "t.db");
    const db = open(dbPath, extPath!);
    try {
      await fn(db);
    } finally {
      db.close();
      rmSync(dir, { recursive: true, force: true });
    }
  };
}

maybe("honker-bun job details", () => {
  test(
    "a claimed job carries every field the core returns",
    withDb((db) => {
      const queue = db.queue<EmailPayload>("emails", {
        visibilityTimeoutS: 120,
        maxAttempts: 5,
      });
      const runAt = now(db) - 5;

      const enqueuedBefore = now(db);
      const id = queue.enqueue(
        { recipient: "alice@example.com", template: "welcome" },
        { runAt, priority: 7, expires: 600 },
      );
      const enqueuedAfter = now(db);

      const claimedBefore = now(db);
      const job = queue.claimOne("worker-1");
      const claimedAfter = now(db);

      expect(job).not.toBeNull();
      expect(job!.id).toBe(id);
      expect(job!.queue).toBe("emails");
      expect(job!.payload).toEqual({
        recipient: "alice@example.com",
        template: "welcome",
      });
      expect(job!.state).toBe("processing");
      expect(job!.priority).toBe(7);
      expect(job!.runAt).toBe(runAt);
      expect(job!.workerId).toBe("worker-1");
      expect(job!.claimExpiresAt).toBeGreaterThanOrEqual(claimedBefore + 120);
      expect(job!.claimExpiresAt).toBeLessThanOrEqual(claimedAfter + 120);
      expect(job!.attempts).toBe(1);
      expect(job!.maxAttempts).toBe(5);
      expect(job!.createdAt).toBeGreaterThanOrEqual(enqueuedBefore);
      expect(job!.createdAt).toBeLessThanOrEqual(enqueuedAfter);
      expect(job!.expiresAt).toBeGreaterThanOrEqual(enqueuedBefore + 600);
      expect(job!.expiresAt).toBeLessThanOrEqual(enqueuedAfter + 600);
    }),
  );

  test(
    "a pending snapshot carries every field the core returns",
    withDb((db) => {
      const queue = db.queue<EmailPayload>("emails", { maxAttempts: 2 });
      const runAt = now(db) + 3600;

      const enqueuedBefore = now(db);
      const id = queue.enqueue(
        { recipient: "bob@example.com", template: "receipt" },
        { runAt, priority: 4, expires: 900 },
      );
      const enqueuedAfter = now(db);

      const snapshot = queue.getJob(id);
      expect(snapshot).not.toBeNull();
      expect(snapshot!.id).toBe(id);
      expect(snapshot!.queue).toBe("emails");
      expect(snapshot!.payload).toEqual({
        recipient: "bob@example.com",
        template: "receipt",
      });
      expect(snapshot!.state).toBe("pending");
      expect(snapshot!.priority).toBe(4);
      expect(snapshot!.runAt).toBe(runAt);
      expect(snapshot!.workerId).toBeNull();
      expect(snapshot!.claimExpiresAt).toBeNull();
      expect(snapshot!.attempts).toBe(0);
      expect(snapshot!.maxAttempts).toBe(2);
      expect(snapshot!.createdAt).toBeGreaterThanOrEqual(enqueuedBefore);
      expect(snapshot!.createdAt).toBeLessThanOrEqual(enqueuedAfter);
      expect(snapshot!.expiresAt).toBeGreaterThanOrEqual(enqueuedBefore + 900);
      expect(snapshot!.expiresAt).toBeLessThanOrEqual(enqueuedAfter + 900);
    }),
  );

  test("a reader sees the processing snapshot, then nothing after ack", async () => {
    const dir = mkdtempSync(join(tmpdir(), "honker-bun-typed-jobs-"));
    const dbPath = join(dir, "t.db");
    const worker = open(dbPath, extPath!);
    const reader = open(dbPath, extPath!);
    try {
      const workerQueue = worker.queue<EmailPayload>("emails", { visibilityTimeoutS: 45 });
      const readerQueue = reader.queue<EmailPayload>("emails", { visibilityTimeoutS: 45 });

      const id = workerQueue.enqueue({ recipient: "carol@example.com", template: "welcome" });
      const claimedBefore = now(worker);
      const job = workerQueue.claimOne("worker-7");
      const claimedAfter = now(worker);
      expect(job).not.toBeNull();

      const processing = readerQueue.getJob(id);
      expect(processing).not.toBeNull();
      expect(processing!.state).toBe("processing");
      expect(processing!.workerId).toBe("worker-7");
      expect(processing!.attempts).toBe(1);
      expect(processing!.claimExpiresAt).toBeGreaterThanOrEqual(claimedBefore + 45);
      expect(processing!.claimExpiresAt).toBeLessThanOrEqual(claimedAfter + 45);
      expect(processing!.payload).toEqual({
        recipient: "carol@example.com",
        template: "welcome",
      });

      expect(job!.ack()).toBe(true);
      expect(readerQueue.getJob(id)).toBeNull();
    } finally {
      reader.close();
      worker.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test(
    "a delayed job reports its runAt and is not claimable yet",
    withDb((db) => {
      const queue = db.queue<EmailPayload>("emails");

      const before = now(db);
      const id = queue.enqueue(
        { recipient: "dan@example.com", template: "receipt" },
        { delay: 60 },
      );
      const after = now(db);

      const snapshot = queue.getJob(id);
      expect(snapshot).not.toBeNull();
      expect(snapshot!.state).toBe("pending");
      expect(snapshot!.runAt).toBeGreaterThanOrEqual(before + 60);
      expect(snapshot!.runAt).toBeLessThanOrEqual(after + 60);
      expect(queue.claimOne("early-worker")).toBeNull();
    }),
  );

  test(
    "a retried job reports the new attempt count and run_at",
    withDb((db) => {
      const queue = db.queue<EmailPayload>("emails", { maxAttempts: 3 });
      const id = queue.enqueue({ recipient: "erin@example.com", template: "welcome" });

      const first = queue.claimOne("worker-1");
      expect(first!.attempts).toBe(1);
      expect(first!.maxAttempts).toBe(3);

      const retriedAt = now(db);
      expect(first!.retry(30, "boom")).toBe(true);

      const snapshot = queue.getJob(id);
      expect(snapshot!.state).toBe("pending");
      expect(snapshot!.attempts).toBe(1);
      expect(snapshot!.workerId).toBeNull();
      expect(snapshot!.runAt).toBeGreaterThanOrEqual(retriedAt + 30);
      expect(snapshot!.runAt).toBeLessThanOrEqual(now(db) + 30);
    }),
  );

  test(
    "the claim waker returns a job carrying the same detail",
    withDb(async (db) => {
      const queue = db.queue<EmailPayload>("emails", { visibilityTimeoutS: 60 });
      const id = queue.enqueue(
        { recipient: "frank@example.com", template: "receipt" },
        { priority: 3 },
      );

      const waker = queue.claimWaker({ idlePollS: 1 });
      try {
        const job = await waker.next("waker-worker");
        expect(job).not.toBeNull();
        expect(job!.id).toBe(id);
        expect(job!.state).toBe("processing");
        expect(job!.priority).toBe(3);
        expect(job!.workerId).toBe("waker-worker");
        expect(job!.payload.recipient).toBe("frank@example.com");
        expect(job!.ack()).toBe(true);
      } finally {
        waker.close();
      }
    }),
  );
});
