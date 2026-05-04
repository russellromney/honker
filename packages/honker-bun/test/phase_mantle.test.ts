// Phase Mantle: Scheduler lifecycle + Queue cancel/getJob.

import { test, expect, describe } from "bun:test";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { open, Scheduler } from "../src/index.ts";

const REPO_ROOT = resolve(import.meta.dir, "..", "..", "..");
const EXT_CANDIDATES = [
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

maybe("phase mantle", () => {
  function withDb(fn: (db: ReturnType<typeof open>) => void) {
    const dir = mkdtempSync(join(tmpdir(), "honker-bun-mantle-"));
    const dbPath = join(dir, "t.db");
    const db = open(dbPath, extPath!);
    try {
      fn(db);
    } finally {
      db.close();
      rmSync(dir, { recursive: true, force: true });
    }
  }

  test("schedule list round-trips fields", () => {
    withDb((db) => {
      const sched = new Scheduler(db);
      sched.add({ name: "recap", queue: "emails", cron: "0 9 * * 1", payload: { team: "premier-league" }, priority: 3 });
      sched.add({ name: "sync", queue: "syncs", cron: "@every 1h", payload: null });

      const rows = sched.list();
      expect(rows.length).toBe(2);
      const recap = rows.find((r) => r.name === "recap")!;
      expect(recap.queue).toBe("emails");
      expect(recap.priority).toBe(3);
      expect(recap.enabled).toBe(true);
      expect(JSON.parse(recap.payload).team).toBe("premier-league");
    });
  });

  test("pause / resume idempotent", () => {
    withDb((db) => {
      const sched = new Scheduler(db);
      sched.add({ name: "a", queue: "q", cron: "0 9 * * *", payload: null });

      expect(sched.pause("a")).toBe(true);
      expect(sched.pause("a")).toBe(false);
      expect(sched.pause("missing")).toBe(false);
      expect(sched.list().find((r) => r.name === "a")!.enabled).toBe(false);

      expect(sched.resume("a")).toBe(true);
      expect(sched.resume("a")).toBe(false);
      expect(sched.list().find((r) => r.name === "a")!.enabled).toBe(true);
    });
  });

  test("update mutates fields, no-op on empty, recomputes next_fire_at on cron", () => {
    withDb((db) => {
      const sched = new Scheduler(db);
      sched.add({ name: "t", queue: "q", cron: "0 9 * * *", payload: { v: 1 } });

      expect(sched.update("t", { payload: { v: 99 }, priority: 5 })).toBe(true);
      const row = sched.list().find((r) => r.name === "t")!;
      expect(JSON.parse(row.payload).v).toBe(99);
      expect(row.priority).toBe(5);

      const before = row.next_fire_at;
      expect(sched.update("t", { cron: "*/5 * * * *" })).toBe(true);
      const after = sched.list().find((r) => r.name === "t")!;
      expect(after.cron_expr).toBe("*/5 * * * *");
      expect(after.next_fire_at).not.toBe(before);

      expect(sched.update("t")).toBe(false); // no-op
      expect(sched.update("missing", { payload: {} })).toBe(false);
    });
  });

  test("queue cancel + getJob", () => {
    withDb((db) => {
      const q = db.queue("emails");
      const id = q.enqueue({ to: "alice" });
      const row = q.getJob(id);
      expect(row).not.toBeNull();
      expect(row!.id).toBe(id);
      expect(row!.state).toBe("pending");

      expect(q.cancel(id)).toBe(true);
      expect(q.cancel(id)).toBe(false);
      expect(q.getJob(id)).toBeNull();
      expect(q.claimOne("worker-1")).toBeNull();
    });
  });

  test("cancel of processing invalidates ack", () => {
    withDb((db) => {
      const q = db.queue("emails");
      const id = q.enqueue({ to: "x" });
      const job = q.claimOne("worker-1")!;
      expect(job.id).toBe(id);
      expect(q.cancel(id)).toBe(true);
      expect(job.ack()).toBe(false);
    });
  });

  test("paused schedule does not emit on tick", async () => {
    let _db: any;
    const dir = mkdtempSync(join(tmpdir(), "honker-bun-mantle-"));
    const dbPath = join(dir, "t.db");
    const db = open(dbPath, extPath!);
    try {
      const sched = new Scheduler(db);
      sched.add({ name: "due", queue: "emails", cron: "@every 1s", payload: { x: 1 } });
      await new Promise((r) => setTimeout(r, 1100));
      sched.pause("due");
      const future = Math.floor(Date.now() / 1000) + 5;
      const fires = sched.tick(future);
      expect(fires.length).toBe(0);
      sched.resume("due");
      const fires2 = sched.tick(future);
      expect(fires2.length).toBeGreaterThanOrEqual(1);
    } finally {
      db.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("get_job misses after ack (separate from cancel)", () => {
    withDb((db) => {
      const q = db.queue("emails");
      const id = q.enqueue({ to: "x" });
      const job = q.claimOne("worker-1")!;
      expect(job.ack()).toBe(true);
      expect(q.getJob(id)).toBeNull();
    });
  });

  test("update payload null vs omitted distinction", () => {
    withDb((db) => {
      const sched = new Scheduler(db);
      sched.add({ name: "t", queue: "q", cron: "0 9 * * *", payload: { v: 1 } });
      // Omitted payload — leaves alone.
      sched.update("t", { priority: 7 });
      let row = sched.list().find((r) => r.name === "t")!;
      expect(JSON.parse(row.payload).v).toBe(1);
      expect(row.priority).toBe(7);
      // payload: null — write JSON null.
      sched.update("t", { payload: null });
      row = sched.list().find((r) => r.name === "t")!;
      expect(JSON.parse(row.payload)).toBeNull();
    });
  });
});
