import { test } from "bun:test";
import { spawnSync, spawn } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import assert from "node:assert/strict";

import { open } from "../src/index.ts";

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
const BACKENDS = [null, "kernel", "shm"] as const;
const MODULE_PATH = resolve(import.meta.dir, "../src/index.ts");

function findExtension(): string | null {
  const fromEnv = process.env.HONKER_EXT_PATH;
  if (fromEnv && existsSync(fromEnv)) return fromEnv;
  for (const rel of EXT_CANDIDATES) {
    const p = join(REPO_ROOT, rel);
    if (existsSync(p)) return p;
  }
  return null;
}

function openOrSkip(dbPath: string, extPath: string, backend: string | null) {
  try {
    return open(dbPath, extPath, { watcherBackend: backend });
  } catch (err) {
    const msg = String(err);
    if (
      (backend === "kernel" || backend === "shm") &&
      (msg.includes("requires the") ||
        msg.includes("-shm unavailable") ||
        msg.includes("unsupported SQLite layout"))
    ) {
      console.log(`skip watcherBackend=${backend}: ${msg}`);
      return null;
    }
    throw err;
  }
}

function waitForLine(proc: ReturnType<typeof spawn>, predicate: (line: string) => boolean, timeoutMs: number) {
  let buf = "";
  const lines: string[] = [];
  return new Promise<string>((resolve, reject) => {
    let settled = false;
    const finish = (fn: () => void) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      fn();
    };
    const timer = setTimeout(() => finish(() => reject(new Error(`timeout waiting for child line`))), timeoutMs);
    proc.stdout.on("data", (chunk) => {
      buf += chunk.toString("utf8");
      let idx;
      while ((idx = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, idx).replace(/\r$/, "");
        buf = buf.slice(idx + 1);
        lines.push(line);
        if (predicate(line)) {
          finish(() => resolve(line));
          return;
        }
      }
    });
    proc.once("exit", (code) => {
      const line = lines.find(predicate);
      if (line) {
        finish(() => resolve(line));
      } else {
        finish(() => reject(new Error(`child exited ${code} before expected line`)));
      }
    });
    proc.once("error", (err) => {
      finish(() => reject(err));
    });
  });
}

function workerScript(dbPath: string, extPath: string, workerId: string, backend: string | null): string {
  return `
    import { open } from ${JSON.stringify(MODULE_PATH)};
    const db = open(${JSON.stringify(dbPath)}, ${JSON.stringify(extPath)}, { watcherBackend: ${JSON.stringify(backend)} });
    const q = db.queue("shared");
    const waker = q.claimWaker({ idlePollS: null });
    console.log("READY");
    const processed = [];
    while (true) {
      const controller = new AbortController();
      const next = waker.next(${JSON.stringify(workerId)}, { signal: controller.signal });
      const result = await Promise.race([
        next,
        new Promise((resolve) => setTimeout(() => resolve({ timeout: true }), 2000)),
      ]);
      if (result?.timeout || !result) {
        controller.abort();
        break;
      }
      processed.push(result.payload.i);
      result.ack();
    }
    waker.close();
    db.close();
    console.log("RESULT " + JSON.stringify(processed));
  `;
}

async function spawnWorker(dbPath: string, extPath: string, workerId: string, backend: string | null) {
  const proc = spawn(process.execPath, ["-e", workerScript(dbPath, extPath, workerId, backend)], {
    stdio: ["ignore", "pipe", "pipe"],
  });
  await waitForLine(proc, (line) => line === "READY", 5000);
  return proc;
}

async function waitWorker(proc: ReturnType<typeof spawn>, workerId: string) {
  const stderr: string[] = [];
  proc.stderr.on("data", (chunk) => stderr.push(chunk.toString("utf8")));
  const line = await waitForLine(proc, (value) => value.startsWith("RESULT "), 30000);
  const result = JSON.parse(line.slice("RESULT ".length));
  const code = proc.exitCode ?? await Promise.race([
    new Promise((resolve) => proc.once("exit", (exitCode) => resolve(exitCode))),
    new Promise((resolve) => setTimeout(() => resolve(undefined), 1000)),
  ]);
  if (code === undefined) proc.kill();
  if (code !== undefined && code !== 0) {
    throw new Error(`worker ${workerId} exited ${code}: ${stderr.join("")}`);
  }
  return result as number[];
}

function writerScript(dbPath: string, extPath: string, n: number, offset = 0, holdMs = 0) {
  return `
    import { open } from ${JSON.stringify(MODULE_PATH)};
    const db = open(${JSON.stringify(dbPath)}, ${JSON.stringify(extPath)});
    const q = db.queue("shared");
    for (let i = ${offset}; i < ${offset} + ${n}; i += 1) q.enqueue({ i });
    if (${holdMs} > 0) await new Promise((resolve) => setTimeout(resolve, ${holdMs}));
    db.close();
  `;
}

function runWriter(dbPath: string, extPath: string, n: number, offset = 0) {
  const script = writerScript(dbPath, extPath, n, offset);
  const res = spawnSync(process.execPath, ["-e", script], {
    encoding: "utf8",
    timeout: 15000,
  });
  assert.equal(res.status, 0, `writer failed: stdout=${res.stdout} stderr=${res.stderr}`);
}

function assertIntSet(values: number[], n: number) {
  assert.deepEqual(
    values.toSorted((a, b) => a - b),
    Array.from({ length: n }, (_, i) => i),
  );
}

const extPath = findExtension();

for (const backend of BACKENDS) {
  const label = backend ?? "polling";

  test(`watcherBackend=${label} queue 1 writer / 1 worker`, async () => {
    if (!extPath) return;
    const dir = mkdtempSync(join(tmpdir(), "honker-bun-queue-watchers-"));
    const dbPath = join(dir, "q.db");
    let worker: ReturnType<typeof spawn> | null = null;
    try {
      const db = openOrSkip(dbPath, extPath!, backend);
      if (!db) return;
      db.queue("shared");
      db.close();

      worker = await spawnWorker(dbPath, extPath!, "w1", backend);
      runWriter(dbPath, extPath!, 25);
      assertIntSet(await waitWorker(worker, "w1"), 25);
    } finally {
      worker?.kill();
      rmSync(dir, { recursive: true, force: true });
    }
  }, 30000);

  test(`watcherBackend=${label} queue 1 writer / many workers`, async () => {
    if (!extPath) return;
    const dir = mkdtempSync(join(tmpdir(), "honker-bun-queue-watchers-"));
    const dbPath = join(dir, "q.db");
    const workers: ReturnType<typeof spawn>[] = [];
    try {
      const db = openOrSkip(dbPath, extPath!, backend);
      if (!db) return;
      db.queue("shared");
      db.close();

      for (let i = 0; i < 3; i += 1) workers.push(await spawnWorker(dbPath, extPath!, `w${i}`, backend));
      runWriter(dbPath, extPath!, 60);
      const results = await Promise.all(workers.map((w, i) => waitWorker(w, `w${i}`)));
      assertIntSet(results.flat(), 60);
      for (let i = 0; i < results.length; i += 1) {
        for (let j = i + 1; j < results.length; j += 1) {
          assert.deepEqual(results[i].filter((value) => results[j].includes(value)), []);
        }
      }
    } finally {
      for (const worker of workers) worker.kill();
      rmSync(dir, { recursive: true, force: true });
    }
  }, 30000);

  test(`watcherBackend=${label} queue many writers / 1 worker`, async () => {
    if (!extPath) return;
    const dir = mkdtempSync(join(tmpdir(), "honker-bun-queue-watchers-"));
    const dbPath = join(dir, "q.db");
    let worker: ReturnType<typeof spawn> | null = null;
    try {
      const db = openOrSkip(dbPath, extPath!, backend);
      if (!db) return;
      db.queue("shared");
      db.close();

      worker = await spawnWorker(dbPath, extPath!, "solo", backend);
      for (let i = 0; i < 3; i += 1) runWriter(dbPath, extPath!, 20, i * 20);
      assertIntSet(await waitWorker(worker, "solo"), 60);
    } finally {
      worker?.kill();
      rmSync(dir, { recursive: true, force: true });
    }
  }, 30000);
}
