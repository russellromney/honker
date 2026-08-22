/**
 * Locate the Honker SQLite loadable extension.
 *
 * For code that wants to load Honker onto a `bun:sqlite` handle it
 * already owns — a Drizzle or Kysely connection, say — instead of
 * going through `open()`. Enqueueing outside your app's transaction
 * loses atomicity, which is the whole reason to do it this way.
 *
 * Resolution order matches every other binding: HONKER_EXTENSION_PATH,
 * then the platform package installed alongside this one, then an
 * in-repo build. A miss throws naming every path searched; there is no
 * silent fallback.
 */

import { existsSync, statSync } from "node:fs";
import { createRequire } from "node:module";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * When no entry point is given, SQLite derives one from the file name.
 * That works for `libhonker_ext.{so,dylib}` and `honker_ext.dll`. Pass
 * this explicitly if you load the library under any other name — the
 * derivation is version-dependent and will not find the symbol.
 */
export const EXTENSION_ENTRYPOINT = "sqlite3_honkerext_init";

function extensionFilename(): string {
  if (process.platform === "win32") return "honker_ext.dll";
  if (process.platform === "darwin") return "libhonker_ext.dylib";
  return "libhonker_ext.so";
}

// The same four targets the extension is published for. Bun has no
// Windows or musl build yet, so those fall through to a clear error
// rather than resolving to something that will not load.
function platformPackage(): string | null {
  const { platform, arch } = process;
  if (platform === "darwin") {
    if (arch === "arm64") return "@russellthehippo/honker-ext-darwin-arm64";
    if (arch === "x64") return "@russellthehippo/honker-ext-darwin-x64";
    return null;
  }
  if (platform === "linux") {
    // Only glibc builds are published. Without this guard, Alpine
    // resolves the -gnu package and extensionPath() hands back a .so
    // that cannot load — bun install does not honour the `libc` field
    // the way npm does, so the package may well be present.
    if (isMusl()) return null;
    if (arch === "x64") return "@russellthehippo/honker-ext-linux-x64-gnu";
    if (arch === "arm64") return "@russellthehippo/honker-ext-linux-arm64-gnu";
    return null;
  }
  return null;
}

function isMusl(): boolean {
  if (process.platform !== "linux") return false;
  const report =
    typeof process.report?.getReport === "function"
      ? (process.report.getReport() as Record<string, unknown>)
      : null;
  const header = report?.header as { glibcVersionRuntime?: string } | undefined;
  if (header?.glibcVersionRuntime) return false;
  const shared = report?.sharedObjects;
  if (Array.isArray(shared)) {
    return shared.some(
      (f) => typeof f === "string" && (f.includes("libc.musl-") || f.includes("ld-musl-")),
    );
  }
  return false;
}

function candidates(): string[] {
  const filename = extensionFilename();
  const found: string[] = [];

  const pkg = platformPackage();
  if (pkg) {
    try {
      found.push(createRequire(import.meta.url).resolve(`${pkg}/${filename}`));
    } catch {
      found.push(join("<not installed>", pkg, filename));
    }
  }

  // Bounded at the first node_modules — see the note in
  // packages/honker-node/extension.js. Walking to the filesystem root
  // can pick up a planted library under a world-writable ancestor.
  let dir = dirname(fileURLToPath(import.meta.url));
  for (;;) {
    found.push(join(dir, "target", "release", filename));
    if (basename(dir) === "node_modules") break;
    const parent = dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }

  return found;
}

function isFile(p: string): boolean {
  try {
    return existsSync(p) && statSync(p).isFile();
  } catch {
    return false;
  }
}

/** Absolute path to the Honker SQLite extension. */
export function extensionPath(): string {
  const override = process.env.HONKER_EXTENSION_PATH;
  if (override) {
    if (isFile(override)) return override;
    throw new Error(`HONKER_EXTENSION_PATH does not exist: ${override}`);
  }

  const searched = candidates();
  for (const candidate of searched) {
    if (isFile(candidate)) return candidate;
  }

  const pkg = platformPackage();
  const hint = pkg
    ? `Expected the optional dependency ${pkg}. It is missing, which usually means the install skipped optional dependencies, the lockfile was built on a different platform, or the registry mirror does not carry it. Reinstall, or set HONKER_EXTENSION_PATH to a libhonker_ext you have.`
    : `No Honker extension is published for ${process.platform}-${process.arch}. Build it with \`cargo build --release -p honker-extension\` and set HONKER_EXTENSION_PATH.`;
  throw new Error(
    `Honker SQLite extension not found. ${hint}\nSearched:\n  ${searched.join("\n  ")}`,
  );
}

/** The extension path and its entry point together. */
export function extensionInfo(): { path: string; entrypoint: string } {
  return { path: extensionPath(), entrypoint: EXTENSION_ENTRYPOINT };
}
