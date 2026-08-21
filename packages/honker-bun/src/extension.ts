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
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * SQLite derives the entry point from the file name: strip a leading
 * `lib`, take characters up to the first `.`, keep the alphabetic ones.
 * `libhonker_ext.so` gives `honkerext`, matching the symbol exported by
 * honker-extension. Only needed if you load the file under another name.
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
    if (arch === "x64") return "@russellthehippo/honker-ext-linux-x64-gnu";
    if (arch === "arm64") return "@russellthehippo/honker-ext-linux-arm64-gnu";
    return null;
  }
  return null;
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

  let dir = dirname(fileURLToPath(import.meta.url));
  for (;;) {
    found.push(join(dir, "target", "release", filename));
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
    ? `Install @russellthehippo/honker-bun so the optional dependency ${pkg} comes with it, or set HONKER_EXTENSION_PATH.`
    : `No Honker extension is published for ${process.platform}-${process.arch}. Build it with \`cargo build --release -p honker-extension\` and set HONKER_EXTENSION_PATH.`;
  throw new Error(
    `Honker SQLite extension not found. ${hint}\nSearched:\n  ${searched.join("\n  ")}`,
  );
}

/** The extension path and its entry point together. */
export function extensionInfo(): { path: string; entrypoint: string } {
  return { path: extensionPath(), entrypoint: EXTENSION_ENTRYPOINT };
}
