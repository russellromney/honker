'use strict';

// Locate the Honker SQLite loadable extension.
//
// This is for users who want to load Honker onto their own
// better-sqlite3 / Drizzle / Kysely connection instead of using
// honker.open(). Honker does not wrap those clients; it just tells you
// where the extension is.
//
// Deliberately pure JavaScript: it must not require('./index.js').
// The napi addon carries its own statically linked SQLite, and an ORM
// user already has better-sqlite3's loaded. Asking for a path string
// should not drag a second SQLite into the process.
//
// Mirrors the resolution order in
// packages/honker/python/honker/__init__.py: HONKER_EXTENSION_PATH,
// then the bundled platform package, then the in-repo build. No silent
// fallback — a miss raises with every path searched.

const fs = require('node:fs');
const path = require('node:path');

// SQLite derives the entry point from the file name: strip a leading
// `lib`, take characters up to the first `.`, keep only the alphabetic
// ones. `libhonker_ext.so` gives `honkerext`, which is why the exported
// symbol in honker-extension/src/lib.rs is `sqlite3_honkerext_init`.
// Renaming the file breaks that derivation, so pass the entry point
// explicitly if you ever load it under another name.
const EXTENSION_ENTRYPOINT = 'sqlite3_honkerext_init';

function extensionFilename() {
  if (process.platform === 'win32') return 'honker_ext.dll';
  if (process.platform === 'darwin') return 'libhonker_ext.dylib';
  return 'libhonker_ext.so';
}

// Same triples the napi build publishes. Node has no musl or Windows
// target yet, so those platforms fall through to a clear error rather
// than resolving to something that will not load.
function platformPackage() {
  const { platform, arch } = process;
  if (platform === 'darwin') {
    if (arch === 'arm64') return '@russellthehippo/honker-ext-darwin-arm64';
    if (arch === 'x64') return '@russellthehippo/honker-ext-darwin-x64';
    return null;
  }
  if (platform === 'linux') {
    if (isMusl()) return null;
    if (arch === 'x64') return '@russellthehippo/honker-ext-linux-x64-gnu';
    if (arch === 'arm64') return '@russellthehippo/honker-ext-linux-arm64-gnu';
    return null;
  }
  return null;
}

function isMusl() {
  if (process.platform !== 'linux') return false;
  const report = typeof process.report?.getReport === 'function' ? process.report.getReport() : null;
  if (report?.header?.glibcVersionRuntime) return false;
  if (Array.isArray(report?.sharedObjects)) {
    return report.sharedObjects.some((f) => f.includes('libc.musl-') || f.includes('ld-musl-'));
  }
  return false;
}

function candidates() {
  const filename = extensionFilename();
  const found = [];

  const pkg = platformPackage();
  if (pkg) {
    try {
      found.push(require.resolve(`${pkg}/${filename}`));
    } catch {
      // Optional dependency not installed for this platform. Fall
      // through to the in-repo build; the error below names both.
      found.push(path.join('<not installed>', pkg, filename));
    }
  }

  // In-repo build, so the bindings' own tests and anyone hacking on the
  // repo resolve without publishing anything.
  for (let dir = __dirname; ; ) {
    found.push(path.join(dir, 'target', 'release', filename));
    const parent = path.dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }

  return found;
}

/**
 * Absolute path to the Honker SQLite extension.
 *
 * @returns {string}
 * @throws {Error} when no extension is found, naming every path tried.
 */
function extensionPath() {
  const override = process.env.HONKER_EXTENSION_PATH;
  if (override) {
    if (fs.existsSync(override) && fs.statSync(override).isFile()) return override;
    throw new Error(`HONKER_EXTENSION_PATH does not exist: ${override}`);
  }

  const searched = candidates();
  for (const candidate of searched) {
    if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) return candidate;
  }

  const pkg = platformPackage();
  const hint = pkg
    ? `Install @russellthehippo/honker-node so the optional dependency ${pkg} comes with it, or set HONKER_EXTENSION_PATH.`
    : `No Honker extension is published for ${process.platform}-${process.arch}. Build it with \`cargo build --release -p honker-extension\` and set HONKER_EXTENSION_PATH.`;
  throw new Error(`Honker SQLite extension not found. ${hint}\nSearched:\n  ${searched.join('\n  ')}`);
}

/**
 * Path and entry point together, for clients that want to name the
 * entry point explicitly instead of relying on SQLite's filename
 * derivation.
 *
 * @returns {{ path: string, entrypoint: string }}
 */
function extensionInfo() {
  return { path: extensionPath(), entrypoint: EXTENSION_ENTRYPOINT };
}

module.exports = { EXTENSION_ENTRYPOINT, extensionPath, extensionInfo };
