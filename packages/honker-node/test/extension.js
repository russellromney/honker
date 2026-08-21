'use strict';

// Resolver unit tests. The acceptance proof — loading the *packaged*
// extension into better-sqlite3 — lives in the clean-consumer job in
// .github/workflows/release-node.yml, because it needs published
// tarballs to be honest.

const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { test } = require('node:test');

const ext = require('../extension.js');

test('entrypoint matches the symbol honker-extension exports', () => {
  // honker-extension/src/lib.rs declares sqlite3_honkerext_init. If that
  // symbol is ever renamed, this constant has to move with it.
  assert.equal(ext.EXTENSION_ENTRYPOINT, 'sqlite3_honkerext_init');
});

test('HONKER_EXTENSION_PATH wins when the file exists', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'honker-ext-'));
  const fake = path.join(dir, 'libhonker_ext.so');
  fs.writeFileSync(fake, '');
  const prev = process.env.HONKER_EXTENSION_PATH;
  process.env.HONKER_EXTENSION_PATH = fake;
  try {
    assert.equal(ext.extensionPath(), fake);
    assert.deepEqual(ext.extensionInfo(), {
      path: fake,
      entrypoint: 'sqlite3_honkerext_init',
    });
  } finally {
    if (prev === undefined) delete process.env.HONKER_EXTENSION_PATH;
    else process.env.HONKER_EXTENSION_PATH = prev;
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('a bad HONKER_EXTENSION_PATH fails loudly instead of falling back', () => {
  const prev = process.env.HONKER_EXTENSION_PATH;
  process.env.HONKER_EXTENSION_PATH = '/nonexistent/libhonker_ext.so';
  try {
    assert.throws(() => ext.extensionPath(), /HONKER_EXTENSION_PATH does not exist/);
  } finally {
    if (prev === undefined) delete process.env.HONKER_EXTENSION_PATH;
    else process.env.HONKER_EXTENSION_PATH = prev;
  }
});

test('resolving the extension does not load the native addon', () => {
  // The whole point of a separate entry point: an ORM user already has
  // better-sqlite3's SQLite in the process and must not get a second
  // one just to ask for a path string.
  const script = `
    require(${JSON.stringify(path.resolve(__dirname, '..', 'extension.js'))});
    const loaded = Object.keys(require.cache).filter((k) => k.endsWith('.node'));
    if (loaded.length) {
      console.error('native addon loaded: ' + loaded.join(', '));
      process.exit(1);
    }
    console.log('clean');
  `;
  const out = execFileSync(process.execPath, ['-e', script], { encoding: 'utf8' });
  assert.equal(out.trim(), 'clean');
});
