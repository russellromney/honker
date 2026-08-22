#!/usr/bin/env bash
# Acceptance proof for issue #99: a foreign SQLite client loading the
# *packaged* Honker extension.
#
# Packs honker-node and its honker-ext-<platform> package, installs both
# into a clean project alongside better-sqlite3, and round-trips
# enqueue -> claim -> ack through raw SQL. Nothing here reads
# target/release, and there is no skip path: a missing extension fails.
#
# Shared by ci.yml (every PR) and release-node.yml (tags) so the proof
# that gates a release is the same one PRs run.
#
# Usage: scripts/proof/node-orm-extension.sh <platform>
#   e.g. scripts/proof/node-orm-extension.sh linux-x64-gnu
set -euo pipefail

PLATFORM="${1:?usage: node-orm-extension.sh <platform>}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

pkgs="$WORK/pkgs"
app="$WORK/app"
mkdir -p "$pkgs" "$app"

echo "packing @russellthehippo/honker-node"
(cd "$ROOT/packages/honker-node" && npm pack --pack-destination "$pkgs" >/dev/null)

ext_dir="$ROOT/packages/honker-ext-npm/$PLATFORM"
test -d "$ext_dir" || { echo "no extension package for $PLATFORM" >&2; exit 1; }
# Fail loudly if the binary was never staged, rather than packing an
# empty package and failing later with a confusing "not found".
compgen -G "$ext_dir"/libhonker_ext.* >/dev/null \
  || compgen -G "$ext_dir"/honker_ext.dll >/dev/null \
  || { echo "no extension binary staged in $ext_dir — run scripts/copy-node-extension.sh $PLATFORM" >&2; exit 1; }
echo "packing @russellthehippo/honker-ext-$PLATFORM"
(cd "$ext_dir" && npm pack --pack-destination "$pkgs" >/dev/null)

root_tgz="$(find "$pkgs" -maxdepth 1 -name 'russellthehippo-honker-node-[0-9]*.tgz' -print -quit)"
ext_tgz="$(find "$pkgs" -maxdepth 1 -name "russellthehippo-honker-ext-$PLATFORM-*.tgz" -print -quit)"
test -n "$root_tgz"
test -n "$ext_tgz"

cd "$app"
npm init -y >/dev/null
npm install --no-audit --no-fund "$root_tgz" "$ext_tgz" better-sqlite3 >/dev/null

PLATFORM="$PLATFORM" node - <<'JS'
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const Database = require('better-sqlite3');
const { extensionPath, extensionInfo } = require('@russellthehippo/honker-node/extension');

const info = extensionInfo();
assert.equal(info.entrypoint, 'sqlite3_honkerext_init');
// Prove it resolved out of the installed package and not some stray
// build lying around the machine.
assert.match(
  info.path,
  new RegExp(`honker-ext-${process.env.PLATFORM}`),
  `resolved from ${info.path}`,
);

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'honker-orm-'));
try {
  const db = new Database(path.join(dir, 'app.db'));
  // No entrypoint argument: filename derivation is part of what is
  // under test.
  db.loadExtension(extensionPath());
  db.prepare('SELECT honker_bootstrap()').run();

  // Every numeric argument is BOUND, never a SQL literal. better-sqlite3
  // binds JS numbers as REAL, so literals would quietly sidestep the
  // coercion path this is here to cover. Swapping these back to literals
  // makes the test pass against a broken build.
  const payload = JSON.stringify({ to: 'alice@example.com' });
  const { id } = db
    .prepare('SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL) AS id')
    .get('emails', payload, 0, 3);
  assert.ok(id > 0, `expected a job id, got ${id}`);

  const claimed = JSON.parse(
    db.prepare('SELECT honker_claim_batch(?, ?, ?, ?) AS jobs').get('emails', 'worker-1', 8, 300).jobs,
  );
  assert.equal(claimed.length, 1);
  assert.equal(claimed[0].id, id);
  assert.equal(JSON.parse(claimed[0].payload).to, 'alice@example.com');

  const { ok } = db.prepare('SELECT honker_ack(?, ?) AS ok').get(id, 'worker-1');
  assert.equal(ok, 1, 'ack must match the claim');

  db.close();
  console.log(`PASS: better-sqlite3 round trip through ${info.path}`);
} finally {
  fs.rmSync(dir, { recursive: true, force: true });
}
JS
