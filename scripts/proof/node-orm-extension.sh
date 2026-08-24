#!/usr/bin/env bash
# Acceptance proof for issue #99: a foreign SQLite client loading the
# *packaged* Honker extension.
#
# Packs honker-node and its honker-ext-<platform> package, installs both
# into a clean project alongside better-sqlite3/Drizzle/Kysely, and runs
# the shared ORM SQL surface plus an ORM-owned commit/rollback. Nothing
# here reads target/release, and there is no skip path: a missing
# extension fails.
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
# Pinned ORM deps come from scripts/proof/orm/js, so a CI run is not at
# the mercy of whatever those packages published this morning. The
# honker tarballs are the two things that must be fresh.
cp "$ROOT/scripts/proof/orm/js/package.json" "$ROOT/scripts/proof/orm/js/package-lock.json" "$app/"
npm ci --no-audit --no-fund >/dev/null
npm install --no-audit --no-fund --no-save "$root_tgz" "$ext_tgz" >/dev/null

# One scenario per integration the docs actually recommend. Prisma is
# absent on purpose: guides/orm/javascript.mdx documents that Prisma
# cannot load SQLite extensions at all, and its fallback is a second
# better-sqlite3 connection, which the first scenario already covers.
# Copy the scenarios into the consumer project. Node resolves bare
# imports from the importing file's own directory, so running them from
# the repo would not see the packages installed here — and a user's
# scenario file lives in their project anyway.
cp "$ROOT"/scripts/proof/orm/js/*.mjs "$ROOT"/scripts/proof/orm/surface.json "$app/"

failed=0
for scenario in better-sqlite3 drizzle kysely; do
  rm -f "$app/$scenario.db"
  if HONKER_PLATFORM="$PLATFORM" HONKER_TEST_DB="$app/$scenario.db" \
    HONKER_ORM_SURFACE="$app/surface.json" node "$app/$scenario.mjs"; then
    :
  else
    echo "FAIL $scenario" >&2
    failed=1
  fi
done
exit "$failed"
