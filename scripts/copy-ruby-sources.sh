#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST="$ROOT/packages/honker-ruby/ext/honker"

# The generic (source) gem compiles the extension on install, so it
# ships the honker-extension and honker-core crate source. This vendors
# them into ext/honker/; the copies are gitignored and refreshed at gem
# build time, mirroring copy-ruby-extension.sh for the prebuilt binary.
for crate in honker-extension honker-core; do
  rm -rf "${DEST:?}/$crate"
  mkdir -p "$DEST/$crate"
  cp -R "$ROOT/$crate/." "$DEST/$crate/"
  rm -rf "$DEST/$crate/target"
done

# Make the vendored extension crate its own workspace root so cargo does
# not attach it to an enclosing Cargo.toml when the gem is built from
# inside this repo.
# Both crates inherit `[lints] workspace = true` from the repo root.
# Inside the gem there is no such root: the line below makes the
# vendored honker-extension its own workspace, and an empty
# `[workspace]` defines no `workspace.lints` to inherit. Cargo then
# refuses to parse the manifest and the gem fails to build on install.
# Drop the inheritance instead of duplicating the root's clippy config
# here, which would silently drift from it. Lints are a dev-time
# concern; someone compiling the gem does not need ours.
for crate in honker-extension honker-core; do
  manifest="$DEST/$crate/Cargo.toml"
  grep -q '^\[lints\]$' "$manifest" || { echo "no [lints] in $crate; the vendoring fix is stale" >&2; exit 1; }
  sed -i.bak '/^\[lints\]$/,/^workspace = true$/d' "$manifest"
  rm -f "$manifest.bak"
done

printf '\n[workspace]\n' >> "$DEST/honker-extension/Cargo.toml"

echo "vendored honker-extension and honker-core into $DEST"
