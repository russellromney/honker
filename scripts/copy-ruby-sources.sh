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
printf '\n[workspace]\n' >> "$DEST/honker-extension/Cargo.toml"

echo "vendored honker-extension and honker-core into $DEST"
