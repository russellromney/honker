#!/usr/bin/env bash
# Move built honker-extension binaries into the honker-ext-<platform>
# npm package directories.
#
# The release workflow builds one extension per napi target and ships
# each in its own npm package, which honker-node and honker-bun pull
# through optionalDependencies.
#
# Usage:
#   scripts/copy-node-extension.sh                  # all four targets
#   scripts/copy-node-extension.sh darwin-arm64     # just one, for local work
#
# Reads staged files named libhonker_ext.<platform>.<so|dylib> from
# packages/honker-node (where the release workflow's artifacts land),
# overridable with HONKER_NODE_EXT_STAGE.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAGE="${HONKER_NODE_EXT_STAGE:-$ROOT/packages/honker-node}"
DEST_ROOT="$ROOT/packages/honker-ext-npm"

if [[ $# -gt 0 ]]; then
  platforms=("$@")
else
  platforms=(darwin-arm64 darwin-x64 linux-x64-gnu linux-arm64-gnu)
fi

for platform in "${platforms[@]}"; do
  case "$platform" in
    darwin-*) libname="libhonker_ext.dylib" ;;
    linux-*) libname="libhonker_ext.so" ;;
    *)
      echo "unknown platform: $platform" >&2
      exit 1
      ;;
  esac

  dest_dir="$DEST_ROOT/$platform"
  if [[ ! -d "$dest_dir" ]]; then
    echo "no npm package directory for $platform at $dest_dir" >&2
    exit 1
  fi

  src="$STAGE/libhonker_ext.$platform.${libname##*.}"
  if [[ ! -f "$src" ]]; then
    echo "extension not found at $src" >&2
    echo "run: cargo build --release -p honker-extension --target <target>" >&2
    exit 1
  fi

  # The file name matters. SQLite derives the entry point from it:
  # strip a leading `lib`, take characters up to the first `.`, keep the
  # alphabetic ones. libhonker_ext -> honkerext -> sqlite3_honkerext_init.
  # Landing it under the staged name would derive the wrong symbol.
  cp "$src" "$dest_dir/$libname"
  echo "copied $src -> $dest_dir/$libname"
done
