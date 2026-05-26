#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC_DIR="$ROOT/target/release"
DEST_DIR="$ROOT/packages/honker-ruby/lib/honker"

case "$(uname -s)" in
  Darwin)
    EXT_NAME="libhonker_ext.dylib"
    ;;
  MINGW*|MSYS*|CYGWIN*|Windows_NT)
    EXT_NAME="honker_ext.dll"
    ;;
  *)
    EXT_NAME="libhonker_ext.so"
    ;;
esac

SRC="$SRC_DIR/$EXT_NAME"
if [[ ! -f "$SRC" ]]; then
  echo "honker extension not found at $SRC" >&2
  echo "run: cargo build --release -p honker-extension" >&2
  exit 1
fi

# Drop any stale extension before copying the current one. DEST_DIR is
# the gem's source directory, so remove only the artifact names, never
# the directory itself.
rm -f "$DEST_DIR/libhonker_ext.so" "$DEST_DIR/libhonker_ext.dylib" "$DEST_DIR/honker_ext.dll"
cp "$SRC" "$DEST_DIR/$EXT_NAME"
echo "copied $SRC -> $DEST_DIR/$EXT_NAME"
