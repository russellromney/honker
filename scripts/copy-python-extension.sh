#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC_DIR="$ROOT/target/release"
DEST_DIR="$ROOT/packages/honker/python/honker/_lib"

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

rm -rf "$DEST_DIR"
mkdir -p "$DEST_DIR"
cp "$SRC" "$DEST_DIR/$EXT_NAME"
echo "copied $SRC -> $DEST_DIR/$EXT_NAME"
