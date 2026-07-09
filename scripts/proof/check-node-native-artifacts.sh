#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -eq 0 ]]; then
  echo "usage: $0 path/to/*.node ..." >&2
  exit 2
fi

for artifact in "$@"; do
  if [[ ! -s "$artifact" ]]; then
    echo "missing or empty native artifact: $artifact" >&2
    exit 1
  fi

  size="$(wc -c <"$artifact" | tr -d '[:space:]')"
  if [[ "$size" -lt 500000 ]]; then
    echo "native artifact is unexpectedly small ($size bytes): $artifact" >&2
    exit 1
  fi

  case "$artifact" in
    *.linux-*.node)
      file "$artifact" | grep -q 'ELF 64-bit'
      readelf_output="$(readelf -h -d "$artifact" 2>&1)"
      if grep -q '^readelf: Error:' <<<"$readelf_output"; then
        printf '%s\n' "$readelf_output" >&2
        exit 1
      fi
      grep -q 'Dynamic section at offset' <<<"$readelf_output"
      ;;
    *.darwin-*.node)
      file "$artifact" | grep -q 'Mach-O 64-bit'
      otool -hv "$artifact" >/dev/null
      ;;
    *)
      file "$artifact" >/dev/null
      ;;
  esac
done
