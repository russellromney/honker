#!/usr/bin/env python3
import sys
import zipfile
from pathlib import Path


def check_wheel(path: Path) -> None:
    if "-abi3-" not in path.name:
        raise SystemExit(f"python wheel is not abi3: {path}")

    with zipfile.ZipFile(path) as zf:
        names = zf.namelist()

    has_native = any(
        name.startswith("honker/") and "_honker_native" in name for name in names
    )
    has_extension = any(
        name.startswith("honker/_lib/") and "honker_ext" in name for name in names
    )
    if not has_native:
        raise SystemExit(f"python wheel is missing _honker_native: {path}")
    if not has_extension:
        raise SystemExit(f"python wheel is missing bundled SQLite extension: {path}")


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("usage: check-python-wheel.py WHEEL [WHEEL ...]")
    for arg in sys.argv[1:]:
        check_wheel(Path(arg))


if __name__ == "__main__":
    main()
