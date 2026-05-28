#!/usr/bin/env python3
import sys
import zipfile
from pathlib import Path


EXPECTED_NATIVE_ASSETS = {
    "runtimes/linux-x64/native/libhonker_ext.so",
    "runtimes/linux-arm64/native/libhonker_ext.so",
    "runtimes/osx-x64/native/libhonker_ext.dylib",
    "runtimes/osx-arm64/native/libhonker_ext.dylib",
    "runtimes/win-x64/native/honker_ext.dll",
}


def check_package(path: Path) -> None:
    with zipfile.ZipFile(path) as zf:
        names = set(zf.namelist())

    missing = sorted(EXPECTED_NATIVE_ASSETS - names)
    if missing:
        raise SystemExit(
            f"NuGet package is missing native assets in {path}:\n"
            + "\n".join(f"  - {name}" for name in missing)
        )


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("usage: check-dotnet-nuget.py NUPKG [NUPKG ...]")
    for arg in sys.argv[1:]:
        check_package(Path(arg))


if __name__ == "__main__":
    main()
