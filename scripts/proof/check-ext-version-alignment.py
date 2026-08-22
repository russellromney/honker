#!/usr/bin/env python3
"""Every honker-ext-* pin must match the package it points at.

The extension packages have their own version line, shared by
honker-node and honker-bun, which version independently of each other
and of it. A pin that drifts resolves to a tarball that does not exist:
npm treats the missing optional dependency as non-fatal, so the install
succeeds and extensionPath() fails later with "not found" instead.

Exits non-zero and names every mismatch.
"""

import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
EXT_PREFIX = "@russellthehippo/honker-ext-"
CONSUMERS = (
    "packages/honker-node/package.json",
    "packages/honker-bun/package.json",
)


def main() -> int:
    ext_dir = ROOT / "packages" / "honker-ext-npm"
    published = {}
    for d in sorted(p for p in ext_dir.iterdir() if p.is_dir()):
        pkg = json.loads((d / "package.json").read_text())
        published[pkg["name"]] = pkg["version"]

    if not published:
        print(f"no extension packages found under {ext_dir}", file=sys.stderr)
        return 1

    versions = set(published.values())
    if len(versions) != 1:
        print(f"extension packages disagree on version: {published}", file=sys.stderr)
        return 1

    problems = []
    checked = 0
    for rel in CONSUMERS:
        pkg = json.loads((ROOT / rel).read_text())
        pins = {
            name: pin
            for name, pin in pkg.get("optionalDependencies", {}).items()
            if name.startswith(EXT_PREFIX)
        }
        if not pins:
            problems.append(f"{pkg['name']} declares no {EXT_PREFIX}* optional dependencies")
            continue
        missing = set(published) - set(pins)
        if missing:
            problems.append(f"{pkg['name']} is missing pins for {sorted(missing)}")
        for name, pin in sorted(pins.items()):
            checked += 1
            actual = published.get(name)
            if actual is None:
                problems.append(f"{pkg['name']} pins {name}, which has no package directory")
            elif actual != pin:
                problems.append(f"{pkg['name']} pins {name}@{pin}, but that package is {actual}")

    if problems:
        print("extension package version alignment failed:", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        return 1

    print(f"extension version alignment ok: {checked} pins at {versions.pop()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
