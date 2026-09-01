#!/usr/bin/env python3
"""Keep the Node addon, npm packages, lockfile, and generated loader in sync."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
NODE = ROOT / "packages" / "honker-node"


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def main() -> int:
    errors: list[str] = []
    package = load_json(NODE / "package.json")
    version = package["version"]

    cargo_match = re.search(
        r'^version = "([^"]+)"$', (NODE / "Cargo.toml").read_text(), re.MULTILINE
    )
    cargo_version = cargo_match.group(1) if cargo_match else None
    if cargo_version != version:
        errors.append(
            f"Cargo.toml is {cargo_version!r}, package.json is {version}"
        )

    native_dependencies = package["optionalDependencies"]
    platform_manifests = sorted((NODE / "npm").glob("*/package.json"))
    if not platform_manifests:
        errors.append("no Node platform package manifests found")
    for path in platform_manifests:
        platform = load_json(path)
        if platform["version"] != version:
            errors.append(f"{path.relative_to(ROOT)} is {platform['version']}, expected {version}")
        pinned = native_dependencies.get(platform["name"])
        if pinned != version:
            errors.append(f"package.json pins {platform['name']} at {pinned!r}, expected {version}")

    lock = load_json(NODE / "package-lock.json")
    lock_root = lock["packages"][""]
    if lock["version"] != version or lock_root["version"] != version:
        errors.append("package-lock.json root version does not match package.json")
    for path in platform_manifests:
        name = load_json(path)["name"]
        pinned = lock_root["optionalDependencies"].get(name)
        if pinned != version:
            errors.append(f"package-lock.json pins {name} at {pinned!r}, expected {version}")

    loader = (NODE / "index.js").read_text()
    loader_versions = re.findall(r"bindingPackageVersion !== '([^']+)'", loader)
    if not loader_versions:
        errors.append("index.js has no native package version checks")
    elif unexpected := sorted(set(loader_versions) - {version}):
        errors.append(f"index.js checks unexpected native versions: {unexpected}")

    # The version the guard compares and the version its error message names
    # are generated separately. A stale index.js can compare correctly while
    # telling the user to install the wrong version, so check both.
    message_versions = re.findall(
        r"Native binding package version mismatch, expected ([^ ]+) but got", loader
    )
    if not message_versions:
        errors.append("index.js has no native package version mismatch messages")
    elif unexpected := sorted(set(message_versions) - {version}):
        errors.append(
            f"index.js mismatch messages name unexpected versions: {unexpected} "
            f"(regenerate with `napi build`)"
        )
    elif len(message_versions) != len(loader_versions):
        errors.append(
            f"index.js has {len(loader_versions)} version checks but "
            f"{len(message_versions)} mismatch messages"
        )

    if errors:
        print("Node package version alignment failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    print(
        f"Node package version alignment ok: {version} "
        f"({len(platform_manifests)} platform packages, {len(loader_versions)} loader checks)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
