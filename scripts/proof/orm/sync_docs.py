#!/usr/bin/env python3
"""Keep ORM guide code blocks byte-for-byte equal to executable proofs.

Proof sources delimit snippets with ``docs:start NAME`` and ``docs:end NAME``
comments. Guide blocks name that source and region in an ``orm-proof`` HTML
comment. This script replaces the fenced body from the source, or fails in
``--check`` mode when a guide has drifted.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
DOCS_ROOT = ROOT / "site/src/content/docs/guides/orm"

BLOCK = re.compile(
    r'(?P<header>\{/\* orm-proof source="(?P<source>[^"]+)" '
    r'region="(?P<region>[^"]+)" \*/\}\n)'
    r"```(?P<lang>[^\n]*)\n(?P<body>.*?)\n```\n"
    r"(?P<footer>\{/\* /orm-proof \*/\})",
    re.DOTALL,
)


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def snippet(source: Path, region: str) -> str:
    lines = source.read_text().splitlines()
    start = [i for i, line in enumerate(lines) if f"docs:start {region}" in line]
    end = [i for i, line in enumerate(lines) if f"docs:end {region}" in line]
    if len(start) != 1 or len(end) != 1 or start[0] >= end[0]:
        fail(
            f"{source}: expected exactly one ordered docs:start/docs:end "
            f"pair for {region!r}"
        )
    selected = lines[start[0] + 1 : end[0]]
    while selected and not selected[0].strip():
        selected.pop(0)
    while selected and not selected[-1].strip():
        selected.pop()
    return "\n".join(selected)


def render(path: Path) -> tuple[str, int]:
    original = path.read_text()
    count = 0

    def replace(match: re.Match[str]) -> str:
        nonlocal count
        source = (ROOT / match.group("source")).resolve()
        if not source.is_relative_to(ROOT) or not source.is_file():
            fail(f"{path}: invalid proof source {match.group('source')!r}")
        body = snippet(source, match.group("region"))
        count += 1
        return (
            f"{match.group('header')}```{match.group('lang')}\n{body}\n```\n"
            f"{match.group('footer')}"
        )

    return BLOCK.sub(replace, original), count


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    if not DOCS_ROOT.is_dir():
        fail(
            f"{DOCS_ROOT} is missing; initialize the site submodule before "
            "checking executable documentation"
        )

    changed: list[Path] = []
    blocks = 0
    for path in sorted(DOCS_ROOT.glob("*.mdx")):
        rendered, count = render(path)
        blocks += count
        if rendered != path.read_text():
            changed.append(path)
            if not args.check:
                path.write_text(rendered)

    if blocks == 0:
        fail("no orm-proof blocks found")
    if args.check and changed:
        rel = ", ".join(str(path.relative_to(ROOT)) for path in changed)
        fail(f"ORM guide snippets are stale: {rel}; run {Path(__file__).relative_to(ROOT)}")
    verb = "checked" if args.check else "synced"
    print(f"PASS orm-docs ({verb} {blocks} executable snippets)")


if __name__ == "__main__":
    main()
