#!/usr/bin/env python3
"""Fail if an ORM proof drops a documented SQL function or atomicity."""

from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
CATALOG = HERE / "surface.json"

JSON_RUNNERS = [
    ROOT / "scripts/proof/orm/python/surface.py",
    ROOT / "scripts/proof/orm/js/surface.mjs",
    ROOT / "scripts/proof/orm/go/main.go",
    ROOT / "scripts/proof/orm/rust/catalog.rs",
    ROOT / "scripts/proof/orm/rust/src/main.rs",
    ROOT / "scripts/proof/orm/rust/diesel/src/main.rs",
    ROOT / "scripts/proof/orm/rust/seaorm/src/main.rs",
    ROOT / "scripts/proof/orm/dotnet/Program.cs",
    ROOT / "scripts/proof/orm/elixir/ecto_proof.exs",
    ROOT / "scripts/proof/orm/ruby/active_record_proof.rb",
    ROOT / "scripts/proof/orm/jvm/src/main/java/dev/honker/ormproof/Surface.java",
    ROOT / "scripts/proof/orm/cpp/orm_proof.cpp",
]

ATOMICITY_FILES = [
    ROOT / "scripts/proof/orm/python/sqlalchemy_orm.py",
    ROOT / "scripts/proof/orm/python/sqlmodel_orm.py",
    ROOT / "scripts/proof/orm/python/django_orm.py",
    ROOT / "scripts/proof/orm/js/better-sqlite3.mjs",
    ROOT / "scripts/proof/orm/js/drizzle.mjs",
    ROOT / "scripts/proof/orm/js/kysely.mjs",
    ROOT / "scripts/proof/orm/go/main.go",
    ROOT / "scripts/proof/orm/rust/src/main.rs",
    ROOT / "scripts/proof/orm/rust/diesel/src/main.rs",
    ROOT / "scripts/proof/orm/rust/seaorm/src/main.rs",
    ROOT / "scripts/proof/orm/dotnet/Program.cs",
    ROOT / "scripts/proof/orm/elixir/ecto_proof.exs",
    ROOT / "scripts/proof/orm/ruby/active_record_proof.rb",
    ROOT / "scripts/proof/orm/cpp/orm_proof.cpp",
    ROOT / "scripts/proof/orm/jvm/src/main/java/dev/honker/ormproof/OrmProof.java",
    ROOT / "scripts/proof/orm/jvm/src/main/kotlin/dev/honker/ormproof/ExposedProof.kt",
]


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def main() -> None:
    catalog = json.loads(CATALOG.read_text())
    required = catalog["functions"]
    joined = "\n".join(step["sql"] for step in catalog["steps"])
    missing_from_catalog = [name for name in required if name not in joined]
    if missing_from_catalog:
        fail(f"surface.json does not call {missing_from_catalog}")

    extra = []
    for step in catalog["steps"]:
        for token in step["sql"].replace("(", " ").replace(")", " ").split():
            if token.startswith("honker_") or token == "notify":
                if token not in required and token != "honker_bootstrap":
                    extra.append(token)
    if extra:
        fail(f"surface.json calls functions not listed: {sorted(set(extra))}")

    for path in JSON_RUNNERS:
        text = path.read_text()
        if (
            "surface.json" not in text
            and "HONKER_ORM_SURFACE" not in text
            and "catalog.rs" not in text
        ):
            fail(f"{path} does not load surface.json")

    for path in ATOMICITY_FILES:
        text = path.read_text()
        if "orders" not in text:
            fail(f"{path} does not create an orders table for atomicity")
        lowered = text.lower()
        if "rollback" not in lowered:
            fail(f"{path} does not prove rollback")

    print(f"PASS orm-surface ({len(required)} functions, {len(ATOMICITY_FILES)} atomicity proofs)")


if __name__ == "__main__":
    main()
