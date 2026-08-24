"""Run the shared ORM SQL surface through a bound-parameter scalar()."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable


def qmark_to_named(sql: str) -> tuple[str, list[str]]:
    """Turn `?` placeholders into `:p0`, `:p1`, ... for SQLAlchemy."""
    names: list[str] = []
    out: list[str] = []
    n = 0
    for ch in sql:
        if ch == "?":
            name = f"p{n}"
            names.append(name)
            out.append(f":{name}")
            n += 1
        else:
            out.append(ch)
    return "".join(out), names


def catalog_path() -> Path:
    env = os.environ.get("HONKER_ORM_SURFACE")
    if env:
        p = Path(env)
        if not p.is_file():
            raise FileNotFoundError(f"HONKER_ORM_SURFACE={env} is not a file")
        return p
    here = Path(__file__).resolve().parent
    for candidate in (here / "surface.json", here.parent / "surface.json"):
        if candidate.is_file():
            return candidate
    raise FileNotFoundError("surface.json not found; set HONKER_ORM_SURFACE")


def load_catalog() -> dict:
    return json.loads(catalog_path().read_text())


def as_int(value: Any) -> int:
    if isinstance(value, bool) or value is None:
        raise AssertionError(f"expected int, got {value!r}")
    return int(value)


def as_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def resolve(token: Any, prefix: str, variables: dict[str, Any]) -> Any:
    if not isinstance(token, str):
        return token
    if token.startswith("$ns:"):
        return f"{prefix}_{token[4:]}"
    if token.startswith("$json:"):
        keys = token[6:].split(",")
        return json.dumps([as_int(variables[k]) for k in keys])
    if token.startswith("$"):
        return variables[token[1:]]
    return token


def resolve_text(text: str, prefix: str, variables: dict[str, Any]) -> str:
    out = text
    for key, value in variables.items():
        out = out.replace(f"${key}", as_text(value))
    return out.replace("$ns:", f"{prefix}_")


def check(expect: dict, result: Any, prefix: str, variables: dict[str, Any]) -> None:
    kind = expect["kind"]
    if kind == "int_gt":
        assert as_int(result) > expect["n"], f"got {result!r}"
    elif kind == "int_eq":
        assert as_int(result) == expect["n"], f"got {result!r}"
    elif kind == "int_ge":
        assert as_int(result) >= expect["n"], f"got {result!r}"
    elif kind == "int_gt_ref":
        assert as_int(result) > as_int(variables[expect["ref"]]), f"got {result!r}"
    elif kind == "eq_ref":
        assert as_int(result) == as_int(variables[expect["ref"]]), f"got {result!r}"
    elif kind == "json_len":
        parsed = json.loads(as_text(result))
        assert len(parsed) == expect["n"], f"got {result!r}"
    elif kind == "json_id_eq_ref":
        parsed = json.loads(as_text(result))
        assert len(parsed) == 1, f"got {result!r}"
        assert as_int(parsed[0]["id"]) == as_int(variables[expect["ref"]]), f"got {result!r}"
    elif kind == "contains":
        needle = resolve_text(expect["s"], prefix, variables)
        assert needle in as_text(result), f"{needle!r} not in {result!r}"
    elif kind == "empty_string":
        text = "" if result is None else as_text(result)
        assert text == "", f"expected empty string, got {result!r}"
    elif kind == "is_null":
        assert result is None, f"expected NULL, got {result!r}"
    else:
        raise AssertionError(f"unknown expect kind {kind}")


def run(scalar: Callable[[str, list[Any]], Any], prefix: str) -> None:
    catalog = load_catalog()
    variables: dict[str, Any] = {}
    for step in catalog["steps"]:
        args = [resolve(arg, prefix, variables) for arg in step["args"]]
        try:
            result = scalar(step["sql"], args)
        except Exception as exc:
            raise AssertionError(f"{step['id']} failed: {exc}") from exc
        if "store" in step:
            variables[step["store"]] = result
        if "expect" in step:
            try:
                check(step["expect"], result, prefix, variables)
            except AssertionError as exc:
                raise AssertionError(f"{step['id']}: {exc}") from exc
