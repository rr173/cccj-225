"""Shared input-layer primitives: canonical hashing, JSON IO, SQL tokeniser.

This module contains NO compiler logic. Both the compiler and the independent
verifier use it, the same way both would share a JSON library.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")


def canonical(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def hash_obj(obj) -> str:
    return hashlib.sha256(canonical(obj).encode("utf-8")).hexdigest()


def load_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def dump_json(obj, path):
    Path(path).write_text(
        json.dumps(obj, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# SQL expression tokeniser (used to resolve lineage expressions)
# ---------------------------------------------------------------------------

SQL_KEYWORDS = {
    "case", "when", "then", "else", "end", "and", "or", "not", "cast", "as",
    "in", "is", "like", "between", "null", "true", "false",
    # type names that may appear inside CAST(...)
    "varchar", "integer", "bigint", "int", "smallint", "double", "float",
    "decimal", "timestamp", "date", "boolean", "text",
}

TOKEN_RE = re.compile(
    r"""
      (?P<str>'(?:[^']|'')*')
    | (?P<num>\d+(?:\.\d+)?)
    | (?P<ident>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)
    | (?P<op>[(),+\-*/<>=!%\[\]]+)
    | (?P<ws>\s+)
    """,
    re.VERBOSE,
)


def tokenize(expr: str):
    """Split a SQL expression into (kind, text) tokens."""
    out = []
    pos = 0
    while pos < len(expr):
        m = TOKEN_RE.match(expr, pos)
        if not m:
            raise ValueError(f"cannot tokenize expression near: {expr[pos:pos + 20]!r}")
        pos = m.end()
        out.append((m.lastgroup, m.group()))
    return out
