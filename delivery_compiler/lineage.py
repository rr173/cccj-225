"""Field lineage: derived-field expressions, identifier resolution, cycle detection.

A derived field is declared as {name, expression, inputs, via}. The expression
is SQL over fully qualified source paths (table.column[.sub]) and/or other
derived fields. The compiler resolves every identifier, checks the declared
inputs match the actual references exactly, rewrites the expression to
executable SQL, and detects cycles. Anything unresolvable is never published.
"""
from __future__ import annotations

from dataclasses import dataclass

from .common import SQL_KEYWORDS, tokenize
from .errors import (CyclicLineageError, DanglingReferenceError,
                     UnresolvableLineageError)
from .schema import SourceSchema, field_sql


@dataclass
class ResolvedDerived:
    name: str
    sql: str                 # executable SQL (table-qualified, struct access)
    source_paths: tuple      # transitive closure of source field paths
    direct_refs: tuple       # direct references (source paths and derived names)
    via: tuple


class LineageResolver:
    def __init__(self, lineage_doc, schema: SourceSchema):
        self.defs = {}
        for d in lineage_doc:
            if d["name"] in self.defs:
                raise UnresolvableLineageError(f"duplicate derived field '{d['name']}'")
            self.defs[d["name"]] = d
        self.schema = schema
        self._cache = {}
        self._stack = []

    def resolve(self, name) -> ResolvedDerived:
        if name in self._cache:
            return self._cache[name]
        if name in self._stack:
            raise CyclicLineageError(
                "cyclic lineage detected: " + " -> ".join(self._stack + [name]))
        d = self.defs.get(name)
        if d is None:
            raise UnresolvableLineageError(
                f"derived field '{name}' is not declared in the lineage document")
        relation_names = {r.name for r in self.schema.relations}
        for v in d.get("via", []):
            if v not in relation_names:
                raise DanglingReferenceError(
                    f"derived field '{name}': unknown join path (relation) '{v}'")
        self._stack.append(name)
        try:
            rd = self._resolve_def(name, d)
            self._cache[name] = rd
            return rd
        finally:
            self._stack.pop()

    def _resolve_def(self, name, d) -> ResolvedDerived:
        try:
            toks = tokenize(d["expression"])
        except ValueError as e:
            raise UnresolvableLineageError(f"derived field '{name}': {e}") from e
        refs = []
        parts = []
        for i, (kind, text) in enumerate(toks):
            if kind != "ident":
                parts.append(text)
                continue
            nxt = toks[i + 1] if i + 1 < len(toks) else None
            is_func = nxt is not None and nxt[0] == "op" and nxt[1].startswith("(")
            if text.lower() in SQL_KEYWORDS or is_func:
                parts.append(text)
                continue
            if "." in text:
                fi = self.schema.field_or_none(text)
                if fi is None or not fi.leaf:
                    raise UnresolvableLineageError(
                        f"cannot resolve '{text}' in expression of derived field "
                        f"'{name}': unknown or non-leaf source field")
                refs.append(text)
                parts.append(field_sql(self.schema, text))
            else:
                if text not in self.defs:
                    raise UnresolvableLineageError(
                        f"cannot resolve identifier '{text}' in expression of derived "
                        f"field '{name}': not a fully qualified source field and not a "
                        f"declared derived field")
                sub = self.resolve(text)
                refs.append(text)
                parts.append(f"({sub.sql})")
        declared = set(d.get("inputs", []))
        got = set(refs)
        if declared != got:
            raise UnresolvableLineageError(
                f"derived field '{name}': declared inputs {sorted(declared)} do not "
                f"match expression references {sorted(got)}")
        srcs = []
        for r in refs:
            if "." in r:
                srcs.append(r)
            else:
                srcs.extend(self._cache[r].source_paths)
        return ResolvedDerived(
            name=name, sql="".join(parts),
            source_paths=tuple(sorted(set(srcs))),
            direct_refs=tuple(sorted(got)),
            via=tuple(d.get("via", ())))
