"""Source lake schema: tables, struct-flattened field paths, tags, relations, snapshot.

This is an *input* reader shared by compiler and verifier. It performs no
policy logic; it only describes what exists in the pinned source version.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

import duckdb

from .errors import DanglingReferenceError

SENSITIVE_TAGS = frozenset({"pii", "gov_id", "financial"})


@dataclass
class FieldInfo:
    table: str
    path: str            # fully qualified: table.col[.sub ...]
    rel: tuple           # path within the table, e.g. ("contact", "email")
    dtype: str
    leaf: bool
    tags: set = field(default_factory=set)


@dataclass(frozen=True)
class Relation:
    name: str
    from_table: str
    from_column: str
    to_table: str
    to_column: str


def _split_top_level(s: str):
    parts, depth, cur = [], 0, []
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if cur:
        parts.append("".join(cur))
    return [p.strip() for p in parts if p.strip()]


def _parse_struct(type_str: str):
    """'STRUCT(line1 VARCHAR, city VARCHAR)' -> [('line1', 'VARCHAR'), ...]"""
    inner = type_str[type_str.index("(") + 1: type_str.rindex(")")]
    out = []
    for part in _split_top_level(inner):
        m = re.match(r'^"?([A-Za-z_][A-Za-z0-9_]*)"?\s+(.+)$', part)
        if not m:
            raise DanglingReferenceError(f"cannot parse struct member {part!r} in {type_str!r}")
        out.append((m.group(1), m.group(2).strip()))
    return out


class SourceSchema:
    def __init__(self):
        self.version = None
        self.tables = {}        # table -> {full_path: FieldInfo}
        self.top_columns = {}   # table -> [top-level column names]
        self.relations = []     # list[Relation]
        self.table_hashes = {}  # table -> {"rows": n, "hash": str}

    # -- loading -----------------------------------------------------------
    @classmethod
    def load(cls, db_path):
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            return cls.from_con(con)
        finally:
            con.close()

    @classmethod
    def from_con(cls, con):
        self = cls()
        self.version = con.execute("SELECT version FROM _snapshot").fetchone()[0]
        tables = [r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' ORDER BY table_name").fetchall()
                  if not r[0].startswith("_")]
        for t in tables:
            cols = con.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = ? ORDER BY ordinal_position", [t]).fetchall()
            self.top_columns[t] = [c for c, _ in cols]
            fields = {}
            for cname, dtype in cols:
                self._register(fields, t, (cname,), dtype)
            self.tables[t] = fields
        # field tags
        tagmap = {}
        for path, tag in con.execute("SELECT path, tag FROM _field_tags").fetchall():
            tagmap.setdefault(path, set()).add(tag)
        for path, tags in tagmap.items():
            fi = self.field(path)  # raises on dangling tag reference
            fi.tags |= tags
        # relations (join paths); endpoints are validated eagerly
        for name, ft, fc, tt, tc in con.execute(
                "SELECT name, from_table, from_column, to_table, to_column "
                "FROM _relations ORDER BY name").fetchall():
            if ft not in self.tables or fc not in self.top_columns.get(ft, []):
                raise DanglingReferenceError(f"relation '{name}': dangling endpoint {ft}.{fc}")
            if tt not in self.tables or tc not in self.top_columns.get(tt, []):
                raise DanglingReferenceError(f"relation '{name}': dangling endpoint {tt}.{tc}")
            self.relations.append(Relation(name, ft, fc, tt, tc))
        # pinned content hashes (order-independent per-row hash)
        for t in tables:
            cols = self.top_columns[t]
            expr = "concat_ws(chr(31), " + ", ".join(
                f'CAST("{c}" AS VARCHAR)' for c in cols) + ")"
            n, h = con.execute(
                f'SELECT count(*), coalesce(bit_xor(hash({expr})), 0) FROM "{t}"').fetchone()
            self.table_hashes[t] = {"rows": int(n), "hash": str(h)}
        return self

    def _register(self, fields, table, rel, dtype):
        path = f"{table}." + ".".join(rel)
        if dtype.upper().startswith("STRUCT("):
            fields[path] = FieldInfo(table, path, rel, dtype, leaf=False, tags=set())
            for sub, subty in _parse_struct(dtype):
                self._register(fields, table, rel + (sub,), subty)
        else:
            fields[path] = FieldInfo(table, path, rel, dtype, leaf=True, tags=set())

    # -- accessors ---------------------------------------------------------
    def field_or_none(self, path):
        t = path.split(".")[0]
        return self.tables.get(t, {}).get(path)

    def field(self, path) -> FieldInfo:
        fi = self.field_or_none(path)
        if fi is None:
            raise DanglingReferenceError(f"unknown field path '{path}'")
        return fi

    def tags_of(self, path):
        return self.field(path).tags

    def is_sensitive(self, path):
        return bool(self.field(path).tags & SENSITIVE_TAGS)

    def leaf_paths(self):
        return sorted(p for t in self.tables.values() for p, f in t.items() if f.leaf)

    # -- join graph ----------------------------------------------------------
    def _reach(self, base):
        adj = {}
        for r in self.relations:
            adj.setdefault(r.from_table, []).append(r)
        reach = {base: []}
        queue = [base]
        while queue:
            cur = queue.pop(0)
            for r in adj.get(cur, []):
                if r.to_table not in reach:
                    reach[r.to_table] = reach[cur] + [r]
                    queue.append(r.to_table)
        return reach

    def find_base(self, needed_tables):
        """Pick a deterministic base table that can reach all needed tables
        following many-to-one relations. Returns (base, {table: [relations]})
        with the reach map pruned to the needed tables only."""
        needed = set(needed_tables)
        best = None
        for t in sorted(needed):
            reach = self._reach(t)
            if all(n in reach for n in needed):
                cost = sum(len(reach[n]) for n in needed)
                if best is None or (cost, t) < (best[0], best[1]):
                    best = (cost, t, reach)
        if best is None:
            raise DanglingReferenceError(
                f"no declared join path covers required tables {sorted(needed)}")
        base, reach = best[1], best[2]
        # prune to needed tables, preserving BFS order so joins chain correctly
        return base, {n: reach[n] for n in reach if n in needed}


def field_sql(schema: SourceSchema, path: str) -> str:
    """Executable SQL for a field path: t."col"['sub']..."""
    fi = schema.field(path)
    expr = f'{fi.table}."{fi.rel[0]}"'
    for sub in fi.rel[1:]:
        expr += f"['{sub}']"
    return expr
