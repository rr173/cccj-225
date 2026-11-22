"""Per-output-row lineage ("row_lineage.jsonl"): provenance carried in the package.

This module is an *input/execution layer* shared by the generator and the
independent verifier, like schema.py/common.py. It contains no policy or plan
logic: callers hand it a neutral description of one dataset (base table, join
chain, published columns, group keys) and it produces the provenance
statements and the streaming JSONL writer.

Provenance identity rules
-------------------------
* Source rows are identified ONLY by their declared stable business row keys
  (``_row_keys``), never by DuckDB physical rowid, scan order or output
  position. Identity is meaningful only inside the source snapshot pinned by
  the package manifest (table content hashes).
* A lineage entry binds the actually published (transformed) field values of
  one output row and a deterministic ``row_ref`` mapping identifier to the
  set of source records that produced it.
* Sources are grouped by source table and row-key values, sorted and
  de-duplicated. Ordinary join rows list every source record participating
  in the row; aggregate rows list ALL records participating in the group
  (never a count, sample or arbitrary single row). Composite keys and a
  source repeated through join paths are never dropped or double counted.
* Only row-key values appear in the file: no non-key source fields, no denied
  values, no policy explanations, no unpublished columns.

Streaming
---------
Provenance is computed in DuckDB (long, sorted, disk-spillable staging
relations) and pulled with bounded ``fetchmany`` batches; the Python process
never holds all source rows or all contribution relations at once.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from .common import canonical
from .errors import RowKeyError

LINEAGE_SUFFIX = ".row_lineage.jsonl"
LINEAGE_FORMAT = "row-lineage/1"


@dataclass(frozen=True)
class LineageColumn:
    """One published output column: output name + the executable SQL expression."""
    name: str
    sql: str


@dataclass(frozen=True)
class LineageJoin:
    """One LEFT JOIN step of the dataset's join chain."""
    from_table: str
    from_column: str
    to_table: str
    to_column: str


@dataclass(frozen=True)
class LineageStage:
    """Neutral description of one dataset's row-lineage computation.

    table_chain is [base, joined_table, ...] in join order; group_key_sqls are
    the executable output expressions of the projected group keys (empty for
    row-level datasets).
    """
    name: str
    base_table: str
    joins: tuple              # tuple[LineageJoin]
    columns: tuple            # tuple[LineageColumn], in published column order
    group_key_sqls: tuple
    table_chain: tuple

    @property
    def grouped(self):
        return bool(self.group_key_sqls)


# ---------------------------------------------------------------------------
# value / identity encoding
# ---------------------------------------------------------------------------

def normalize_scalar(value):
    """Convert a DuckDB scalar into a deterministic, JSON-able key/value form.

    Row keys and published values are leaf scalars only. Floats keep their
    Python ``repr`` (shortest round-trip representation, deterministic for a
    given IEEE-754 value); Decimal becomes float; date/time become ISO text.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    # date / time / timestamp and anything string-like
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, str):
        return value
    raise RowKeyError(
        f"row-key/output value of type {type(value).__name__} cannot be encoded "
        f"in row lineage; only scalar leaf columns may be row keys")


def make_row_ref(published: dict, sources: list) -> str:
    """Deterministic mapping identifier for one output row.

    Hash of the full published value multiset AND the exact source set, so
    identical projections produced by different source rows get distinct
    identifiers while regeneration from the same pinned snapshot is stable.
    The hash depends on neither physical rowid nor row position.
    """
    payload = {"values": published, "sources": sources}
    return "rr_" + hashlib.sha256(canonical(payload).encode("utf-8")).hexdigest()[:32]


def source_ref(table, key_columns, key_values):
    """One provenance source record: table + ordered composite key value map."""
    return {"table": table,
            "key": {c: normalize_scalar(v) for c, v in zip(key_columns, key_values)}}


# ---------------------------------------------------------------------------
# SQL construction (neutral; used by generator and verifier identically)
# ---------------------------------------------------------------------------

def _link_col(table, column):
    # internal staging column names can never collide with published names
    return f'__lk_{table}__{column}'


def join_clause(joins):
    sql = ""
    for j in joins:
        sql += (f' LEFT JOIN "{j.to_table}" AS {j.to_table} ON '
                f'{j.from_table}."{j.from_column}" = '
                f'{j.to_table}."{j.to_column}"')
    return sql


def build_row_stage_sql(stage: LineageStage, row_keys: dict) -> str:
    """Row-level provenance stage: one row per joined input row (pre-dedup).

    Ordered deterministically by every participating table's row-key values,
    so the result is independent of source insertion/scan order.
    """
    select = [f'{c.sql} AS "{c.name}"' for c in stage.columns]
    order = []
    for t in stage.table_chain:
        for kc in row_keys[t]:
            lc = _link_col(t, kc)
            select.append(f'{t}."{kc}" AS {lc}')
            order.append(f"{lc} NULLS LAST")
    sql = f'SELECT {", ".join(select)} FROM "{stage.base_table}" AS {stage.base_table}'
    sql += join_clause(stage.joins)
    sql += " ORDER BY " + ", ".join(order)
    return sql


def build_group_stage_sql(stage: LineageStage, row_keys: dict) -> str:
    """Grouped provenance stage: long (output values, source keys) rows.

    The dataset's aggregates are recomputed as window functions partitioned by
    exactly the plan's GROUP BY expression(s), so every joined row carries its
    group's published aggregate values. DISTINCT then yields one row per
    (published row, contributing source record); a source table absent on a
    LEFT JOIN contributes no row. Window functions spill to disk under the
    connection memory limit, and ordering is deterministic independently of
    source insertion/scan order.
    """
    gk_sqls = set(stage.group_key_sqls)
    win_select = []
    for c in stage.columns:
        if c.sql in gk_sqls:
            win_select.append(f'{c.sql} AS "{c.name}"')
        else:
            win_select.append(
                f'{c.sql} OVER (PARTITION BY {", ".join(stage.group_key_sqls)}) '
                f'AS "{c.name}"')
    order, gk_order = [], []
    for t in stage.table_chain:
        for kc in row_keys[t]:
            lc = _link_col(t, kc)
            win_select.append(f'{t}."{kc}" AS {lc}')
            order.append(f"{lc} NULLS LAST")
    for c in stage.columns:
        if c.sql in gk_sqls:
            gk_order.append(f'"{c.name}" NULLS LAST')
    inner = (f'SELECT {", ".join(win_select)} '
             f'FROM "{stage.base_table}" AS {stage.base_table}'
             + join_clause(stage.joins))
    all_cols = [f'"{c.name}"' for c in stage.columns] + [
        _link_col(t, kc) for t in stage.table_chain for kc in row_keys[t]]
    distinct = f"SELECT DISTINCT {', '.join(all_cols)} FROM ({inner})"
    return f"SELECT * FROM ({distinct}) ORDER BY " + ", ".join(gk_order + order)


# ---------------------------------------------------------------------------
# streaming JSONL writer
# ---------------------------------------------------------------------------

def _link_value(row, desc, table, key_columns):
    vals = [row[desc.index(_link_col(table, kc))] for kc in key_columns]
    if all(v is None for v in vals):
        return None  # LEFT JOIN had no matching record in this table
    return vals


def write_lineage(con, stage: LineageStage, row_keys: dict, out_path,
                  batch_size: int = 4096):
    """Execute a lineage stage and stream JSONL to disk with bounded memory.

    Returns {"entries": n, "source_refs": m}.

    Row-level datasets stream one output row per fetch batch. Grouped datasets
    materialise the long contribution relation into disk-backed DuckDB staging
    tables (they spill under the connection memory limit) and assemble each
    group's JSONL line, including the row_ref hash, entirely in SQL; Python
    only streams finished lines, so memory never scales with the source.
    """
    if not stage.grouped:
        return _write_row_lineage(con, stage, row_keys, out_path, batch_size)
    return _write_group_lineage(con, stage, row_keys, out_path, batch_size)


def _write_row_lineage(con, stage, row_keys, out_path, batch_size):
    sql = build_row_stage_sql(stage, row_keys)
    cur = con.execute(sql)
    col_names = [d[0] for d in cur.description]
    out_names = [c.name for c in stage.columns]
    out_idx = [col_names.index(n) for n in out_names]

    n_entries = n_refs = 0
    with open(out_path, "w", encoding="utf-8") as fh:
        while True:
            batch = cur.fetchmany(batch_size)
            if not batch:
                break
            for row in batch:
                sources, seen_here = [], set()
                for t in stage.table_chain:
                    vals = _link_value(row, col_names, t, row_keys[t])
                    if vals is None:
                        continue
                    ref = source_ref(t, row_keys[t], vals)
                    key = (t, canonical(ref["key"]))
                    if key in seen_here:
                        continue
                    seen_here.add(key)
                    sources.append(ref)
                sources.sort(key=lambda r: (r["table"], canonical(r["key"])))
                n_refs += len(sources)
                published = {n: normalize_scalar(row[i])
                             for n, i in zip(out_names, out_idx)}
                entry = {"row_ref": make_row_ref(published, sources),
                         "values": published, "sources": sources}
                fh.write(json.dumps(entry, ensure_ascii=False,
                                    separators=(",", ":")) + "\n")
                n_entries += 1
    return {"entries": n_entries, "source_refs": n_refs}


def _json_lit(expr):
    """SQL expression emitting the canonical JSON encoding of a scalar.

    Matches Python json.dumps for the leaf types used as row keys and
    published values: NULL -> null, booleans lower-cased, everything else via
    DuckDB to_json (numbers unquoted, strings quoted and escaped).
    """
    return (f"CASE WHEN {expr} IS NULL THEN 'null' "
            f"WHEN typeof({expr}) = 'BOOLEAN' THEN lower(cast({expr} AS VARCHAR)) "
            f"ELSE to_json({expr}) END")


def _write_group_lineage(con, stage, row_keys, out_path, batch_size):
    """Disk-backed grouped provenance with bounded Python memory.

    The long contribution relation is materialised in a spillable DuckDB
    table; every group's complete JSONL line -- canonical published values,
    sorted/de-duplicated source array and the row_ref hash -- is assembled
    inside SQL. Python only streams finished lines in fetch batches, so neither
    the whole source nor a single giant group's contribution set is held in
    the Python process. The canonical serialisation and sha256 match
    :func:`canonical` / :func:`make_row_ref` byte-for-byte.
    """
    long_sql = build_group_stage_sql(stage, row_keys)
    con.execute("DROP TABLE IF EXISTS __row_lineage_stage")
    con.execute(f"CREATE TEMP TABLE __row_lineage_stage AS {long_sql}")

    gk_sqls = set(stage.group_key_sqls)
    gk_names = [c.name for c in stage.columns if c.sql in gk_sqls]

    # one long, de-duplicated row per (group, contributing source record),
    # carrying the canonical key JSON, the table and the full canonical ref.
    pair_selects = []
    gk_qual = ", ".join(f's."{g}"' for g in gk_names)
    for t in stage.table_chain:
        link_cols = [_link_col(t, kc) for kc in row_keys[t]]
        not_null = " OR ".join(f"{lc} IS NOT NULL" for lc in link_cols)
        # canonical() sorts nested object keys, so build from sorted pairs
        pairs = sorted(zip(row_keys[t], link_cols))
        key_segs = [f'to_json(\'{kc}\') || \':\' || {_json_lit(lc)}'
                    for kc, lc in pairs]
        key_obj = "concat('{', " + ", ',', ".join(key_segs) + ", '}')"
        ref_obj = (f"concat('{{\"key\":', {key_obj}, ',\"table\":', "
                   f"to_json('{t}'), '}}')")
        pair_selects.append(
            f"SELECT {gk_qual}, to_json('{t}') AS __src_table, "
            f"{key_obj} AS __src_key, {ref_obj} AS __src_canon "
            f"FROM __row_lineage_stage s WHERE {not_null}")
    con.execute("DROP TABLE IF EXISTS __row_lineage_canon")
    con.execute(
        "CREATE TEMP TABLE __row_lineage_canon AS "
        "SELECT DISTINCT * FROM (" + " UNION ALL ".join(pair_selects) + ")")

    gk_list = ", ".join(f'"{g}"' for g in gk_names)
    gk_order = ", ".join(f'v."{g}" NULLS LAST' for g in gk_names)
    # canonical source ARRAY per group, members de-duplicated and ordered by
    # (table, canonical key) -- the required grouping/sort order.
    src_array = ("concat('[', coalesce(string_agg(__src_canon, ',' "
                 "ORDER BY __src_table, __src_key), ''), ']')")
    con.execute("DROP TABLE IF EXISTS __row_lineage_src")
    con.execute(
        f"CREATE TEMP TABLE __row_lineage_src AS "
        f"SELECT {gk_list}, {src_array} AS __src_array "
        f"FROM __row_lineage_canon GROUP BY {gk_list}")

    # canonical published VALUES object per group. canonical() sorts object
    # keys, so segments are emitted in sorted column-name order, not output order.
    out_names = [c.name for c in stage.columns]
    val_segs = []
    for n in sorted(out_names):
        col_ref = '"' + n + '"'
        val_segs.append(
            "to_json('" + n + "') || ':' || " + _json_lit(col_ref))
    val_obj = "concat('{', " + ", ',', ".join(val_segs) + ", '}')"
    val_cols = ", ".join(f'"{g}"' for g in gk_names)
    con.execute("DROP TABLE IF EXISTS __row_lineage_val")
    con.execute(
        f"CREATE TEMP TABLE __row_lineage_val AS "
        f"SELECT DISTINCT {val_cols}, {val_obj} AS __val_canon "
        f"FROM __row_lineage_stage")

    # Assemble the final line and row_ref in SQL. The hashed payload is
    # canonical({"values":...,"sources":...}); canonical() sorts object keys,
    # and "sources" < "values", so its exact bytes are
    # concat('{"sources":', src, ',"values":', val, '}').
    join_on = " AND ".join(f'v."{g}" IS NOT DISTINCT FROM r."{g}"'
                           for g in gk_names)
    line_sql = f"""
        SELECT concat(
            '{{"row_ref":"rr_',
            substr(sha256(concat('{{"sources":', r.__src_array,
                                 ',"values":', v.__val_canon, '}}')), 1, 32),
            '","values":', v.__val_canon,
            ',"sources":', r.__src_array, '}}') AS __line
        FROM __row_lineage_val v
        JOIN __row_lineage_src r ON {join_on}
        ORDER BY {gk_order}
    """
    cur = con.execute(line_sql)
    n_entries = n_refs = 0
    with open(out_path, "w", encoding="utf-8") as fh:
        while True:
            batch = cur.fetchmany(batch_size)
            if not batch:
                break
            for (line,) in batch:
                fh.write(line + "\n")
                n_entries += 1
                n_refs += line.count('"table":')
    con.execute("DROP TABLE __row_lineage_stage")
    con.execute("DROP TABLE __row_lineage_canon")
    con.execute("DROP TABLE __row_lineage_src")
    con.execute("DROP TABLE __row_lineage_val")
    return {"entries": n_entries, "source_refs": n_refs}


def read_entries(path, batch_size: int = 4096):
    """Stream JSONL lineage entries from disk (offline, stdlib only)."""
    with open(Path(path), "r", encoding="utf-8") as fh:
        batch = []
        for line in fh:
            line = line.strip()
            if not line:
                continue
            batch.append(json.loads(line))
            if len(batch) >= batch_size:
                for e in batch:
                    yield e
                batch = []
        for e in batch:
            yield e
