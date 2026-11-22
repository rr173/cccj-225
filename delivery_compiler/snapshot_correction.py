"""Snapshot correction: build a new source database from an old snapshot plus
one batch of business corrections (``snapshot-correct``).

The old snapshot is closed and never modified. A UTF-8 JSONL *correction
sheet* describes, per line, one row-level operation on one of the demo
business tables (``region``, ``customer_profile``, ``transactions``):

    {"table": "region", "key": {"region_code": "R5"}, "op": "insert",
     "values": {"region_name": "Northeast", "country": "US", "tier": "T1"}}
    {"table": "transactions", "key": {"txn_id": 1001}, "op": "update",
     "values": {"customer_id": 13}}
    {"table": "region", "key": {"region_code": "R2"}, "op": "delete"}

A batch is a *set* of corrections against the same old snapshot, not an
ordered command script: the same (table, row key) may appear at most once,
and reordering lines never changes the resulting database. Child rows may be
listed before the new parents they need; all referential checks run against
the fully corrected database (including rows the sheet never touches).

Correctness / publication rules
-------------------------------
* Every value is validated against the existing column type before anything
  is written: ISO date-time strings for timestamps (numeric strings are never
  accepted as numbers), STRUCT columns are replaced as whole top-level columns
  using JSON objects whose members exactly match the existing struct.
* Insert keys must not exist in the old table; update/delete keys must. Keys
  can never be changed (they live in ``key``, never in ``values``).
* After the whole batch is applied, every row key and the two declared
  relations (transaction -> customer, customer -> region) are re-validated
  over ALL rows: each foreign key must be non-NULL and match exactly one
  parent. Deleting a parent that still has unedited children is rejected.
* The new database keeps every table definition, key declaration, field tag
  and relation statement; only business data and the ``_snapshot`` version
  change. It loads through the existing
  :class:`~delivery_compiler.schema.SourceSchema` and works with ``compile``
  and ``generate`` unchanged.
* The target path is the publication boundary. A completed database reaches
  it only through an atomic, no-clobber hard-link publish from a private
  staging file; failures (bad digest, same version, invalid corrections,
  interruption) never create or overwrite the target. Retrying an identical
  request recognises the already-published result and rewrites nothing; a
  different request aimed at an occupied target is a conflict.

Bounded memory
--------------
The correction sheet is streamed line by line and staged in a DuckDB table
(batched reads/writes), and source rows are never pulled into Python:
existence and referential checks are set-based SQL and corrections are
re-applied through ``fetchmany`` batches. The Python process never holds the
whole sheet or the whole source in memory.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import tempfile
from datetime import date, datetime
from pathlib import Path

import duckdb

from .common import canonical
from .errors import CorrectionError, SourceDriftError
from .schema import SourceSchema, _parse_struct

# Only the three demo business tables are correctable; metadata tables and
# schema changes are explicitly out of scope.
CORRECTABLE_TABLES = frozenset(("region", "customer_profile", "transactions"))
OPS = frozenset(("insert", "update", "delete"))
MARKER_TABLE = "_snapshot_correction"
STAGING_TABLE = "__corrections_stage"
BATCH_SIZE = 512
_CHUNK = 1 << 20


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------

def _qident(name: str) -> str:
    """Double-quote an identifier; callers only pass validated column names."""
    return '"' + name.replace('"', '""') + '"'


def _qjson_path(name: str) -> str:
    """A JSON path literal (``'$."name"'``) with quotes/backslashes escaped."""
    escaped = name.replace("\\", "\\\\").replace('"', '\\"')
    return "'$.\"" + escaped + "\"'"


def _sha256_stream(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def _table_columns(con, table: str) -> list:
    """[(column_name, upper-cased logical type)] in ordinal order."""
    return con.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = ? ORDER BY ordinal_position", [table]).fetchall()


def _column_type(con, table, column):
    return con.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = ? AND column_name = ?",
        [table, column]).fetchone()[0]


# ---------------------------------------------------------------------------
# strict value validation against existing column types
# ---------------------------------------------------------------------------

class _TypeSpec:
    """Resolved type description of one top-level column."""

    __slots__ = ("kind", "type_str", "members")

    def __init__(self, kind: str, type_str: str, members=None):
        self.kind = kind            # SCALAR | STRUCT
        self.type_str = type_str
        self.members = members      # list[(member_name, _TypeSpec)]


def _build_type_spec(type_str: str) -> _TypeSpec:
    up = type_str.upper()
    if up.startswith("STRUCT("):
        members = [(name, _build_type_spec(sub))
                   for name, sub in _parse_struct(type_str)]
        return _TypeSpec("STRUCT", type_str, members)
    # The demo schema only uses scalar leaf types; anything else (LIST, MAP,
    # ENUM, ...) is not representable from a JSONL correction and is rejected.
    if up in ("VARCHAR", "TEXT", "BIGINT", "INTEGER", "INT", "SMALLINT",
              "DOUBLE", "FLOAT", "REAL", "BOOLEAN", "TIMESTAMP", "DATE"):
        return _TypeSpec("SCALAR", type_str)
    raise CorrectionError(
        f"column type {type_str!r} is not supported by snapshot-correct; only "
        f"the scalar and STRUCT types used by the demo schema are correctable")


def _parse_timestamp(value, *, where: str):
    if not isinstance(value, str):
        raise CorrectionError(
            f"{where}: TIMESTAMP must be an ISO date-time string, got "
            f"{type(value).__name__}")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as e:
        raise CorrectionError(f"{where}: invalid TIMESTAMP {value!r}: {e}") from e
    if parsed.tzinfo is not None:
        raise CorrectionError(
            f"{where}: TIMESTAMP {value!r} must be a naive date-time without "
            f"time zone")
    # A bare date ("2024-01-02") is a DATE, not a TIMESTAMP.
    if len(value) == 10:
        raise CorrectionError(
            f"{where}: TIMESTAMP {value!r} must include a time of day")
    if parsed.microsecond and len(value.split(".", 1)[1].rstrip("Z")) > 6:
        raise CorrectionError(
            f"{where}: TIMESTAMP {value!r} has sub-microsecond precision and "
            f"cannot be represented exactly")
    return parsed


def _parse_date(value, *, where: str):
    if not isinstance(value, str):
        raise CorrectionError(
            f"{where}: DATE must be an ISO date string, got "
            f"{type(value).__name__}")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as e:
        raise CorrectionError(f"{where}: invalid DATE {value!r}: {e}") from e
    if len(value) != 10:
        raise CorrectionError(f"{where}: DATE {value!r} must be YYYY-MM-DD")
    return parsed


def _convert_value(value, spec: _TypeSpec, *, where: str):
    """Validate one JSON-decoded value against a column spec.

    Returns a value DuckDB accepts through a typed parameter. Raises
    CorrectionError on anything that cannot be represented exactly -- notably
    numeric strings are never coerced to numbers and booleans never to ints.
    """
    if value is None:
        return None
    up = spec.type_str.upper()
    if spec.kind == "STRUCT":
        if not isinstance(value, dict):
            raise CorrectionError(
                f"{where}: {up} column must be replaced with a JSON object")
        have = set(value)
        want = {name for name, _ in spec.members}
        if have != want:
            detail = []
            missing = sorted(want - have)
            extra = sorted(have - want)
            if missing:
                detail.append(f"missing member(s) {missing}")
            if extra:
                detail.append(f"unknown member(s) {extra}")
            raise CorrectionError(
                f"{where}: struct object must use exactly the existing "
                f"members {sorted(want)}; " + "; ".join(detail))
        return {name: _convert_value(value[name], sub,
                                     where=f"{where}.{name}")
                for name, sub in spec.members}
    if up in ("VARCHAR", "TEXT"):
        if not isinstance(value, str):
            raise CorrectionError(
                f"{where}: {up} column requires a string, got "
                f"{type(value).__name__}")
        return value
    if up in ("BIGINT", "INTEGER", "INT", "SMALLINT"):
        if isinstance(value, bool) or not isinstance(value, int):
            raise CorrectionError(
                f"{where}: {up} column requires a JSON integer, got "
                f"{type(value).__name__} {value!r}")
        limits = {"SMALLINT": (-32768, 32767),
                  "INTEGER": (-2147483648, 2147483647),
                  "INT": (-2147483648, 2147483647),
                  "BIGINT": (-9223372036854775808, 9223372036854775807)}
        lo, hi = limits[up]
        if not lo <= value <= hi:
            raise CorrectionError(
                f"{where}: integer {value} does not fit into {up}")
        return value
    if up in ("DOUBLE", "FLOAT", "REAL"):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise CorrectionError(
                f"{where}: {up} column requires a JSON number, got "
                f"{type(value).__name__} {value!r}")
        num = float(value)
        if not math.isfinite(num):
            raise CorrectionError(
                f"{where}: {up} value {value!r} is not finite and cannot be "
                f"represented")
        return num
    if up == "BOOLEAN":
        if not isinstance(value, bool):
            raise CorrectionError(
                f"{where}: BOOLEAN column requires true/false, got "
                f"{type(value).__name__} {value!r}")
        return value
    if up == "TIMESTAMP":
        return _parse_timestamp(value, where=where)
    if up == "DATE":
        return _parse_date(value, where=where)
    raise CorrectionError(f"{where}: unsupported column type {spec.type_str!r}")


# ---------------------------------------------------------------------------
# streaming parse + staging
# ---------------------------------------------------------------------------

def _validate_record(obj, line_no, specs, key_cols):
    """Validate one decoded JSONL object's shape and value types.

    Returns (table, op, canonical_key, raw_values_dict_or_None). Converted
    values are not retained: only the raw JSON text is staged on disk, and
    conversion is repeated during application, keeping per-line state tiny.
    """
    if not isinstance(obj, dict):
        raise CorrectionError(f"line {line_no}: correction must be a JSON object")
    for field_name in ("table", "key", "op"):
        if field_name not in obj:
            raise CorrectionError(
                f"line {line_no}: missing required field {field_name!r}")
    table = obj["table"]
    if not isinstance(table, str) or table not in CORRECTABLE_TABLES:
        raise CorrectionError(
            f"line {line_no}: unknown table {table!r}; allowed tables: "
            f"{sorted(CORRECTABLE_TABLES)}")
    op = obj["op"]
    if not isinstance(op, str) or op not in OPS:
        raise CorrectionError(
            f"line {line_no}: unknown op {op!r}; allowed ops: {sorted(OPS)}")
    key = obj["key"]
    if not isinstance(key, dict) or not key:
        raise CorrectionError(
            f"line {line_no}: key must be a non-empty object naming the "
            f"declared row-key column(s)")
    want_keys = key_cols[table]
    if set(key) != set(want_keys):
        raise CorrectionError(
            f"line {line_no}: key for table {table!r} must name exactly "
            f"{want_keys}, got {sorted(key)}")
    converted_key = {}
    for col in want_keys:
        if key[col] is None:
            raise CorrectionError(
                f"line {line_no}: row-key column {col!r} must not be NULL")
        converted_key[col] = _convert_value(
            key[col], specs[table][col],
            where=f"line {line_no} key {table}.{col}")
    canon_key = canonical(converted_key)

    if "values" in obj and not isinstance(obj["values"], dict):
        raise CorrectionError(f"line {line_no}: values must be an object")
    raw_values = obj.get("values")
    if op == "delete":
        if raw_values is not None:
            raise CorrectionError(
                f"line {line_no}: delete must not carry values")
        return table, op, canon_key, None
    if raw_values is None:
        raise CorrectionError(
            f"line {line_no}: {op} requires a values object")
    if not raw_values:
        raise CorrectionError(
            f"line {line_no}: {op} values must list at least one non-key column")
    non_key = {c for c in specs[table] if c not in want_keys}
    unknown = sorted(set(raw_values) - non_key)
    if unknown:
        raise CorrectionError(
            f"line {line_no}: unknown or key column(s) {unknown} in values; "
            f"row keys cannot be changed and unknown columns are rejected")
    for col, raw in raw_values.items():
        _convert_value(raw, specs[table][col],
                       where=f"line {line_no} values {table}.{col}")
    if op == "insert":
        missing_cols = sorted(non_key - set(raw_values))
        if missing_cols:
            raise CorrectionError(
                f"line {line_no}: insert must give every non-key column "
                f"(explicit null allowed); missing {missing_cols}")
    return table, op, canon_key, raw_values


def _stream_sheet(con, sheet_path: Path, specs, key_cols):
    """Stream the JSONL sheet into an in-database staging table.

    Blank lines are tolerated; every non-blank line must be one valid JSON
    object. Duplicate (table, key) entries are rejected by both a per-batch
    bounded Python check and a PRIMARY KEY constraint inside DuckDB, so the
    sheet is never accumulated in Python memory.
    """
    con.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE}")
    con.execute(
        f"CREATE TABLE {STAGING_TABLE} ("
        "line_no BIGINT NOT NULL, table_name VARCHAR NOT NULL, "
        "op VARCHAR NOT NULL, key_json VARCHAR NOT NULL, "
        "values_json VARCHAR, PRIMARY KEY (table_name, key_json))")
    pending, n_lines, batch_seen = [], 0, {}

    def insert_rows(rows):
        # Wrap the batch so a constraint failure rolls back any rows the
        # executemany managed to insert; otherwise pinpointing the collision
        # would trip over the partially inserted batch itself.
        con.execute("BEGIN TRANSACTION")
        try:
            con.executemany(
                f"INSERT INTO {STAGING_TABLE} VALUES (?, ?, ?, ?, ?)", rows)
            con.execute("COMMIT")
        except duckdb.ConstraintException:
            con.execute("ROLLBACK")
            # Re-insert row by row to pinpoint the first colliding line.
            for row in rows:
                try:
                    con.execute(
                        f"INSERT INTO {STAGING_TABLE} VALUES (?, ?, ?, ?, ?)",
                        row)
                except duckdb.ConstraintException:
                    prior = con.execute(
                        f"SELECT line_no FROM {STAGING_TABLE} "
                        "WHERE table_name = ? AND key_json = ?",
                        [row[1], row[3]]).fetchone()
                    prior_no = prior[0] if prior else "an earlier line"
                    raise CorrectionError(
                        f"line {row[0]}: table {row[1]!r} key {row[3]} appears "
                        f"more than once in this batch (first at line "
                        f"{prior_no}); each row key may be corrected at most "
                        f"once") from None

    with open(sheet_path, "rb") as fh:
        for line_no, raw in enumerate(fh, start=1):
            if not raw.strip():
                continue
            n_lines += 1
            try:
                obj = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                raise CorrectionError(
                    f"line {line_no}: cannot parse UTF-8 JSON: {e}") from e
            table, op, canon_key, raw_values = _validate_record(
                obj, line_no, specs, key_cols)
            if (table, canon_key) in batch_seen:
                raise CorrectionError(
                    f"line {line_no}: table {table!r} key {canon_key} appears "
                    f"more than once in this batch (first at line "
                    f"{batch_seen[(table, canon_key)]}); each row key may be "
                    f"corrected at most once")
            batch_seen[(table, canon_key)] = line_no
            pending.append((
                line_no, table, op, canon_key,
                json.dumps(raw_values, sort_keys=True, ensure_ascii=False)
                if raw_values is not None else None))
            if len(pending) >= BATCH_SIZE:
                insert_rows(pending)
                pending, batch_seen = [], {}
    if not n_lines:
        raise CorrectionError("correction sheet contains no correction lines")
    if pending:
        insert_rows(pending)


# ---------------------------------------------------------------------------
# existence preconditions (set-based SQL; no source rows enter Python)
# ---------------------------------------------------------------------------

def _materialise_keys(con, table, key_cols):
    """Temp table of referenced keys: op + canonical key JSON + typed keys."""
    tmp = f"__keys_{table}"
    con.execute(f"DROP TABLE IF EXISTS {tmp}")
    extracted = []
    for col in key_cols:
        extracted.append(
            f"CAST(json_extract_string(key_json, {_qjson_path(col)}) AS "
            f"{_column_type(con, table, col)}) AS {_qident(col)}")
    con.execute(
        f"CREATE TEMP TABLE {tmp} AS "
        f"SELECT s.op, s.key_json AS key_json, {', '.join(extracted)} "
        f"FROM {STAGING_TABLE} s WHERE s.table_name = ?", [table])
    return tmp


def _check_preconditions(con, table, key_cols):
    tmp = f"__keys_{table}"
    key_eq = " AND ".join(
        f"k.{_qident(c)} = t.{_qident(c)}" for c in key_cols)
    colliding = con.execute(
        f"SELECT k.key_json FROM {tmp} k "
        f"JOIN {_qident(table)} t ON {key_eq} "
        f"WHERE k.op = 'insert' ORDER BY k.key_json LIMIT 1").fetchone()
    if colliding:
        line_no = con.execute(
            f"SELECT min(line_no) FROM {STAGING_TABLE} "
            "WHERE table_name = ? AND op = 'insert' AND key_json = ?",
            [table, colliding[0]]).fetchone()[0]
        raise CorrectionError(
            f"line {line_no}: insert target key already exists in table "
            f"{table!r}; inserts require a new key")
    missing = con.execute(
        f"SELECT k.key_json FROM {tmp} k "
        f"LEFT JOIN {_qident(table)} t ON {key_eq} "
        f"WHERE k.op IN ('update','delete') "
        f"AND t.{_qident(key_cols[0])} IS NULL "
        f"ORDER BY k.key_json LIMIT 1").fetchone()
    if missing:
        line_no = con.execute(
            f"SELECT min(line_no) FROM {STAGING_TABLE} "
            "WHERE table_name = ? AND op IN ('update','delete') AND key_json = ?",
            [table, missing[0]]).fetchone()[0]
        raise CorrectionError(
            f"line {line_no}: update/delete target key does not exist in "
            f"table {table!r}")


# ---------------------------------------------------------------------------
# application of staged corrections (fetchmany batches, re-validated types)
# ---------------------------------------------------------------------------

def _apply_table(con, table, key_cols, specs):
    non_key_cols = [c for c in specs[table] if c not in key_cols]
    # Windowed batches over the staging table. Each SELECT is fully drained
    # before any DML runs on this connection (a new execute() would otherwise
    # replace the open result set); memory stays bounded by BATCH_SIZE.
    offset = 0
    while True:
        batch = con.execute(
            f"SELECT line_no, op, key_json, values_json FROM {STAGING_TABLE} "
            "WHERE table_name = ? ORDER BY line_no LIMIT ? OFFSET ?",
            [table, BATCH_SIZE, offset]).fetchall()
        if not batch:
            break
        offset += len(batch)
        for line_no, op, key_json, values_json in batch:
            key_obj = json.loads(key_json)
            key_params = [
                _convert_value(key_obj[c], specs[table][c],
                               where=f"line {line_no} key {table}.{c}")
                for c in key_cols]
            where_sql = " AND ".join(
                f"{_qident(c)} = ?" for c in key_cols)
            if op == "delete":
                con.execute(
                    f"DELETE FROM {_qident(table)} WHERE {where_sql}",
                    key_params)
                continue
            values_obj = json.loads(values_json)
            if op == "insert":
                cols = key_cols + non_key_cols
                params = list(key_params)
                for c in non_key_cols:
                    params.append(_convert_value(
                        values_obj[c], specs[table][c],
                        where=f"line {line_no} values {table}.{c}"))
                placeholders = ", ".join("?" for _ in cols)
                con.execute(
                    f"INSERT INTO {_qident(table)} "
                    f"({', '.join(_qident(c) for c in cols)}) "
                    f"VALUES ({placeholders})", params)
            else:  # update: only listed non-key columns are replaced
                set_cols = [c for c in non_key_cols if c in values_obj]
                params = [
                    _convert_value(values_obj[c], specs[table][c],
                                   where=f"line {line_no} values {table}.{c}")
                    for c in set_cols]
                set_sql = ", ".join(f"{_qident(c)} = ?" for c in set_cols)
                con.execute(
                    f"UPDATE {_qident(table)} SET {set_sql} WHERE {where_sql}",
                    params + key_params)


# ---------------------------------------------------------------------------
# whole-database integrity checks (set based; never pulls rows into Python)
# ---------------------------------------------------------------------------

def _check_row_keys(con, schema: SourceSchema):
    """Re-assert non-NULL unique row keys over every corrected business table."""
    for table, cols in schema.row_keys.items():
        null_sql = " OR ".join(f"{_qident(c)} IS NULL" for c in cols)
        nulls = con.execute(
            f"SELECT count(*) FROM {_qident(table)} WHERE {null_sql}").fetchone()[0]
        if nulls:
            raise CorrectionError(
                f"table {table!r}: {nulls} row(s) have NULL row-key values "
                f"after correction")
        distinct = con.execute(
            "SELECT count(*) FROM (SELECT DISTINCT "
            + ", ".join(_qident(c) for c in cols)
            + f" FROM {_qident(table)})").fetchone()[0]
        total = con.execute(
            f"SELECT count(*) FROM {_qident(table)}").fetchone()[0]
        if distinct != total:
            raise CorrectionError(
                f"table {table!r}: row key {cols} is not unique after "
                f"correction ({total} rows, {distinct} distinct)")


def _check_relations(con, schema: SourceSchema):
    """Validate every declared relation over the FULLY corrected tables.

    Each child key must be non-NULL and match exactly one parent row. This
    covers unedited children whose parent was deleted, not just edited rows.
    """
    for rel in schema.relations:
        child, parent = rel.from_table, rel.to_table
        cc, pc = rel.from_column, rel.to_column
        nulls = con.execute(
            f"SELECT count(*) FROM {_qident(child)} "
            f"WHERE {_qident(cc)} IS NULL").fetchone()[0]
        if nulls:
            raise CorrectionError(
                f"relation {rel.name!r}: {nulls} row(s) in {child!r} have a "
                f"NULL foreign key {cc!r}; empty foreign keys are not allowed")
        dangling = con.execute(
            f"SELECT count(*) FROM {_qident(child)} c WHERE NOT EXISTS ("
            f"SELECT 1 FROM {_qident(parent)} p "
            f"WHERE p.{_qident(pc)} IS NOT DISTINCT FROM c.{_qident(cc)})"
        ).fetchone()[0]
        if dangling:
            raise CorrectionError(
                f"relation {rel.name!r}: {dangling} row(s) in {child!r} no "
                f"longer match a {parent!r}.{pc}; an unedited child may have "
                f"lost its parent")
        # A child key must match AT MOST one parent row. Count distinct parent
        # rows per child key (not joined rows, which would also count several
        # children sharing one valid parent).
        ambiguous = con.execute(
            f"SELECT count(*) FROM (SELECT c.{_qident(cc)} "
            f"FROM {_qident(child)} c "
            f"JOIN {_qident(parent)} p ON p.{_qident(pc)} = c.{_qident(cc)} "
            f"GROUP BY c.{_qident(cc)} "
            f"HAVING count(DISTINCT (p.{_qident(pc)})) > 1)").fetchone()[0]
        if ambiguous:
            raise CorrectionError(
                f"relation {rel.name!r}: {ambiguous} foreign key value(s) "
                f"match more than one parent; parent keys must stay unique")


# ---------------------------------------------------------------------------
# publication
# ---------------------------------------------------------------------------

def _read_marker(db_path: Path):
    """Return the published correction marker of a database, or None."""
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except duckdb.Error:
        return None
    try:
        tables = {r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'").fetchall()}
        if MARKER_TABLE not in tables:
            return None
        rows = con.execute(
            "SELECT source_sha256, corrections_sha256, new_version "
            f"FROM {MARKER_TABLE}").fetchall()
    except duckdb.Error:
        return None
    finally:
        con.close()
    if len(rows) != 1:
        return None
    return {"source_sha256": rows[0][0],
            "corrections_sha256": rows[0][1],
            "new_version": rows[0][2]}


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _reap_stale_staging(parent: Path, target_name: str):
    """Remove staging directories whose owning process no longer exists.

    Staging names are ``.snapshot_correct_<target>.<pid>.<mkdtemp>``. Only
    directories for THIS target and a dead PID are removed, so a concurrent
    live run (unique mkdtemp suffix, live PID) is never disturbed. A directory
    whose name cannot be parsed is left strictly alone.
    """
    prefix = f".snapshot_correct_{target_name}."
    for entry in parent.iterdir():
        if not entry.name.startswith(prefix) or not entry.is_dir():
            continue
        rest = entry.name[len(prefix):].split(".", 1)
        if len(rest) != 2 or not rest[0].isdigit():
            continue
        pid = int(rest[0])
        if pid == os.getpid() or _pid_alive(pid):
            continue
        shutil.rmtree(entry, ignore_errors=True)


def _atomic_publish(staging_path: Path, target: Path, marker: dict) -> str:
    """Publish the complete staging database without ever clobbering target.

    Uses an exclusive hard link (an atomic create on the same filesystem)
    rather than "check absence then rename": with two racing processes only
    one link() can win. The loser inspects the winner's marker and either
    accepts an identical retry or reports a conflict.
    """
    try:
        os.link(staging_path, target)
        return "generated"
    except FileExistsError:
        existing = _read_marker(target)
        if existing is None:
            raise CorrectionError(
                f"target {target} is already occupied by a database that "
                f"carries no correction marker; refusing to overwrite a "
                f"published file")
        if existing == marker:
            return "exists-identical"
        raise CorrectionError(
            f"target {target} is already occupied by a different correction "
            f"result (version {existing['new_version']!r}); requests must "
            f"not share a target path")
    except OSError as e:
        raise CorrectionError(f"cannot publish to {target}: {e}") from e


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------

def correct_snapshot(source_path, corrections_path, expected_source_sha256,
                     new_version, target_path):
    """Build a corrected source database at ``target_path``.

    Parameters
    ----------
    source_path: closed, no-longer-written old snapshot DuckDB file.
    corrections_path: UTF-8 JSONL correction sheet.
    expected_source_sha256: required SHA-256 hex digest of the old snapshot.
    new_version: non-empty version string different from the old version.
    target_path: destination ``.duckdb`` path; never overwritten.

    Returns ``(target_path, status)`` where status is ``"generated"`` or
    ``"exists-identical"``. Raises DeliveryError subclasses on any failure;
    failures never create the target.
    """
    source_path = Path(source_path)
    corrections_path = Path(corrections_path)
    target_path = Path(target_path)

    if not source_path.is_file():
        raise CorrectionError(f"source snapshot not found: {source_path}")
    if not corrections_path.is_file():
        raise CorrectionError(f"correction sheet not found: {corrections_path}")
    if not isinstance(new_version, str) or not new_version.strip():
        raise CorrectionError("new version must be a non-empty string")
    if source_path.resolve() == target_path.resolve():
        raise CorrectionError(
            "target path must differ from the source path; the old snapshot "
            "is preserved read-only")

    actual_source_sha = _sha256_stream(source_path)
    if actual_source_sha != expected_source_sha256:
        raise SourceDriftError(
            f"source digest mismatch: expected {expected_source_sha256}, "
            f"actual {actual_source_sha}; refusing to correct an unexpected "
            f"snapshot")
    corrections_sha = _sha256_stream(corrections_path)
    marker = {"source_sha256": actual_source_sha,
              "corrections_sha256": corrections_sha,
              "new_version": new_version}

    # Idempotency / conflict check BEFORE doing any work. A prior completed
    # publish is recognised even if the caller never got the response.
    if target_path.exists():
        existing = _read_marker(target_path)
        if existing is None:
            raise CorrectionError(
                f"target {target_path} already exists but is not a "
                f"snapshot-correct result; refusing to overwrite a published "
                f"file")
        if existing == marker:
            return target_path, "exists-identical"
        raise CorrectionError(
            f"target {target_path} is already occupied by a different "
            f"correction result (version {existing['new_version']!r})")

    target_path.parent.mkdir(parents=True, exist_ok=True)

    # The old snapshot must load through the existing source reader, which
    # also validates row-key declarations, tags and relations.
    old_schema = SourceSchema.load(source_path)
    if old_schema.version == new_version:
        raise CorrectionError(
            f"new version {new_version!r} equals the old snapshot version; "
            f"a correction must publish a new, non-empty version")

    # Reap staging directories left behind by processes that died hard (they
    # hold a transactionally rolled-back, never-published database). Each
    # directory name carries its owning PID; live processes are never touched,
    # and every process gets a unique mkdtemp name regardless.
    _reap_stale_staging(target_path.parent, target_path.name)

    # Staging lives next to the target so the final hard link is a local,
    # atomic filesystem operation. Readers never see this directory.
    work_dir = Path(tempfile.mkdtemp(
        dir=str(target_path.parent),
        prefix=f".snapshot_correct_{target_path.name}.{os.getpid()}."))
    staging_db = work_dir / "staging.duckdb"
    try:
        # Private byte copy of the old snapshot; discarded on any failure.
        shutil.copyfile(source_path, staging_db)
        con = duckdb.connect(str(staging_db))
        try:
            specs = {t: {c: _build_type_spec(ts)
                         for c, ts in _table_columns(con, t)}
                     for t in CORRECTABLE_TABLES}
            _stream_sheet(con, corrections_path, specs, old_schema.row_keys)

            for table in CORRECTABLE_TABLES:
                _materialise_keys(con, table, old_schema.row_keys[table])
                _check_preconditions(con, table, old_schema.row_keys[table])

            # All data changes, the version bump and the provenance marker
            # commit in one transaction: an interruption rolls back to the
            # pristine copy, never leaving a half-corrected database.
            con.execute("BEGIN TRANSACTION")
            try:
                for table in sorted(CORRECTABLE_TABLES):
                    _apply_table(con, table, old_schema.row_keys[table], specs)
                _check_row_keys(con, old_schema)
                _check_relations(con, old_schema)

                n_versions = con.execute(
                    "SELECT count(*) FROM _snapshot").fetchone()[0]
                if n_versions != 1:
                    raise CorrectionError(
                        f"_snapshot must hold exactly one version row, found "
                        f"{n_versions}")
                con.execute("UPDATE _snapshot SET version = ?", [new_version])
                con.execute(f"DROP TABLE IF EXISTS {MARKER_TABLE}")
                con.execute(
                    f"CREATE TABLE {MARKER_TABLE} ("
                    "source_sha256 VARCHAR NOT NULL, "
                    "corrections_sha256 VARCHAR NOT NULL, "
                    "new_version VARCHAR NOT NULL)")
                con.execute(
                    f"INSERT INTO {MARKER_TABLE} VALUES (?, ?, ?)",
                    [marker["source_sha256"], marker["corrections_sha256"],
                     marker["new_version"]])
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise

            # Drop scratch tables and fold WAL state into the main file before
            # it is hard-linked into place.
            con.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE}")
            for table in CORRECTABLE_TABLES:
                con.execute(f"DROP TABLE IF EXISTS __keys_{table}")
            con.execute("CHECKPOINT")
        finally:
            con.close()

        # Re-open the finished database through the existing reader to prove
        # it is directly usable by compile/generate.
        new_schema = SourceSchema.load(staging_db)
        if new_schema.version != new_version:
            raise CorrectionError(
                "staging database did not record the new version")
        status = _atomic_publish(staging_db, target_path, marker)
        return target_path, status
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
