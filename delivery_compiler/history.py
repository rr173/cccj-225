"""Consumer-side customer history store ( DuckDB ).

The marketing team receives one delivery package per business day.  Packages
arrive late, out of order, and are sometimes corrected (a higher revision of
the same business date).  This module maintains a small history database that

* always reflects the latest corrected history, and
* can replay exactly what a report saw in the past, by interpreting every
  business date with the highest revision *ingested up to a given sequence
  number*.

It consumes already-delivered packages only: it never recompiles policy,
never touches the source lake, and never writes into a package.

Semantics
---------
* Each package holds the *complete roster* of its business date.  A customer
  missing from the package is absent from that date on -- this is neither
  "keep the previous value" nor "present with NULL attributes".
* A date's roster stays in effect until the next ingested business date;
  before the first roster a customer is not in the roster.
* ``history-load`` returns an increasing ingestion sequence number starting
  at 1.  Only a higher revision replaces a date's effective roster.  A
  byte-identical resend of an already-ingested (date, revision) returns the
  original sequence number and changes nothing.  Reusing a revision with
  different content, or submitting a never-ingested revision below the
  current one, is an error.  Errors consume no sequence number and leave no
  partial state: ingestion record, effective roster and replay history
  commit in one transaction.
* ``history-show`` reads per-customer state boundaries directly from the
  store (it never re-reads day rosters), clips them to ``[start, end)`` and
  merges adjacent identical states.

Locality
--------
Ingesting a package touches only: the new roster, the changed date's old
effective roster, and the adjacent previous/next effective dates' rosters
(for the row-level diff), plus point reads/updates of history boundaries for
the affected customers at the changed date and the next date.  No other
day's roster is scanned and no history span is rebuilt, so revising old
dates stays cheap as history grows.  See README ("客户历史库") for how to
verify this from the actual read/write paths with DuckDB's profiler.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime
from pathlib import Path

import duckdb

from .errors import HistoryError

SUPPORTED_FORMAT = "delivery-package/1"
SUPPORTED_CONSUMER = "marketing"
DATASET_NAME = "customers"

# The only columns ingested, with the exact physical types this store relies
# on.  Packages are validated upstream; these checks cover only what this
# feature needs (bindings, files, actual column types, primary key).
REQUIRED_COLUMNS = {
    "customer_id": "BIGINT",     # unique, non-NULL primary key
    "city": "VARCHAR",           # nullable
    "segment": "VARCHAR",        # nullable
    "region_label": "VARCHAR",   # nullable
}

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SAFE_FILE_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.\-/]*$")

SCHEMA_DDL = [
    """
    CREATE TABLE IF NOT EXISTS ingestions (
        seq            BIGINT  NOT NULL UNIQUE,   -- 入库序号, increasing from 1
        business_date  DATE    NOT NULL,
        revision       BIGINT  NOT NULL,
        package_id     VARCHAR,
        manifest_sha256 VARCHAR NOT NULL,
        parquet_sha256  VARCHAR NOT NULL,
        row_count      BIGINT  NOT NULL,
        ingested_at    TIMESTAMP NOT NULL DEFAULT current_timestamp,
        PRIMARY KEY (business_date, revision)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS day_states (
        business_date  DATE    NOT NULL PRIMARY KEY,
        revision       BIGINT  NOT NULL,          -- current effective revision
        seq            BIGINT  NOT NULL           -- ingestion that made it effective
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS rosters (
        business_date  DATE    NOT NULL,
        customer_id    BIGINT  NOT NULL,
        city           VARCHAR,
        segment        VARCHAR,
        region_label   VARCHAR,
        PRIMARY KEY (business_date, customer_id)
    )
    """,
    # Per-customer state boundaries, versioned by ingestion sequence number:
    # a row is visible to replays with cutoff s iff seq_from <= s < seq_to
    # (seq_to NULL = still live).  At most one live row per (customer, date).
    """
    CREATE TABLE IF NOT EXISTS history (
        customer_id    BIGINT  NOT NULL,
        valid_from     DATE    NOT NULL,
        present        BOOLEAN NOT NULL,
        city           VARCHAR,
        segment        VARCHAR,
        region_label   VARCHAR,
        seq_from       BIGINT  NOT NULL,
        seq_to         BIGINT,
        PRIMARY KEY (customer_id, valid_from, seq_from)
    )
    """,
]


# ---------------------------------------------------------------------------
# arguments / connections
# ---------------------------------------------------------------------------

def parse_business_date(text):
    """Strictly parse a YYYY-MM-DD business date."""
    if not isinstance(text, str) or not _DATE_RE.match(text):
        raise HistoryError(f"business date must be YYYY-MM-DD, got {text!r}")
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as e:
        raise HistoryError(f"invalid business date {text!r}: {e}") from e


def parse_revision(value):
    """A revision is a positive integer."""
    try:
        rev = int(value)
    except (TypeError, ValueError):
        raise HistoryError(f"revision must be a positive integer, got {value!r}")
    if rev < 1:
        raise HistoryError(f"revision must be a positive integer, got {value!r}")
    return rev


def connect(db_path, *, read_only=False):
    """Open the history store; create the schema unless read_only."""
    p = Path(db_path)
    try:
        if read_only:
            if not p.exists():
                raise HistoryError(f"history database not found: {p}")
            con = duckdb.connect(str(p), read_only=True)
            tables = {r[0] for r in con.execute(
                "SELECT table_name FROM information_schema.tables").fetchall()}
            missing = {"ingestions", "day_states", "rosters", "history"} - tables
            if missing:
                con.close()
                raise HistoryError(
                    f"{p} is not an initialised history database "
                    f"(missing tables: {', '.join(sorted(missing))})")
            return con
        p.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(p))
        for ddl in SCHEMA_DDL:
            con.execute(ddl)
        return con
    except duckdb.Error as e:
        raise HistoryError(f"cannot open history database {p}: {e}") from e


# ---------------------------------------------------------------------------
# package inspection (read-only; packages are never modified)
# ---------------------------------------------------------------------------

def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _locate_customers_parquet(pkg_dir):
    """Validate the package bindings this feature needs and locate the
    customers parquet via the manifest.  Other datasets are never read."""
    pkg_dir = Path(pkg_dir)
    if not pkg_dir.is_dir():
        raise HistoryError(f"package directory not found: {pkg_dir}")
    manifest_path = pkg_dir / "manifest.json"
    if not manifest_path.is_file():
        raise HistoryError(f"package {pkg_dir} has no manifest.json")
    manifest_bytes = manifest_path.read_bytes()
    try:
        manifest = json.loads(manifest_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise HistoryError(f"cannot parse manifest.json: {e}") from e

    fmt = manifest.get("format")
    if fmt != SUPPORTED_FORMAT:
        raise HistoryError(
            f"unsupported package format {fmt!r}; only {SUPPORTED_FORMAT!r} "
            f"is supported")
    consumer = manifest.get("consumer")
    if consumer != SUPPORTED_CONSUMER:
        raise HistoryError(
            f"package is bound to consumer {consumer!r}; only "
            f"{SUPPORTED_CONSUMER!r} packages are supported")

    entries = [d for d in manifest.get("datasets", [])
               if d.get("name") == DATASET_NAME]
    if not entries:
        raise HistoryError(
            f"package has no {DATASET_NAME!r} dataset in its manifest")
    if len(entries) > 1:
        raise HistoryError(
            f"manifest lists the {DATASET_NAME!r} dataset {len(entries)} times")
    file_name = entries[0].get("file")
    if not isinstance(file_name, str) or not _SAFE_FILE_RE.match(file_name) \
            or file_name.startswith("/") or ".." in file_name.split("/"):
        raise HistoryError(
            f"unsafe dataset file name in manifest: {file_name!r}")
    parquet_path = (pkg_dir / file_name).resolve()
    if pkg_dir.resolve() not in parquet_path.parents:
        raise HistoryError(f"dataset file escapes the package: {file_name!r}")
    if not parquet_path.is_file():
        raise HistoryError(f"dataset file not found: {parquet_path}")
    return manifest, manifest_bytes, parquet_path


def _check_columns(con, parquet_path):
    """Verify the actual parquet column types this store depends on."""
    path_sql = str(parquet_path).replace("'", "''")
    try:
        described = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path_sql}')").fetchall()
    except duckdb.Error as e:
        raise HistoryError(f"cannot read {parquet_path.name}: {e}") from e
    types = {name: ctype.upper() for name, ctype, *_ in described}
    for col, want in REQUIRED_COLUMNS.items():
        got = types.get(col)
        if got is None:
            raise HistoryError(
                f"customers parquet is missing required column {col!r}")
        if got != want:
            raise HistoryError(
                f"customers parquet column {col!r} has type {got}, "
                f"expected {want}")


# ---------------------------------------------------------------------------
# load
# ---------------------------------------------------------------------------

def load_package(con, pkg_dir, business_date, revision):
    """Ingest one delivery package into the history store.

    Returns {"seq": n, "status": "ingested"|"duplicate", ...}.  Raises
    HistoryError on any validation or revision conflict; a failed load
    consumes no sequence number and leaves no partial state.
    """
    business_date = _as_date(business_date)
    revision = parse_revision(revision)
    manifest, manifest_bytes, parquet_path = _locate_customers_parquet(pkg_dir)
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    parquet_sha = _sha256(parquet_path)

    con.execute("BEGIN TRANSACTION")
    try:
        row = con.execute(
            "SELECT seq, manifest_sha256, parquet_sha256 FROM ingestions "
            "WHERE business_date = ? AND revision = ?",
            [business_date, revision]).fetchone()
        if row is not None:
            seq, m_sha, p_sha = row
            if m_sha == manifest_sha and p_sha == parquet_sha:
                con.execute("COMMIT")
                return {"seq": seq, "status": "duplicate",
                        "business_date": business_date.isoformat(),
                        "revision": revision}
            raise HistoryError(
                f"revision {revision} of {business_date} was already ingested "
                f"with different content (seq {seq}); reusing a revision with "
                f"new content is not allowed")

        current = con.execute(
            "SELECT revision FROM day_states WHERE business_date = ?",
            [business_date]).fetchone()
        if current is not None and revision <= current[0]:
            raise HistoryError(
                f"revision {revision} of {business_date} was never ingested "
                f"and is not higher than the current revision {current[0]}; "
                f"only a higher revision can replace the effective roster")

        _check_columns(con, parquet_path)
        path_sql = str(parquet_path).replace("'", "''")
        con.execute("DROP TABLE IF EXISTS new_roster")
        con.execute(
            "CREATE TEMP TABLE new_roster AS SELECT customer_id, city, "
            f"segment, region_label FROM read_parquet('{path_sql}')")
        n, nn, nd = con.execute(
            "SELECT count(*), count(customer_id), count(DISTINCT customer_id) "
            "FROM new_roster").fetchone()
        if nn < n:
            raise HistoryError(
                f"customers parquet has {n - nn} NULL customer_id values; "
                f"customer_id is the primary key and must be non-NULL")
        if nd < n:
            raise HistoryError(
                f"customers parquet has duplicate customer_id values "
                f"({n - nd} extra rows); customer_id must be unique")

        seq = con.execute(
            "SELECT coalesce(max(seq), 0) + 1 FROM ingestions").fetchone()[0]

        prev_date = con.execute(
            "SELECT max(business_date) FROM day_states WHERE business_date < ?",
            [business_date]).fetchone()[0]
        next_date = con.execute(
            "SELECT min(business_date) FROM day_states WHERE business_date > ?",
            [business_date]).fetchone()[0]

        replacing = current is not None
        _maintain_history(con, business_date, prev_date, next_date,
                          replacing, seq)

        con.execute("DELETE FROM rosters WHERE business_date = ?",
                    [business_date])
        con.execute(
            "INSERT INTO rosters SELECT ?, customer_id, city, segment, "
            "region_label FROM new_roster", [business_date])
        con.execute("DELETE FROM day_states WHERE business_date = ?",
                    [business_date])
        con.execute(
            "INSERT INTO day_states VALUES (?, ?, ?)",
            [business_date, revision, seq])
        con.execute(
            "INSERT INTO ingestions (seq, business_date, revision, package_id,"
            " manifest_sha256, parquet_sha256, row_count) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [seq, business_date, revision, manifest.get("package_id"),
             manifest_sha, parquet_sha, n])
        con.execute("DROP TABLE IF EXISTS new_roster")
        con.execute("DROP TABLE IF EXISTS affected")
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except duckdb.Error:
            pass  # already rolled back
        raise
    return {"seq": seq, "status": "ingested",
            "business_date": business_date.isoformat(),
            "revision": revision, "rows": n}


def _roster_rel(date_or_none):
    """SQL relation for one date's effective roster (empty when None)."""
    if date_or_none is None:
        return ("(SELECT NULL::BIGINT AS customer_id, NULL::VARCHAR AS city, "
                "NULL::VARCHAR AS segment, NULL::VARCHAR AS region_label "
                "WHERE FALSE)")
    return ("(SELECT customer_id, city, segment, region_label FROM rosters "
            f"WHERE business_date = DATE '{date_or_none.isoformat()}')")


def _maintain_history(con, business_date, prev_date, next_date, replacing, seq):
    """Update per-customer history boundaries for one ingested roster.

    Row-level diff of the new roster against exactly one stored roster (the
    changed date's old roster when replacing, otherwise the previous
    effective date's roster).  For each affected customer only the boundary
    at the changed date and at the next effective date is reconsidered;
    neighbouring dates' rosters are point-read by customer_id.  Nothing else
    is scanned or rebuilt.
    """
    diff_rel = _roster_rel(business_date if replacing else prev_date)
    prev_rel = _roster_rel(prev_date)
    next_rel = _roster_rel(next_date)
    prev_cols = ("(p.customer_id IS NOT NULL) AS prev_present, "
                 "p.city AS prev_city, p.segment AS prev_segment, "
                 "p.region_label AS prev_region_label") if prev_date else (
        "FALSE AS prev_present, NULL::VARCHAR AS prev_city, "
        "NULL::VARCHAR AS prev_segment, NULL::VARCHAR AS prev_region_label")
    next_cols = ("(q.customer_id IS NOT NULL) AS next_present, "
                 "q.city AS next_city, q.segment AS next_segment, "
                 "q.region_label AS next_region_label") if next_date else (
        "FALSE AS next_present, NULL::VARCHAR AS next_city, "
        "NULL::VARCHAR AS next_segment, NULL::VARCHAR AS next_region_label")
    prev_join = (f"LEFT JOIN {prev_rel} p ON p.customer_id = d.customer_id"
                 if prev_date else "")
    next_join = (f"LEFT JOIN {next_rel} q ON q.customer_id = d.customer_id"
                 if next_date else "")

    con.execute("DROP TABLE IF EXISTS affected")
    con.execute(f"""
        CREATE TEMP TABLE affected AS
        WITH diff AS (
            SELECT coalesce(n.customer_id, o.customer_id) AS customer_id,
                   (n.customer_id IS NOT NULL) AS new_present,
                   n.city AS new_city, n.segment AS new_segment,
                   n.region_label AS new_region_label
            FROM new_roster n
            FULL OUTER JOIN {diff_rel} o ON n.customer_id = o.customer_id
            WHERE (n.customer_id IS NULL) <> (o.customer_id IS NULL)
               OR n.city IS DISTINCT FROM o.city
               OR n.segment IS DISTINCT FROM o.segment
               OR n.region_label IS DISTINCT FROM o.region_label
        )
        SELECT d.*, {prev_cols}, {next_cols}
        FROM diff d {prev_join} {next_join}
    """)

    day = business_date.isoformat()
    # Boundary at the changed date: desired state = the new roster state,
    # needed iff it differs from the state governed by the previous date.
    _retire_boundaries(con, day, "prev", seq)
    _insert_boundaries(con, day, "prev", seq)
    if next_date is not None:
        # Boundary at the next date: still needed iff the next date's own
        # roster state differs from the (new) state before it.
        nxt = next_date.isoformat()
        _retire_boundaries(con, nxt, "next", seq)
        _insert_boundaries(con, nxt, "next", seq)


def _retire_boundaries(con, day, anchor, seq):
    """Set seq_to on live boundaries at `day` that must no longer exist.

    anchor="prev": boundary carries the new roster state; drop it when the
    new state equals the previous date's state, or when its content differs
    from the desired state.  anchor="next": same against the next date's
    own roster state.
    """
    same_as_anchor = _state_eq("new", anchor)
    content_differs = _state_neq("h", "new" if anchor == "prev" else "next",
                                 hist_alias="h")
    con.execute(f"""
        UPDATE history AS h SET seq_to = {seq}
        FROM affected AS a
        WHERE h.customer_id = a.customer_id
          AND h.valid_from = DATE '{day}'
          AND h.seq_to IS NULL
          AND ({same_as_anchor} OR {content_differs})
    """)


def _insert_boundaries(con, day, anchor, seq):
    """Insert boundaries at `day` that are missing for affected customers."""
    differs_from_anchor = _state_neq("new", anchor)
    carry = "new" if anchor == "prev" else "next"
    con.execute(f"""
        INSERT INTO history (customer_id, valid_from, present, city, segment,
                             region_label, seq_from)
        SELECT a.customer_id, DATE '{day}', a.{carry}_present, a.{carry}_city,
               a.{carry}_segment, a.{carry}_region_label, {seq}
        FROM affected a
        WHERE {differs_from_anchor}
          AND NOT EXISTS (
              SELECT 1 FROM history h
              WHERE h.customer_id = a.customer_id
                AND h.valid_from = DATE '{day}'
                AND h.seq_to IS NULL
                AND {_state_eq('h', carry, hist_alias='h')})
    """)


def _state_eq(left, right, hist_alias=None):
    """SQL: full state (present + 3 attributes) of `left` equals `right`.

    A side is either an `affected` column prefix ("new"/"prev"/"next") or a
    history-row alias ("h").
    """
    def col(side, name):
        if side == "h":
            return f"{hist_alias}.{name}"
        return f"a.{side}_{name}"
    return (f"({col(left, 'present')} = {col(right, 'present')}"
            f" AND {col(left, 'city')} IS NOT DISTINCT FROM {col(right, 'city')}"
            f" AND {col(left, 'segment')} IS NOT DISTINCT FROM {col(right, 'segment')}"
            f" AND {col(left, 'region_label')} IS NOT DISTINCT FROM {col(right, 'region_label')})")


def _state_neq(left, right, hist_alias=None):
    def col(side, name):
        if side == "h":
            return f"{hist_alias}.{name}"
        return f"a.{side}_{name}"
    return (f"({col(left, 'present')} <> {col(right, 'present')}"
            f" OR {col(left, 'city')} IS DISTINCT FROM {col(right, 'city')}"
            f" OR {col(left, 'segment')} IS DISTINCT FROM {col(right, 'segment')}"
            f" OR {col(left, 'region_label')} IS DISTINCT FROM {col(right, 'region_label')})")


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------

def show_history(con, customer_id, start, end, upto=None):
    """Replay one customer's roster states over [start, end).

    Uses only ingestions with seq <= upto (default: latest; 0 = nothing
    ingested yet).  Returns JSON-ready intervals sorted by start, clipped to
    the requested range, with adjacent identical states merged.
    """
    try:
        customer_id = int(customer_id)
    except (TypeError, ValueError):
        raise HistoryError(f"customer_id must be an integer, "
                           f"got {customer_id!r}")
    start = _as_date(start)
    end = _as_date(end)
    if end < start:
        raise HistoryError(f"empty range: end {end} is before start {start}")
    if upto is None:
        upto = con.execute(
            "SELECT coalesce(max(seq), 0) FROM ingestions").fetchone()[0]
    upto = int(upto)
    if upto < 0:
        raise HistoryError(f"upto must be >= 0, got {upto}")

    rows = con.execute(
        "SELECT valid_from, present, city, segment, region_label "
        "FROM history "
        "WHERE customer_id = ? AND seq_from <= ? "
        "  AND (seq_to IS NULL OR seq_to > ?) "
        "ORDER BY valid_from",
        [customer_id, upto, upto]).fetchall()

    intervals = _intervals(rows, start, end)
    return {"customer_id": customer_id,
            "start": start.isoformat(), "end": end.isoformat(),
            "upto": upto, "intervals": intervals}


def _intervals(boundaries, start, end):
    """Clip boundary states to [start, end) and merge adjacent equals."""
    absent = (False, None, None, None)
    # raw segments: (seg_start, seg_end, state); None = open ended
    raw = []
    seg_start, state = None, absent
    for valid_from, present, city, segment, region_label in boundaries:
        raw.append((seg_start, valid_from, state))
        seg_start = valid_from
        state = (present, city, segment, region_label)
    raw.append((seg_start, None, state))

    clipped = []
    for s, e, st in raw:
        lo = s if s is not None and s > start else start
        hi = e if e is not None and e < end else end
        if lo < hi:
            if clipped and clipped[-1][2] == st and clipped[-1][1] == lo:
                clipped[-1] = (clipped[-1][0], hi, st)
            else:
                clipped.append((lo, hi, st))

    out = []
    for s, e, (present, city, segment, region_label) in clipped:
        iv = {"start": s.isoformat(), "end": e.isoformat(),
              "present": bool(present)}
        if present:
            iv.update({"city": city, "segment": segment,
                       "region_label": region_label})
        out.append(iv)
    return out


def _as_date(value):
    if isinstance(value, date):
        return value
    return parse_business_date(value)
