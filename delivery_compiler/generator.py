"""Package generator: executes a compiled plan against the pinned source version.

Execution is column-projected and streaming: each dataset query reads only the
columns in the compiled plan (denied fields are never scanned) and DuckDB's
COPY streams row groups to Parquet under a memory limit -- sensitive source
values are never materialised in Python.

Every dataset additionally gets a ``row_lineage.jsonl``: per-output-row
provenance keyed by the source's declared stable business row keys, produced
through the same join/group pipeline (never by reverse-looking-up output
values) and streamed to disk in bounded batches.

Packages are immutable and content-addressed: package_id is a hash of
(source version+hashes, policy version, lineage version, projection version,
consumer). Regenerating with identical inputs is a no-op; any version change
produces a new package id. A package becomes visible only by an atomic rename
after the manifest, every data file and every lineage file are written and
cross-checked; a failure never overwrites an existing immutable package.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path

import duckdb

from .common import canonical, dump_json
from .compiler import Plan
from .errors import PackageExistsError, SourceDriftError
from .row_lineage import (LINEAGE_FORMAT, LINEAGE_SUFFIX, LineageColumn,
                          LineageJoin, LineageStage, make_row_ref,
                          read_entries, write_lineage)
from .schema import SourceSchema


def _sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _stage_for(ds, schema):
    rel_by_name = {r.name: r for r in schema.relations}
    joins = tuple(
        LineageJoin(from_table=rel_by_name[name].from_table,
                    from_column=rel_by_name[name].from_column,
                    to_table=rel_by_name[name].to_table,
                    to_column=rel_by_name[name].to_column)
        for name in ds.joins)
    columns = tuple(LineageColumn(name=c.name, sql=c.sql) for c in ds.columns)
    gk_sqls = tuple(next(c.sql for c in ds.columns if c.lineage == [gk])
                    for gk in ds.group_keys)
    return LineageStage(
        name=ds.name, base_table=ds.base_table, joins=joins, columns=columns,
        group_key_sqls=gk_sqls, table_chain=tuple(ds.table_chain))


def _cross_check_package(con, tmp, plan, schema, lineage_files):
    """Internal consistency check before the package is published.

    This is only a self-check (the independent verifier recomputes everything
    from source and never trusts it). It runs in bounded batches: lineage
    entries are streamed in and inserted into a DuckDB staging table, so the
    Python process never holds the whole dataset.
    """
    for ds in plan.datasets:
        parquet = tmp / f"{ds.name}.parquet"
        lin_path = tmp / lineage_files[ds.name]["file"]
        col_names = [c.name for c in ds.columns]
        con.execute("DROP TABLE IF EXISTS lin_vals")
        con.execute(
            f"CREATE TEMP TABLE lin_vals AS SELECT * FROM read_parquet('{parquet}') "
            "LIMIT 0")
        n = 0
        for batch in _batched_entries(lin_path, 1024):
            con.executemany(
                f"INSERT INTO lin_vals VALUES ({', '.join('?' for _ in col_names)})",
                [[e["values"].get(c) for c in col_names] for e in batch])
            n += len(batch)
        n_parquet = con.execute(
            f"SELECT count(*) FROM read_parquet('{parquet}')").fetchone()[0]
        if n != n_parquet:
            raise RuntimeError(
                f"row-lineage self-check failed for dataset {ds.name!r}: "
                f"{n} lineage entries vs {n_parquet} parquet rows")
        col_list = ", ".join(f'"{c}"' for c in col_names)
        d1 = con.execute(
            f"SELECT count(*) FROM ((SELECT {col_list} FROM lin_vals) EXCEPT ALL "
            f"(SELECT {col_list} FROM read_parquet('{parquet}')))").fetchone()[0]
        d2 = con.execute(
            f"SELECT count(*) FROM ((SELECT {col_list} FROM read_parquet('{parquet}')) "
            f"EXCEPT ALL (SELECT {col_list} FROM lin_vals))").fetchone()[0]
        if d1 or d2:
            raise RuntimeError(
                f"row-lineage self-check failed for dataset {ds.name!r}: lineage "
                f"published values do not match the parquet rows")
        # Deterministic order follows the lineage SQL ORDER BY. Row-level
        # datasets follow the base table's unique row-key link; grouped
        # datasets follow their (sorted) group-key output values. The
        # comparison uses typed values, never JSON text ("10" < "9" and
        # "R10" < "R5" as strings).
        base_table = ds.base_table
        if ds.group_keys:
            name_by_lineage = {}
            for c in ds.columns:
                for ln in c.lineage:
                    name_by_lineage[ln] = c.name
            gk_output_names = [name_by_lineage[gk] for gk in ds.group_keys]
        prev_key = None
        for entry in read_entries(lin_path):
            values, sources = entry.get("values"), entry.get("sources")
            if set(entry) != {"row_ref", "values", "sources"} or not sources:
                raise RuntimeError(
                    f"row-lineage self-check failed for dataset {ds.name!r}: "
                    f"malformed entry or empty source set")
            if entry.get("row_ref") != make_row_ref(values, sources):
                raise RuntimeError(
                    f"row-lineage self-check failed for dataset {ds.name!r}: "
                    f"row_ref mapping identifier mismatch")
            sig = [(s["table"], canonical(s["key"])) for s in sources]
            if sig != sorted(sig) or len(set(sig)) != len(sig):
                raise RuntimeError(
                    f"row-lineage self-check failed for dataset {ds.name!r}: "
                    f"sources are not sorted and de-duplicated")
            if ds.group_keys:
                order_key = tuple((g, values.get(g)) for g in gk_output_names)
            else:
                base_ref = next((s for s in sources if s["table"] == base_table),
                                None)
                order_key = tuple(sorted(base_ref["key"].items())) \
                    if base_ref else None
            if prev_key is not None and order_key < prev_key:
                raise RuntimeError(
                    f"row-lineage self-check failed for dataset {ds.name!r}: "
                    f"entry order is not deterministic")
            prev_key = order_key
        con.execute("DROP TABLE lin_vals")


def _batched_entries(path, size):
    batch = []
    for entry in read_entries(path, batch_size=size):
        batch.append(entry)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def generate(plan: Plan, db_path, out_root):
    # runtime pin: refuse to read a source that drifted since compile time
    now = SourceSchema.load(db_path)
    if now.version != plan.source_version or now.table_hashes != plan.table_hashes:
        raise SourceDriftError(
            f"source drifted since compile (compiled against "
            f"{plan.source_version}/{plan.table_hashes}, now "
            f"{now.version}/{now.table_hashes}); recompile the plan")

    pid = plan.package_id()
    out = Path(out_root) / pid
    if out.exists():
        existing = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
        if existing.get("package_id") != pid or existing.get("identity") != plan.identity():
            raise PackageExistsError(
                f"package directory {out} exists with different content; "
                f"packages are immutable")
        return out, "exists-identical"

    tmp = out.parent / (pid + ".building")
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)

    con = duckdb.connect(str(db_path), read_only=True)
    # DuckDB spill directory lives OUTSIDE the package staging dir so scratch
    # files can never be published inside an immutable package.
    spill = tempfile.mkdtemp(prefix=pid + "_spill_")
    try:
        con.execute("PRAGMA memory_limit='256MB'")
        con.execute("PRAGMA threads=4")
        con.execute("PRAGMA temp_directory='" + spill + "'")
        stats = {}
        explains = {}
        lineage_files = {}
        for ds in plan.datasets:
            explains[ds.name] = "\n".join(
                str(r) for r in con.execute("EXPLAIN " + ds.sql).fetchall())
            target = tmp / f"{ds.name}.parquet"
            con.execute(f"COPY ({ds.sql}) TO '{target}' (FORMAT PARQUET)")
            rows = con.execute(
                f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]
            st = {"rows": int(rows)}
            if ds.group_keys:
                st["groups"] = int(rows)

            # per-row provenance, streamed through the same join/group pipeline
            stage = _stage_for(ds, now)
            lin_name = f"{ds.name}{LINEAGE_SUFFIX}"
            lin_stats = write_lineage(con, stage, now.row_keys, tmp / lin_name)
            st["lineage_entries"] = lin_stats["entries"]
            st["lineage_source_refs"] = lin_stats["source_refs"]
            stats[ds.name] = st
            lineage_files[ds.name] = {"file": lin_name, "format": LINEAGE_FORMAT}
        _cross_check_package(con, tmp, plan, now, lineage_files)
    finally:
        con.close()
        shutil.rmtree(spill, ignore_errors=True)

    manifest = {
        "format": "delivery-package/1",
        "package_id": pid,
        "identity": plan.identity(),
        "consumer": plan.consumer,
        "purpose": plan.purpose,
        "source": {"version": plan.source_version, "tables": plan.table_hashes},
        "policy_version": plan.policy_version,
        "lineage_version": plan.lineage_version,
        "projection_version": plan.projection_version,
        "row_key_declarations": {
            t: {"columns": cols} for t, cols in sorted(now.row_keys.items())},
        "datasets": [{
            "name": ds.name,
            "file": f"{ds.name}.parquet",
            "row_lineage": {
                "file": lineage_files[ds.name]["file"],
                "format": LINEAGE_FORMAT,
                "sha256": _sha256_file(tmp / lineage_files[ds.name]["file"]),
            },
            "group_keys": ds.group_keys,
            "base_table": ds.base_table,
            "joins": ds.joins,
            "table_chain": ds.table_chain,
            "columns": [{
                "name": c.name, "action": c.action, "transform": c.sql,
                "lineage": c.lineage, "matched_rules": c.matched_rules,
                "reason": c.reason,
            } for c in ds.columns],
        } for ds in plan.datasets],
        "forbidden_paths": plan.forbidden_paths,
        "excluded_outputs": plan.excluded_outputs,
        "scanned_columns": plan.scanned_columns,
        "stats": {"datasets": stats},
        # Self-attestation is evidence ONLY. The independent verifier never
        # reads this block; it recomputes everything from source + policy.
        "compiler_attestation": {
            "self_check": "passed",
            "note": "compiler self-attestation; verifiers MUST NOT trust this",
        },
    }
    # bind every data + lineage file's digest into the manifest, then hash the
    # manifest itself so tampering with any file breaks verification
    for ds_entry in manifest["datasets"]:
        ds_entry["sha256"] = _sha256_file(tmp / ds_entry["file"])
    dump_json(manifest, tmp / "manifest.json")
    dump_json(plan.to_dict(), tmp / "plan.json")
    with open(tmp / "decisions.jsonl", "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"type": "package", "package_id": pid}) + "\n")
        for d in plan.decisions:
            fh.write(json.dumps({"type": "decision", **d}, ensure_ascii=False) + "\n")
    with open(tmp / "plan_explain.txt", "w", encoding="utf-8") as fh:
        for name, text in explains.items():
            fh.write(f"=== dataset {name} ===\n{text}\n\n")
    os.rename(tmp, out)
    return out, "generated"
