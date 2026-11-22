"""Package generator: executes a compiled plan against the pinned source version.

Execution is column-projected and streaming: each dataset query reads only the
columns in the compiled plan (denied fields are never scanned) and DuckDB's
COPY streams row groups to Parquet under a memory limit -- sensitive source
values are never materialised in Python.

Packages are immutable and content-addressed: package_id is a hash of
(source version+hashes, policy version, lineage version, projection version,
consumer). Regenerating with identical inputs is a no-op; any version change
produces a new package id.
"""
from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import duckdb

from .common import dump_json
from .compiler import Plan
from .errors import PackageExistsError, SourceDriftError
from .schema import SourceSchema


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
    try:
        con.execute("PRAGMA memory_limit='256MB'")
        con.execute("PRAGMA threads=4")
        stats = {}
        explains = {}
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
            stats[ds.name] = st
    finally:
        con.close()

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
        "datasets": [{
            "name": ds.name,
            "file": f"{ds.name}.parquet",
            "group_keys": ds.group_keys,
            "base_table": ds.base_table,
            "joins": ds.joins,
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
