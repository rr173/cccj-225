"""Read interface: open a delivery package as its bound consumer.

Also provides offline row-lineage lookup: given only the package directory
(delivery Parquet, manifest, row_lineage.jsonl) a consumer can locate the
source table(s) and stable row key(s) of any output row, without the original
source database or any compile-time temporary file.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .common import canonical
from .errors import DeliveryError


def read_package(pkg_dir, consumer):
    """Return {dataset_name: {"columns": [...], "rows": [...]}} for the package.

    The package is bound to a consumer identity; reading as any other consumer
    is refused. Run the independent verifier first to establish integrity.
    """
    pkg_dir = Path(pkg_dir)
    manifest = json.loads((pkg_dir / "manifest.json").read_text(encoding="utf-8"))
    bound = manifest.get("consumer")
    if consumer != bound:
        raise DeliveryError(
            f"package {manifest.get('package_id')} is bound to consumer "
            f"'{bound}', not '{consumer}'")
    out = {}
    con = duckdb.connect()
    try:
        for ds in manifest.get("datasets", []):
            file = pkg_dir / ds["file"]
            cur = con.execute(f"SELECT * FROM read_parquet('{file}')")
            cols = [d[0] for d in cur.description]
            out[ds["name"]] = {"columns": cols, "rows": cur.fetchall()}
    finally:
        con.close()
    return out


def _iter_lineage(pkg_dir, ds_entry):
    """Yield lineage entries one at a time (streaming, stdlib only)."""
    rl = ds_entry.get("row_lineage")
    if not rl or not rl.get("file"):
        raise DeliveryError("package carries no row_lineage for this dataset")
    path = Path(pkg_dir) / rl["file"]
    if not path.is_file():
        raise DeliveryError(f"row lineage file missing: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def lookup_row_lineage(pkg_dir, dataset, *, row_ref=None, values=None):
    """Offline provenance lookup for one output row of a dataset.

    Identify the row either by its ``row_ref`` mapping identifier or by the
    exact published field ``values`` dict. When several output rows share the
    same projected values, all matching lineage entries are returned (each
    with its distinct row_ref and source set), so identical projections from
    different source rows remain separately locatable. The file is scanned in
    a single streaming pass; the source database is never opened.

    Returns a list of {"row_ref", "values", "sources"} entries. Raises
    DeliveryError if the row cannot be located. Works without the source DB.
    """
    pkg_dir = Path(pkg_dir)
    manifest = json.loads((pkg_dir / "manifest.json").read_text(encoding="utf-8"))
    ds_entry = next((d for d in manifest.get("datasets", [])
                     if d["name"] == dataset), None)
    if ds_entry is None:
        raise DeliveryError(f"dataset {dataset!r} not found in package manifest")
    if row_ref is None and values is None:
        raise DeliveryError("provide either row_ref= or values= to locate a row")
    target = canonical(values) if values is not None else None
    matches = []
    for entry in _iter_lineage(pkg_dir, ds_entry):
        if row_ref is not None:
            if entry["row_ref"] == row_ref:
                return [entry]
        elif canonical(entry["values"]) == target:
            matches.append(entry)
    if row_ref is not None:
        raise DeliveryError(f"no output row with row_ref {row_ref!r}")
    if not matches:
        raise DeliveryError(
            f"no output row in dataset {dataset!r} has published values {values}")
    return matches
