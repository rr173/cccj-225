"""Read interface: open a delivery package as its bound consumer."""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

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
