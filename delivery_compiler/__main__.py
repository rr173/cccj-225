"""CLI: python -m delivery_compiler <compile|preview|generate|verify|read|lineage|history-load|history-show|snapshot-correct>"""
from __future__ import annotations

import argparse
import json
import sys

from .common import dump_json, load_json
from .compiler import Plan, compile_plan, format_decisions
from .errors import DeliveryError
from .generator import generate
from .history import connect as history_connect
from .history import load_package, show_history
from .reader import lookup_row_lineage, read_package
from .schema import SourceSchema
from .snapshot_correction import correct_snapshot
from .verifier import verify_package


def _add_inputs(p):
    p.add_argument("--source", required=True, help="source DuckDB file")
    p.add_argument("--policy", required=True, help="consumer policy JSON")
    p.add_argument("--lineage", required=True, help="field lineage JSON")
    p.add_argument("--projections", required=True, help="query projections JSON")


def main(argv=None):
    ap = argparse.ArgumentParser(prog="delivery-compiler")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("compile", help="validate and write a compiled plan")
    _add_inputs(p)
    p.add_argument("--plan-out", help="write the compiled plan JSON here")

    p = sub.add_parser("preview", help="print per-field decisions without generating")
    _add_inputs(p)

    p = sub.add_parser("generate", help="compile and generate an immutable package")
    _add_inputs(p)
    p.add_argument("--out", required=True, help="packages root directory")

    p = sub.add_parser("verify", help="independently verify a package")
    _add_inputs(p)
    p.add_argument("--package", required=True, help="package directory")

    p = sub.add_parser("read", help="read a package as its bound consumer")
    p.add_argument("--package", required=True)
    p.add_argument("--consumer", required=True)

    p = sub.add_parser("lineage",
                       help="offline lookup of one output row's source "
                            "tables and row keys (no source DB needed)")
    p.add_argument("--package", required=True)
    p.add_argument("--dataset", required=True)
    p.add_argument("--row-ref", help="locate by row_ref mapping identifier")
    p.add_argument("--values",
                   help="JSON object of exact published field values, e.g. "
                        "'{\"customer_id\": 1}'")

    p = sub.add_parser("history-load",
                       help="ingest one delivered package into the customer "
                            "history store")
    p.add_argument("--db", required=True, help="history DuckDB file")
    p.add_argument("--package", required=True, help="delivery package directory")
    p.add_argument("--date", required=True, help="business date YYYY-MM-DD")
    p.add_argument("--revision", required=True, type=int,
                   help="positive integer revision of --date")

    p = sub.add_parser("history-show",
                       help="replay one customer's roster states over a date "
                            "range")
    p.add_argument("--db", required=True, help="history DuckDB file")
    p.add_argument("--customer-id", required=True, type=int)
    p.add_argument("--start", required=True, help="range start YYYY-MM-DD "
                                                  "(inclusive)")
    p.add_argument("--end", required=True, help="range end YYYY-MM-DD "
                                                "(exclusive)")
    p.add_argument("--upto", type=int, default=None,
                   help="only use ingestions with seq <= upto "
                        "(default: latest; 0 = nothing ingested yet)")

    p = sub.add_parser(
        "snapshot-correct",
        help="build a corrected source snapshot from a JSONL correction sheet")
    p.add_argument("--source", required=True,
                   help="closed, no-longer-written old snapshot .duckdb file")
    p.add_argument("--source-sha256", required=True,
                   help="expected SHA-256 hex digest of --source")
    p.add_argument("--corrections", required=True,
                   help="UTF-8 JSONL correction sheet")
    p.add_argument("--new-version", required=True,
                   help="non-empty version for the new snapshot (must differ "
                        "from the old version)")
    p.add_argument("--out", required=True,
                   help="target .duckdb path; never overwritten")

    args = ap.parse_args(argv)
    try:
        if args.cmd in ("compile", "preview", "generate"):
            schema = SourceSchema.load(args.source)
            plan = compile_plan(schema, load_json(args.policy), load_json(args.lineage),
                                load_json(args.projections))
            if args.cmd == "compile":
                print(f"plan OK: consumer={plan.consumer} package_id={plan.package_id()}")
                if args.plan_out:
                    dump_json(plan.to_dict(), args.plan_out)
                    print(f"plan written to {args.plan_out}")
            elif args.cmd == "preview":
                print(format_decisions(plan))
            else:
                out, status = generate(plan, args.source, args.out)
                print(f"{status}: {out}")
        elif args.cmd == "verify":
            rep = verify_package(args.package, args.source, args.policy,
                                 args.lineage, args.projections)
            print(rep.format())
            return 0 if rep.ok else 1
        elif args.cmd == "read":
            data = read_package(args.package, args.consumer)
            for name, ds in data.items():
                print(f"== {name} ({len(ds['rows'])} rows)")
                print("   " + " | ".join(ds["columns"]))
                for row in ds["rows"][:20]:
                    print("   " + " | ".join(str(v) for v in row))
        elif args.cmd == "lineage":
            values = json.loads(args.values) if args.values else None
            hits = lookup_row_lineage(args.package, args.dataset,
                                      row_ref=args.row_ref, values=values)
            print(json.dumps(hits, ensure_ascii=False, indent=2))
        elif args.cmd == "history-load":
            con = history_connect(args.db)
            try:
                result = load_package(con, args.package, args.date,
                                      args.revision)
            finally:
                con.close()
            print(json.dumps(result, ensure_ascii=False, indent=2))
        elif args.cmd == "history-show":
            con = history_connect(args.db, read_only=True)
            try:
                result = show_history(con, args.customer_id, args.start,
                                      args.end, args.upto)
            finally:
                con.close()
            print(json.dumps(result, ensure_ascii=False, indent=2))
        elif args.cmd == "snapshot-correct":
            out, status = correct_snapshot(
                args.source, args.corrections, args.source_sha256,
                args.new_version, args.out)
            print(json.dumps(
                {"status": status, "out": str(out),
                 "new_version": args.new_version}, ensure_ascii=False))
        return 0
    except DeliveryError as e:
        print(f"error: {type(e).__name__}: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
