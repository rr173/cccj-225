"""CLI: python -m delivery_compiler <compile|preview|generate|verify|read>"""
from __future__ import annotations

import argparse
import sys

from .common import dump_json, load_json
from .compiler import Plan, compile_plan, format_decisions
from .errors import DeliveryError
from .generator import generate
from .reader import read_package
from .schema import SourceSchema
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
        return 0
    except DeliveryError as e:
        print(f"error: {type(e).__name__}: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
