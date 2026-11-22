"""End-to-end demo of the declarative delivery compiler.

Covers: parent-path policies, explicit exceptions, derived-field inheritance,
allowed aggregates, deny conflicts, missing lineage, duplicate output names,
repeat generation (immutability) and source-version changes -- plus tampering
attacks that the independent verifier must catch.
"""
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import duckdb  # noqa: E402

from delivery_compiler import (SourceSchema, compile_plan, format_decisions,
                               generate, read_package, verify_package)
from delivery_compiler.common import load_json  # noqa: E402
from delivery_compiler.errors import DeliveryError  # noqa: E402
from demo.make_source import build  # noqa: E402

DEMO = Path(__file__).resolve().parent
CFG = DEMO / "configs"
OUT = DEMO / "out"
SRC_V1 = DEMO / "source_v1.duckdb"
SRC_V2 = DEMO / "source_v2.duckdb"
PKGS = OUT / "packages"

RESULTS = []


def hr(title):
    print("\n" + "=" * 78 + f"\n== {title}\n" + "=" * 78)


def record(name, ok, detail=""):
    RESULTS.append((name, bool(ok), detail))
    print(f"  --> [{'OK' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))


def expect_error(label, exc_types, fn):
    try:
        fn()
    except exc_types as e:
        print(f"  rejected as expected: {type(e).__name__}: {e}")
        record(label, True)
        return
    except Exception as e:  # wrong error type
        print(f"  UNEXPECTED error type: {type(e).__name__}: {e}")
        record(label, False, "wrong exception type")
        return
    record(label, False, "was NOT rejected")


def main():
    if OUT.exists():
        shutil.rmtree(OUT)
    PKGS.mkdir(parents=True)

    hr("0. Build pinned source versions v1 and v2")
    build(SRC_V1, "v1")
    build(SRC_V2, "v2")

    schema = SourceSchema.load(SRC_V1)
    print(f"source version: {schema.version}")
    print(f"tables: { {t: h['rows'] for t, h in schema.table_hashes.items()} }")
    print("field paths and tags:")
    for t, fields in schema.tables.items():
        for p, f in sorted(fields.items()):
            tag = f"  tags={sorted(f.tags)}" if f.tags else ""
            kind = "leaf" if f.leaf else "struct"
            print(f"  {p:45s} [{kind}]{tag}")
    print("relations: " + ", ".join(
        f"{r.name}({r.from_table}.{r.from_column}->{r.to_table}.{r.to_column})"
        for r in schema.relations))

    pol_m = load_json(CFG / "policy_marketing.json")
    pol_f = load_json(CFG / "policy_fraud.json")
    pol_a = load_json(CFG / "policy_auditor.json")
    lin = load_json(CFG / "lineage.json")
    proj_m = load_json(CFG / "proj_marketing.json")
    proj_f = load_json(CFG / "proj_fraud.json")
    proj_a = load_json(CFG / "proj_auditor.json")

    hr("1. Compile + preview: marketing (parent paths, exceptions, inheritance, aggregates)")
    con_v1 = duckdb.connect(str(SRC_V1), read_only=True)
    plan_m = compile_plan(schema, pol_m, lin, proj_m, con=con_v1)
    print(format_decisions(plan_m))
    print(f"\nforbidden (deny) source paths: {plan_m.forbidden_paths}")
    print(f"excluded outputs:              {plan_m.excluded_outputs}")
    print(f"scanned source columns:        {plan_m.scanned_columns}")
    record("marketing plan compiles", True)

    hr("2. Preview: auditor -- a deny is lifted ONLY by a higher-priority explicit path rule")
    plan_a = compile_plan(schema, pol_a, lin, proj_a, con=con_v1)
    print(format_decisions(plan_a))
    full = next(d for d in plan_a.decisions if d["output"] == "full_name")
    mail = next(d for d in plan_a.decisions if d["output"] == "email")
    record("explicit path rule lifts deny (full_name=allow)",
           full["status"] == "included" and full["action"] == "allow")
    record("tag-level allow cannot lift deny (email stays denied)",
           mail["status"] == "excluded" and mail["action"] == "deny")

    hr("3. Preview: fraud_analyst (tag mask default, explicit allow exception, row-level amount)")
    plan_f = compile_plan(schema, pol_f, lin, proj_f, con=con_v1)
    print(format_decisions(plan_f))
    con_v1.close()

    hr("4. Compile-time rejections")
    from delivery_compiler import errors as E

    expect_error("deny conflict (equal priority+specificity) rejected",
                 (E.PolicyConflictError,),
                 lambda: compile_plan(schema, load_json(CFG / "policy_conflict.json"),
                                      lin, proj_m))

    def bad_proj(field, ds_name="customers", gks=None, extra=None):
        fields = [field] if extra is None else [field, extra]
        return {"consumer": "marketing", "purpose": "analytics",
                "datasets": [{"name": ds_name, "group_keys": gks or [],
                              "fields": fields}]}

    expect_error("unresolvable lineage (mystery_score) rejected",
                 (E.UnresolvableLineageError,),
                 lambda: compile_plan(schema, pol_m, lin,
                                      bad_proj({"name": "mystery", "derived": "mystery_score"})))
    expect_error("declared inputs != expression refs (shadow_ratio) rejected",
                 (E.UnresolvableLineageError,),
                 lambda: compile_plan(schema, pol_m, lin,
                                      bad_proj({"name": "shadow", "derived": "shadow_ratio"})))
    expect_error("cyclic lineage rejected",
                 (E.CyclicLineageError,),
                 lambda: compile_plan(schema, pol_m, load_json(CFG / "lineage_cycle.json"),
                                      bad_proj({"name": "cyc", "derived": "cyc_a"})))
    expect_error("duplicate output names rejected",
                 (E.OutputNameConflictError,),
                 lambda: compile_plan(schema, pol_m, lin,
                                      bad_proj({"name": "email", "source": "customer_profile.contact.email"},
                                               extra={"name": "email", "source": "customer_profile.segment"})))
    expect_error("dangling reference rejected",
                 (E.DanglingReferenceError,),
                 lambda: compile_plan(schema, pol_m, lin,
                                      bad_proj({"name": "nick", "source": "customer_profile.nickname"})))
    expect_error("plain projection of aggregate-only field rejected",
                 (E.InvalidProjectionError,),
                 lambda: compile_plan(schema, pol_m, lin,
                                      bad_proj({"name": "total_amount", "source": "transactions.amount"})))
    expect_error("derived field inheriting 'aggregate' rejected at row level",
                 (E.InvalidProjectionError,),
                 lambda: compile_plan(schema, pol_m, lin,
                                      bad_proj({"name": "bucket", "derived": "amount_bucket"})))
    expect_error("stat not allowed by the aggregate rule rejected",
                 (E.InvalidProjectionError,),
                 lambda: compile_plan(schema, pol_m, lin,
                                      bad_proj({"name": "s", "kind": "aggregate", "stat": "min",
                                                "of": "transactions.amount"},
                                               ds_name="agg", gks=["customer_profile.region_code"],
                                               extra={"name": "region_code",
                                                      "source": "customer_profile.region_code"})))
    expect_error("plain projection inside grouped dataset rejected",
                 (E.InvalidProjectionError,),
                 lambda: compile_plan(schema, pol_m, lin,
                                      bad_proj({"name": "segment", "source": "customer_profile.segment"},
                                               ds_name="agg", gks=["customer_profile.region_code"],
                                               extra={"name": "region_code",
                                                      "source": "customer_profile.region_code"})))
    expect_error("sensitive field without policy coverage rejected",
                 (E.UncoveredSensitiveError,),
                 lambda: compile_plan(schema, load_json(CFG / "policy_growth.json"), lin,
                                      load_json(CFG / "proj_growth.json")))

    hr("5. Generate packages (v1); repeat generation must be a no-op")
    pkg_m, st1 = generate(plan_m, SRC_V1, PKGS)
    print(f"marketing package: {pkg_m.name}  [{st1}]")
    pkg_m2, st2 = generate(plan_m, SRC_V1, PKGS)
    print(f"regenerated:       {pkg_m2.name}  [{st2}]")
    record("repeat generation keeps package identity",
           pkg_m == pkg_m2 and st2 == "exists-identical")
    pkg_f, _ = generate(plan_f, SRC_V1, PKGS)
    print(f"fraud package:     {pkg_f.name}")
    print("package contents:", sorted(p.name for p in pkg_m.iterdir()))

    hr("6. Independent verification of both packages")
    rep_m = verify_package(pkg_m, SRC_V1, CFG / "policy_marketing.json",
                           CFG / "lineage.json", CFG / "proj_marketing.json")
    print(rep_m.format())
    record("marketing package verified", rep_m.ok)
    rep_f = verify_package(pkg_f, SRC_V1, CFG / "policy_fraud.json",
                           CFG / "lineage.json", CFG / "proj_fraud.json")
    print(rep_f.format())
    record("fraud package verified", rep_f.ok)

    hr("7. Manual listing: marketing's final visible columns and why")
    for ds in plan_m.datasets:
        print(f"  dataset '{ds.name}' (base={ds.base_table}, joins={ds.joins})")
        for c in ds.columns:
            print(f"    {c.name:15s} action={c.action:10s} transform={c.sql}")
    print("\n  (each decision reason was printed in section 1 and is stored in "
          "decisions.jsonl)")

    hr("8. Tampering attacks vs the independent verifier")

    # T1: flip an action in the manifest (mask -> allow). The directory keeps
    # the original package id so ONLY the content tampering is tested.
    t1 = OUT / "t1" / pkg_m.name
    t1.parent.mkdir(parents=True)
    shutil.copytree(pkg_m, t1)
    m = json.loads((t1 / "manifest.json").read_text())
    m["datasets"][0]["columns"][1]["action"] = "allow"
    (t1 / "manifest.json").write_text(json.dumps(m, indent=2, sort_keys=True))
    print(f"  note: tampered manifest still carries compiler_attestation."
          f"self_check={m['compiler_attestation']['self_check']!r}")
    rep_t1 = verify_package(t1, SRC_V1, CFG / "policy_marketing.json",
                            CFG / "lineage.json", CFG / "proj_marketing.json")
    print(rep_t1.format())
    record("T1 manifest tampering detected (self-attestation ignored)",
           not rep_t1.ok
           and any(c.name == "columns_match_independent_resolution" and not c.ok
                   for c in rep_t1.checks))

    # T2: inject a forbidden column (id_number) into the data
    t2 = OUT / "t2" / pkg_m.name
    t2.parent.mkdir(parents=True)
    shutil.copytree(pkg_m, t2)
    con = duckdb.connect()
    f = t2 / "customers.parquet"
    con.execute(f"COPY (SELECT *, 'LEAKED' AS id_number FROM read_parquet('{f}')) "
                f"TO '{f}.tmp' (FORMAT PARQUET)")
    con.close()
    shutil.move(str(f) + ".tmp", f)
    rep_t2 = verify_package(t2, SRC_V1, CFG / "policy_marketing.json",
                            CFG / "lineage.json", CFG / "proj_marketing.json")
    print(rep_t2.format())
    record("T2 injected forbidden column detected",
           not rep_t2.ok
           and any(c.name == "no_forbidden_columns_and_schema_exact" and not c.ok
                   for c in rep_t2.checks))

    # T3: forge the compiler's own decision evidence; the verifier recomputes
    # how many decisions there must be and never trusts the file's verdicts.
    t3 = OUT / "t3" / pkg_m.name
    t3.parent.mkdir(parents=True)
    shutil.copytree(pkg_m, t3)
    (t3 / "decisions.jsonl").write_text(
        json.dumps({"type": "package", "package_id": pkg_m.name}) + "\n" +
        json.dumps({"type": "decision", "output": "everything", "status": "included",
                    "action": "allow", "reason": "forged by attacker"}) + "\n")
    rep_t3 = verify_package(t3, SRC_V1, CFG / "policy_marketing.json",
                            CFG / "lineage.json", CFG / "proj_marketing.json")
    print(rep_t3.format())
    record("T3 forged compiler evidence detected (verifier recomputes, never trusts)",
           not rep_t3.ok
           and any(c.name == "decision_evidence_present" and not c.ok
                   for c in rep_t3.checks))

    hr("9. Source version change: drift detection + new immutable package")
    expect_error("generation against a drifted source rejected",
                 (E.SourceDriftError,),
                 lambda: generate(plan_m, SRC_V2, PKGS))
    schema2 = SourceSchema.load(SRC_V2)
    plan_m_v2 = compile_plan(schema2, pol_m, lin, proj_m)
    pkg_m_v2, st = generate(plan_m_v2, SRC_V2, PKGS)
    print(f"new package for v2: {pkg_m_v2.name}  [{st}]")
    record("source version change produces a new package id",
           pkg_m_v2.name != pkg_m.name)
    record("old package still intact", (pkg_m / "manifest.json").exists())
    rep_old = verify_package(pkg_m, SRC_V1, CFG / "policy_marketing.json",
                             CFG / "lineage.json", CFG / "proj_marketing.json")
    rep_new = verify_package(pkg_m_v2, SRC_V2, CFG / "policy_marketing.json",
                             CFG / "lineage.json", CFG / "proj_marketing.json")
    record("old package verifies against v1 source", rep_old.ok)
    record("new package verifies against v2 source", rep_new.ok)
    rep_stale = verify_package(pkg_m, SRC_V2, CFG / "policy_marketing.json",
                               CFG / "lineage.json", CFG / "proj_marketing.json")
    record("old package does NOT verify against the wrong source", not rep_stale.ok)

    hr("10. Read interface (bound to consumer identity)")
    data = read_package(pkg_m, "marketing")
    for name, ds in data.items():
        print(f"  dataset {name}: {len(ds['rows'])} rows x {len(ds['columns'])} cols "
              f"{ds['columns']}")
        for row in ds["rows"][:3]:
            print("    " + " | ".join(str(v) for v in row))
    expect_error("reading as another consumer refused", (DeliveryError,),
                 lambda: read_package(pkg_m, "fraud_analyst"))

    hr("SUMMARY")
    ok = all(r[1] for r in RESULTS)
    for name, passed, detail in RESULTS:
        print(f"  [{'OK' if passed else 'FAIL'}] {name}")
    print(f"\n{'ALL CHECKS PASSED' if ok else 'SOME CHECKS FAILED'} "
          f"({sum(1 for r in RESULTS if r[1])}/{len(RESULTS)})")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
