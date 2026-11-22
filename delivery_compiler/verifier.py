"""Independent delivery-package verifier.

This module is deliberately independent of the compiler: it does NOT import
delivery_compiler.compiler / .policy / .lineage / .generator. Given only the
source schema, the raw policy/lineage/projection documents and the package
manifest, it re-derives what the package SHOULD contain and checks what it
ACTUALLY contains:

  1. package identity hash matches source/policy/lineage/projection versions;
  2. the source database still matches the pinned version in the manifest;
  3. manifest columns/actions/lineage match an independent policy resolution;
  4. forbidden (deny) fields appear nowhere: not in Parquet schema, not in
     stats, not as lineage of any published column;
  5. Parquet contents equal an independently recomputed query result;
  6. row/group counts in the manifest match the actual data;
  7. the recorded scanned-column set is exactly the minimal required set and
     contains no denied field.

It never reads the compiler's self-attestation block or any "passed" flag.
Only schema.py / common.py (input readers) are shared with the compiler.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from .common import SQL_KEYWORDS, hash_obj, load_json, tokenize
from .schema import SENSITIVE_TAGS, SourceSchema, field_sql

# --- independent copy of the policy semantics (spec, not compiler code) -----
_STRICT = {"allow": 0, "mask": 1, "aggregate": 2, "deny": 3}
_ACTIONS = ("allow", "mask", "aggregate", "deny")


def _parse_rules(policy_doc):
    rules = []
    for raw in policy_doc.get("rules", []):
        r = dict(raw)
        r["kind"] = "path" if "path" in raw else "tag"
        r["value"] = raw.get("path", raw.get("tag"))
        r["consumer"] = raw.get("consumer", "*")
        r["purpose"] = raw.get("purpose", "*")
        r["priority"] = int(raw.get("priority", 0))
        r["via"] = tuple(raw.get("via", ()))
        r["stats"] = tuple(raw.get("stats", ()))
        r["group_keys"] = tuple(raw.get("group_keys", ()))
        rules.append(r)
    return rules


def _spec(rule):
    return len(rule["value"].split(".")) if rule["kind"] == "path" else 0


def _match(rule, consumer, purpose, path, tags, via_ctx):
    if rule["consumer"] not in ("*", consumer):
        return False
    if rule["purpose"] not in ("*", purpose):
        return False
    if rule["via"] and not (set(rule["via"]) & set(via_ctx)):
        return False
    if rule["kind"] == "path":
        return path == rule["value"] or path.startswith(rule["value"] + ".")
    return rule["value"] in tags


def _resolve(rules, consumer, purpose, path, tags, via_ctx):
    """Independent re-implementation of policy resolution. Returns (action, rule|None, covered)."""
    cand = [r for r in rules if _match(r, consumer, purpose, path, tags, via_ctx)]
    if not cand:
        return "allow", None, False
    denies = [r for r in cand if r["action"] == "deny"]
    others = [r for r in cand if r["action"] != "deny"]

    def key(r):
        return (r["priority"], _spec(r))

    def top(rs):
        bk = max(key(r) for r in rs)
        tops = [r for r in rs if key(r) == bk]
        if len({r["action"] for r in tops}) > 1:
            raise ValueError(f"policy conflict for '{path}' at {bk}")
        return tops[0]

    bd = top(denies) if denies else None
    bo = top(others) if others else None
    if bd is None:
        return bo["action"], bo, True
    if bo is None:
        return "deny", bd, True
    lifters = [r for r in others
               if r["kind"] == "path" and r["priority"] > bd["priority"]]
    if lifters:
        w = top(lifters)
        return w["action"], w, True
    if key(bo) == key(bd) and bo["kind"] == "path" and bd["kind"] == "path":
        raise ValueError(f"policy conflict for '{path}': deny vs non-deny at {key(bd)}")
    return "deny", bd, True


def _mask(style, expr):
    if style == "email_domain":
        return ("concat('u_', substr(md5(cast({e} as varchar)), 1, 8), "
                "'@', split_part(cast({e} as varchar), '@', 2))").format(e=expr)
    if style == "null":
        return "cast(null as varchar)"
    return f"substr(md5(cast({expr} as varchar)), 1, 10)"


class _Derived:
    """Independent lineage resolution (references + transitive source paths + SQL)."""

    def __init__(self, lineage_doc, schema):
        self.defs = {d["name"]: d for d in lineage_doc}
        self.schema = schema
        self.cache = {}

    def resolve(self, name, stack=()):
        if name in self.cache:
            return self.cache[name]
        if name in stack:
            raise ValueError(f"cyclic lineage: {' -> '.join(list(stack) + [name])}")
        d = self.defs[name]
        toks = tokenize(d["expression"])
        refs, parts = [], []
        for i, (kind, text) in enumerate(toks):
            if kind != "ident":
                parts.append(text)
                continue
            nxt = toks[i + 1] if i + 1 < len(toks) else None
            if text.lower() in SQL_KEYWORDS or (
                    nxt and nxt[0] == "op" and nxt[1].startswith("(")):
                parts.append(text)
                continue
            if "." in text:
                refs.append(text)
                parts.append(field_sql(self.schema, text))
            else:
                sub = self.resolve(text, stack + (name,))
                refs.append(text)
                parts.append(f"({sub['sql']})")
        srcs = []
        for r in refs:
            srcs.extend([r] if "." in r else self.cache[r]["source_paths"])
        out = {"sql": "".join(parts), "source_paths": sorted(set(srcs))}
        self.cache[name] = out
        return out


def _expected(schema, policy_doc, lineage_doc, proj_doc):
    """Independently recompute the expected package content."""
    rules = _parse_rules(policy_doc)
    consumer = proj_doc.get("consumer") or policy_doc.get("consumer")
    purpose = proj_doc.get("purpose", "*")
    derived = _Derived(lineage_doc, schema)
    forbidden, scanned, excluded = set(), set(), []
    uncovered = []
    datasets = []
    table_via = {}

    for spec in proj_doc.get("datasets", []):
        gks = list(spec.get("group_keys", []))
        items = []
        for f in spec.get("fields", []):
            kind = f.get("kind", "row")
            if kind == "row":
                if "source" in f:
                    paths, sql = [f["source"]], field_sql(schema, f["source"])
                else:
                    rd = derived.resolve(f["derived"])
                    paths, sql = list(rd["source_paths"]), rd["sql"]
                items.append({"name": f["name"], "kind": "row", "paths": paths,
                              "sql": sql, "stat": None, "of": None})
            else:
                of = f.get("of")
                items.append({"name": f["name"], "kind": "aggregate",
                              "paths": [of] if of else [],
                              "sql": field_sql(schema, of) if of else None,
                              "stat": f.get("stat"), "of": of})

        def tables_of(its):
            ts = {p.split(".")[0] for it in its for p in it["paths"]}
            ts |= {g.split(".")[0] for g in gks}
            return ts

        def ctx_for(its):
            base, reach = schema.find_base(tables_of(its))
            via = {t: [r.name for r in rs] for t, rs in reach.items()}
            joins = [rs[-1] for t, rs in reach.items() if rs]
            return base, via, joins

        def path_decisions(it, via):
            out = []
            for p in it["paths"]:
                fi = schema.field(p)
                a, r, cov = _resolve(rules, consumer, purpose, p, fi.tags,
                                     via.get(fi.table, ()))
                if not cov and (fi.tags & SENSITIVE_TAGS):
                    uncovered.append(p)
                out.append((p, a, r))
            return out

        def act_of(it, via):
            best = ("allow", None)
            for p, a, r in path_decisions(it, via):
                if _STRICT[a] > _STRICT[best[0]]:
                    best = (a, r)
            return best

        _, via0, _ = ctx_for(items)
        active = [it for it in items
                  if not (not gks and it["kind"] == "row"
                          and act_of(it, via0)[0] == "deny")]
        base, via, joins = ctx_for(active)
        for t, v in via.items():
            table_via.setdefault(t, set()).update(v)

        for r in joins:
            for ep in (f"{r.from_table}.{r.from_column}", f"{r.to_table}.{r.to_column}"):
                scanned.add(ep)
        for gk in gks:
            scanned.add(gk)

        columns, gk_sqls = [], []
        for it in items:
            act, rule = act_of(it, via)
            if it["kind"] == "row" and not gks and act == "deny":
                excluded.append(it["name"])
                for p in it["paths"]:
                    a, _, _ = _resolve(rules, consumer, purpose, p,
                                       schema.field(p).tags,
                                       via.get(p.split(".")[0], ()))
                    if a == "deny":
                        forbidden.add(p)
                continue
            if it["kind"] == "row" and not gks and act == "aggregate":
                continue  # compiler rejects; verifier just never expects it
            if it["kind"] == "aggregate" and it["of"] is not None:
                a, _, _ = _resolve(rules, consumer, purpose, it["of"],
                                   schema.field(it["of"]).tags,
                                   via.get(it["of"].split(".")[0], ()))
                if a == "deny":
                    excluded.append(it["name"])
                    forbidden.add(it["of"])
                    continue
                sql = f"{it['stat']}({it['sql']})"
                act = "aggregate"
            elif it["kind"] == "aggregate":
                sql, act = "count(*)", "aggregate"
            else:
                sql = it["sql"] if act == "allow" else _mask(
                    (rule or {}).get("mask_style"), it["sql"])
            matched = sorted({r["id"] for _, _, r in path_decisions(it, via) if r})
            columns.append({"name": it["name"], "action": act, "sql": sql,
                            "lineage": sorted(it["paths"]), "matched_rules": matched})
            scanned.update(it["paths"])
            if gks and it["kind"] == "row":
                gk_sqls.append(sql)

        selects = ", ".join(f'{c["sql"]} AS "{c["name"]}"' for c in columns)
        sql = f'FROM "{base}" AS {base}'
        for r in joins:
            sql += (f' LEFT JOIN "{r.to_table}" AS {r.to_table}'
                    f' ON {r.from_table}."{r.from_column}" = {r.to_table}."{r.to_column}"')
        sql = "SELECT " + selects + " " + sql
        if gks:
            sql += " GROUP BY " + ", ".join(gk_sqls)
        datasets.append({"name": spec["name"], "group_keys": gks,
                         "columns": columns, "sql": sql})

    # global forbidden set: every source leaf resolving to deny for this consumer
    for p in schema.leaf_paths():
        fi = schema.field(p)
        a, _, _ = _resolve(rules, consumer, purpose, p, fi.tags,
                           tuple(table_via.get(fi.table, ())))
        if a == "deny":
            forbidden.add(p)

    return {"datasets": datasets, "forbidden": sorted(forbidden),
            "scanned": sorted(scanned), "excluded": sorted(excluded),
            "uncovered": sorted(set(uncovered)),
            "consumer": consumer, "purpose": purpose}


# ---------------------------------------------------------------------------

@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""


@dataclass
class VerifyReport:
    package: str
    checks: list = field(default_factory=list)

    @property
    def ok(self):
        return all(c.ok for c in self.checks)

    def add(self, name, ok, detail=""):
        self.checks.append(Check(name, bool(ok), detail))

    def format(self):
        lines = []
        for c in self.checks:
            mark = "PASS" if c.ok else "FAIL"
            lines.append(f"  [{mark}] {c.name}" + (f" -- {c.detail}" if c.detail else ""))
        verdict = "VERIFIED" if self.ok else "VERIFICATION FAILED"
        return f"package {self.package}: {verdict}\n" + "\n".join(lines)


def verify_package(pkg_dir, db_path, policy_path, lineage_path, projections_path) -> VerifyReport:
    pkg_dir = Path(pkg_dir)
    rep = VerifyReport(str(pkg_dir))
    manifest = json.loads((pkg_dir / "manifest.json").read_text(encoding="utf-8"))
    schema = SourceSchema.load(db_path)
    policy_doc = load_json(policy_path)
    lineage_doc = load_json(lineage_path)
    proj_doc = load_json(projections_path)

    # 1. package identity: recompute from inputs, compare to manifest and dirname
    identity = {
        "consumer": proj_doc.get("consumer") or policy_doc.get("consumer"),
        "purpose": proj_doc.get("purpose", "*"),
        "source": {"version": schema.version, "tables": schema.table_hashes},
        "policy_version": hash_obj(policy_doc),
        "lineage_version": hash_obj(lineage_doc),
        "projection_version": hash_obj(proj_doc),
    }
    pid = "pkg_" + hash_obj(identity)[:16]
    rep.add("package_identity",
            pid == manifest.get("package_id") == pkg_dir.name
            and manifest.get("identity") == identity,
            f"expected {pid}, manifest says {manifest.get('package_id')}")

    # 2. source still matches the pinned version
    src = manifest.get("source", {})
    rep.add("source_version_pinned",
            src.get("version") == schema.version and src.get("tables") == schema.table_hashes,
            f"manifest source {src.get('version')} vs actual {schema.version}")

    # 3. config versions
    rep.add("policy_version_matches", manifest.get("policy_version") == hash_obj(policy_doc))
    rep.add("lineage_version_matches", manifest.get("lineage_version") == hash_obj(lineage_doc))
    rep.add("projection_version_matches",
            manifest.get("projection_version") == hash_obj(proj_doc))

    # 4. independent re-derivation of expected content
    try:
        exp = _expected(schema, policy_doc, lineage_doc, proj_doc)
    except Exception as e:
        rep.add("independent_recompute", False, f"{type(e).__name__}: {e}")
        return rep
    exp_ds = {d["name"]: d for d in exp["datasets"]}
    man_ds = {d["name"]: d for d in manifest.get("datasets", [])}

    col_problems = []
    if set(exp_ds) != set(man_ds):
        col_problems.append(f"datasets differ: expected {sorted(exp_ds)}, "
                            f"manifest {sorted(man_ds)}")
    for name in sorted(set(exp_ds) & set(man_ds)):
        ecols = {c["name"]: c for c in exp_ds[name]["columns"]}
        mcols = {c["name"]: c for c in man_ds[name]["columns"]}
        if set(ecols) != set(mcols):
            col_problems.append(f"{name}: columns differ: expected {sorted(ecols)}, "
                                f"manifest {sorted(mcols)}")
            continue
        for cname in ecols:
            e, m = ecols[cname], mcols[cname]
            if e["action"] != m.get("action"):
                col_problems.append(f"{name}.{cname}: action manifest={m.get('action')} "
                                    f"expected={e['action']}")
            if sorted(e["lineage"]) != sorted(m.get("lineage", [])):
                col_problems.append(f"{name}.{cname}: lineage manifest={m.get('lineage')} "
                                    f"expected={e['lineage']}")
            if sorted(e["matched_rules"]) != sorted(m.get("matched_rules", [])):
                col_problems.append(f"{name}.{cname}: matched_rules differ")
        if sorted(exp_ds[name]["group_keys"]) != sorted(man_ds[name].get("group_keys", [])):
            col_problems.append(f"{name}: group_keys differ")
    rep.add("columns_match_independent_resolution", not col_problems,
            "; ".join(col_problems[:4]))

    # 5. forbidden fields: manifest must list them; data/schema/stats must not
    man_forbidden = set(manifest.get("forbidden_paths", []))
    missing = set(exp["forbidden"]) - man_forbidden
    rep.add("forbidden_paths_declared", not missing,
            f"missing from manifest: {sorted(missing)}" if missing else "")
    rep.add("excluded_outputs_match",
            sorted(manifest.get("excluded_outputs", [])) == exp["excluded"],
            f"manifest {sorted(manifest.get('excluded_outputs', []))} vs expected "
            f"{exp['excluded']}")
    rep.add("no_uncovered_sensitive_fields", not exp["uncovered"],
            f"sensitive fields without policy coverage: {exp['uncovered']}"
            if exp["uncovered"] else "")

    forb_leaves = {p.split(".")[-1] for p in exp["forbidden"]} | set(exp["forbidden"])
    forb_leaves |= set(exp["excluded"])
    stats_text = json.dumps(manifest.get("stats", {}))
    stats_bad = [f for f in forb_leaves if f in stats_text]
    rep.add("no_forbidden_in_stats", not stats_bad,
            f"forbidden names in stats: {stats_bad}" if stats_bad else "")

    # 6. actual Parquet schema + data vs independent recomputation.
    # Expected queries run against the pinned SOURCE database; the package's
    # Parquet files are only ever read, never executed against.
    con = duckdb.connect(str(db_path), read_only=True)
    schema_bad, data_bad, count_bad = [], [], []
    try:
        for name in sorted(set(exp_ds) & set(man_ds)):
            file = pkg_dir / man_ds[name]["file"]
            if not file.exists():
                schema_bad.append(f"{name}: missing file {file.name}")
                continue
            actual_cols = [r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{file}')").fetchall()]
            expected_names = [c["name"] for c in exp_ds[name]["columns"]]
            if actual_cols != expected_names:
                schema_bad.append(f"{name}: parquet columns {actual_cols} != "
                                  f"expected {expected_names}")
            leaked = [c for c in actual_cols if c in forb_leaves]
            if leaked:
                schema_bad.append(f"{name}: forbidden columns present: {leaked}")
            if actual_cols == expected_names:
                col_list = ", ".join(f'"{c}"' for c in expected_names)
                actual_q = f"SELECT {col_list} FROM read_parquet('{file}')"
                expected_q = exp_ds[name]["sql"]
                d1 = con.execute(
                    f"SELECT count(*) FROM (({expected_q}) EXCEPT ALL ({actual_q}))").fetchone()[0]
                d2 = con.execute(
                    f"SELECT count(*) FROM (({actual_q}) EXCEPT ALL ({expected_q}))").fetchone()[0]
                if d1 or d2:
                    data_bad.append(f"{name}: {d1}+{d2} differing rows")
                rows = con.execute(f"SELECT count(*) FROM read_parquet('{file}')").fetchone()[0]
                st = manifest.get("stats", {}).get("datasets", {}).get(name, {})
                if st.get("rows") != rows:
                    count_bad.append(f"{name}: manifest rows={st.get('rows')} actual={rows}")
                if exp_ds[name]["group_keys"] and st.get("groups") != rows:
                    count_bad.append(f"{name}: manifest groups={st.get('groups')} actual={rows}")
    finally:
        con.close()
    rep.add("no_forbidden_columns_and_schema_exact", not schema_bad,
            "; ".join(schema_bad[:4]))
    rep.add("data_matches_independent_recompute", not data_bad,
            "; ".join(data_bad[:4]))
    rep.add("row_and_group_counts_match", not count_bad,
            "; ".join(count_bad[:4]))

    # 7. scanned columns: exactly the minimal required set, nothing denied
    man_scanned = set(manifest.get("scanned_columns", []))
    scan_problems = []
    if man_scanned != set(exp["scanned"]):
        scan_problems.append(f"manifest {sorted(man_scanned)} != required {exp['scanned']}")
    bad_scan = man_scanned & set(exp["forbidden"])
    if bad_scan:
        scan_problems.append(f"denied fields were scanned: {sorted(bad_scan)}")
    rep.add("scanned_columns_minimal_and_clean", not scan_problems,
            "; ".join(scan_problems))

    # 8. decision evidence exists (presence only -- its verdicts are NOT trusted)
    ev_ok, ev_detail = True, ""
    ev_file = pkg_dir / "decisions.jsonl"
    if not ev_file.exists():
        ev_ok, ev_detail = False, "decisions.jsonl missing"
    else:
        entries = [json.loads(l) for l in ev_file.read_text().splitlines() if l.strip()]
        n_dec = sum(1 for e in entries if e.get("type") == "decision")
        n_exp = sum(len(d["columns"]) for d in exp["datasets"]) + len(exp["excluded"])
        if n_dec != n_exp:
            ev_ok = False
            ev_detail = f"{n_dec} decision entries, expected {n_exp}"
    rep.add("decision_evidence_present", ev_ok, ev_detail)
    return rep
