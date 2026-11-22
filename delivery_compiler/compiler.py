"""Delivery compiler: validates inputs, resolves policy, builds a per-consumer plan.

Compile-time validation rejects: dangling references, cyclic/unresolvable
lineage, duplicate output names, sensitive fields not covered by any rule,
aggregate misuse (missing group keys / undeclared stats / plain projections
renamed as aggregates), and denied join keys. The plan pins the source version
(table content hashes) so generation always reads a fixed source.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .common import NAME_RE, hash_obj
from .errors import (DanglingReferenceError, InvalidProjectionError,
                     OutputNameConflictError, UncoveredSensitiveError)
from .lineage import LineageResolver
from .policy import STRICTNESS, load_rules, resolve
from .schema import SENSITIVE_TAGS, SourceSchema, field_sql

STATS = ("count", "sum", "avg", "min", "max")

MASK_SQL = {
    "hash": lambda e: f"substr(md5(cast({e} as varchar)), 1, 10)",
    "email_domain": lambda e: (
        "concat('u_', substr(md5(cast({e} as varchar)), 1, 8), "
        "'@', split_part(cast({e} as varchar), '@', 2))").format(e=e),
    "null": lambda e: "cast(null as varchar)",
}


def _mask_sql(style, expr):
    return MASK_SQL[style or "hash"](expr)


@dataclass
class PlannedColumn:
    name: str
    action: str
    sql: str
    lineage: list
    matched_rules: list
    reason: str


@dataclass
class PlannedDataset:
    name: str
    group_keys: list
    base_table: str
    joins: list               # relation names, in join order
    columns: list             # PlannedColumn
    sql: str
    table_chain: list = None  # [base, joined_table, ...], in join order


@dataclass
class Plan:
    consumer: str
    purpose: str
    source_version: str
    table_hashes: dict
    policy_version: str
    lineage_version: str
    projection_version: str
    datasets: list            # PlannedDataset
    decisions: list           # per-field decision dicts (included + excluded)
    forbidden_paths: list     # source paths resolved to deny (never published)
    excluded_outputs: list    # requested output names that were excluded
    scanned_columns: list     # source paths the generated query actually reads

    def identity(self):
        return {
            "consumer": self.consumer,
            "purpose": self.purpose,
            "source": {"version": self.source_version, "tables": self.table_hashes},
            "policy_version": self.policy_version,
            "lineage_version": self.lineage_version,
            "projection_version": self.projection_version,
        }

    def package_id(self):
        return "pkg_" + hash_obj(self.identity())[:16]

    # -- serialisation (CLI) ------------------------------------------------
    def to_dict(self):
        return {
            "consumer": self.consumer, "purpose": self.purpose,
            "source_version": self.source_version, "table_hashes": self.table_hashes,
            "policy_version": self.policy_version,
            "lineage_version": self.lineage_version,
            "projection_version": self.projection_version,
            "datasets": [{
                "name": d.name, "group_keys": d.group_keys, "base_table": d.base_table,
                "joins": d.joins, "table_chain": d.table_chain, "sql": d.sql,
                "columns": [vars(c) for c in d.columns],
            } for d in self.datasets],
            "decisions": self.decisions,
            "forbidden_paths": self.forbidden_paths,
            "excluded_outputs": self.excluded_outputs,
            "scanned_columns": self.scanned_columns,
        }

    @classmethod
    def from_dict(cls, d):
        datasets = [PlannedDataset(
            name=ds["name"], group_keys=ds["group_keys"], base_table=ds["base_table"],
            joins=ds["joins"], sql=ds["sql"],
            columns=[PlannedColumn(**c) for c in ds["columns"]],
            table_chain=ds.get("table_chain"))
            for ds in d["datasets"]]
        return cls(d["consumer"], d["purpose"], d["source_version"], d["table_hashes"],
                   d["policy_version"], d["lineage_version"], d["projection_version"],
                   datasets, d["decisions"], d["forbidden_paths"],
                   d["excluded_outputs"], d["scanned_columns"])


def _needed_tables(items, gks):
    ts = set()
    for it in items:
        for p in it["paths"]:
            ts.add(p.split(".")[0])
    for gk in gks:
        ts.add(gk.split(".")[0])
    return ts


def _compile_dataset(schema, rules, resolver, consumer, purpose, spec, ctx):
    name = spec["name"]
    if not NAME_RE.match(name):
        raise InvalidProjectionError(f"invalid dataset name {name!r}")
    gks = list(spec.get("group_keys", []))
    for gk in gks:
        fi = schema.field(gk)
        if not fi.leaf:
            raise DanglingReferenceError(f"group key '{gk}' is not a leaf field")

    # ---- parse and validate requested fields ------------------------------
    seen = set()
    items = []
    for f in spec.get("fields", []):
        fname = f.get("name", "")
        if not NAME_RE.match(fname):
            raise InvalidProjectionError(f"invalid output name {fname!r}")
        if fname in seen:
            raise OutputNameConflictError(
                f"duplicate output name '{fname}' in dataset '{name}'")
        seen.add(fname)
        kind = f.get("kind", "row")
        if kind == "row":
            if ("source" in f) == ("derived" in f):
                raise InvalidProjectionError(
                    f"field '{fname}': declare exactly one of 'source' or 'derived'")
            if "source" in f:
                fi = schema.field(f["source"])
                if not fi.leaf:
                    raise DanglingReferenceError(
                        f"'{f['source']}' is a struct group; project a leaf path")
                items.append(dict(name=fname, kind="row", paths=[f["source"]],
                                  sql=field_sql(schema, f["source"]), derived=None))
            else:
                rd = resolver.resolve(f["derived"])   # unresolvable/cyclic -> error
                items.append(dict(name=fname, kind="row", paths=list(rd.source_paths),
                                  sql=rd.sql, derived=f["derived"]))
        elif kind == "aggregate":
            stat = f.get("stat")
            if stat not in STATS:
                raise InvalidProjectionError(
                    f"field '{fname}': unknown stat {stat!r} (allowed: {list(STATS)})")
            of = f.get("of")
            if of is None:
                if stat != "count":
                    raise InvalidProjectionError(
                        f"field '{fname}': stat '{stat}' requires an 'of' field")
                items.append(dict(name=fname, kind="aggregate", paths=[], sql=None,
                                  stat=stat, of=None, derived=None))
            else:
                fi = schema.field(of)
                if not fi.leaf:
                    raise DanglingReferenceError(f"'{of}' is not a leaf field")
                items.append(dict(name=fname, kind="aggregate", paths=[of],
                                  sql=field_sql(schema, of), stat=stat, of=of,
                                  derived=None))
        else:
            raise InvalidProjectionError(f"field '{fname}': unknown kind {kind!r}")

    # ---- join planning (two passes: conservative, then pruned) ------------
    def build_ctx(active_items):
        base, reach = schema.find_base(_needed_tables(active_items, gks))
        via = {t: [r.name for r in rs] for t, rs in reach.items()}
        join_order = [rs[-1] for t, rs in reach.items() if rs]
        return base, reach, via, join_order

    def resolve_paths(active_items, via):
        out = []
        for it in active_items:
            per = []
            for p in it["paths"]:
                fi = schema.field(p)
                d = resolve(rules, consumer, purpose, p, tags=fi.tags,
                            via_ctx=via.get(fi.table, ()))
                if not d.covered and (fi.tags & SENSITIVE_TAGS):
                    raise UncoveredSensitiveError(
                        f"sensitive field '{p}' (tags "
                        f"{sorted(fi.tags & SENSITIVE_TAGS)}) is not covered by any "
                        f"policy rule for consumer '{consumer}'")
                per.append((p, d))
            out.append(per)
        return out

    _, _, via1, _ = build_ctx(items)
    per_item = resolve_paths(items, via1)

    def strictest_of(per):
        if not per:
            return None
        return max(per, key=lambda pd: STRICTNESS[pd[1].action])

    # fields excluded at row level do not pull their tables into the join plan
    def is_excluded(i):
        it = items[i]
        if gks or it["kind"] != "row":
            return False
        s = strictest_of(per_item[i])
        return s is not None and s[1].action == "deny"

    active = [it for i, it in enumerate(items) if not is_excluded(i)]
    if not active and not gks:
        raise InvalidProjectionError(
            f"dataset '{name}' has no publishable fields for consumer '{consumer}'")
    base, reach, via, join_order = build_ctx(active)
    per_item = resolve_paths(items, via)   # final resolution in the pruned context

    # ---- join keys must be readable ---------------------------------------
    for r in join_order:
        for ep in (f"{r.from_table}.{r.from_column}", f"{r.to_table}.{r.to_column}"):
            fi = schema.field(ep)
            d = resolve(rules, consumer, purpose, ep, tags=fi.tags,
                        via_ctx=via.get(fi.table, ()))
            if d.action == "deny":
                raise InvalidProjectionError(
                    f"join key '{ep}' (relation '{r.name}') is denied for consumer "
                    f"'{consumer}'; cannot build the join path")
            ctx["scanned"].add(ep)

    # ---- stable row keys must be declared and readable --------------------
    # Row lineage identifies source records solely by these business keys, so
    # every table participating in the dataset (base + join chain) must have a
    # valid _row_keys declaration whose columns are not denied.
    table_chain = [base] + [r.to_table for r in join_order]
    for t in table_chain:
        key_cols = schema.row_keys.get(t)
        if not key_cols:
            raise InvalidProjectionError(
                f"table '{t}' has no stable row-key declaration (_row_keys); "
                f"row lineage cannot identify its source records")
        for kc in key_cols:
            path = f"{t}.{kc}"
            fi = schema.field(path)
            d = resolve(rules, consumer, purpose, path, tags=fi.tags,
                        via_ctx=via.get(t, ()))
            if d.action == "deny":
                raise InvalidProjectionError(
                    f"row-key column '{path}' of table '{t}' is denied for "
                    f"consumer '{consumer}'; provenance requires readable "
                    f"stable row keys")
            ctx["scanned"].add(path)

    # ---- group keys must be visible and projected --------------------------
    for gk in gks:
        fi = schema.field(gk)
        d = resolve(rules, consumer, purpose, gk, tags=fi.tags,
                    via_ctx=via.get(fi.table, ()))
        if d.action not in ("allow", "mask"):
            raise InvalidProjectionError(
                f"group key '{gk}' is not visible for consumer '{consumer}' "
                f"(action: {d.action})")
        if not any(it["kind"] == "row" and it["paths"] == [gk] for it in items):
            raise InvalidProjectionError(
                f"group key '{gk}' must be projected as a field in dataset '{name}'")
        ctx["scanned"].add(gk)

    # ---- build columns and decisions ---------------------------------------
    columns, deferred_count = [], []
    agg_stat_grants = set()
    for i, it in enumerate(items):
        per = per_item[i]
        strict = strictest_of(per)
        act = strict[1].action if strict else "allow"
        matched = sorted({d.rule.rid for _, d in per if d.rule})
        if it["derived"]:
            reason = ("derived field inherits the strictest restriction of its inputs: "
                      + "; ".join(
                          f"{p} -> {d.action} ({'rule ' + d.rule.rid if d.rule else 'default allow'})"
                          for p, d in per))
        else:
            reason = strict[1].reason if strict else "no inputs"

        if it["kind"] == "row":
            if gks:
                if len(it["paths"]) != 1 or it["paths"][0] not in gks:
                    raise InvalidProjectionError(
                        f"plain projection '{it['name']}' in grouped dataset '{name}': "
                        f"only declared group keys and aggregate stats are allowed; a "
                        f"plain projection cannot be renamed into an aggregate")
                col_sql = it["sql"] if act == "allow" else _mask_sql(
                    strict[1].rule.mask_style if strict[1].rule else None, it["sql"])
                columns.append(PlannedColumn(it["name"], act, col_sql,
                                             list(it["paths"]), matched, reason))
                ctx["scanned"].update(it["paths"])
                ctx["decisions"].append(_decision(name, it, "included", act,
                                                  it["paths"], matched, reason))
            else:
                if act == "deny":
                    for p, d in per:
                        if d.action == "deny":
                            ctx["forbidden"].add(p)
                    ctx["excluded"].append(it["name"])
                    ctx["decisions"].append(_decision(
                        name, it, "excluded", "deny", it["paths"], matched,
                        reason + "; denied fields are not published"))
                    continue
                if act == "aggregate":
                    raise InvalidProjectionError(
                        f"field '{it['name']}' resolves to 'aggregate' for consumer "
                        f"'{consumer}'; it cannot be projected at row level, and a "
                        f"plain projection cannot be renamed into an aggregate")
                col_sql = it["sql"] if act == "allow" else _mask_sql(
                    strict[1].rule.mask_style if strict[1].rule else None, it["sql"])
                columns.append(PlannedColumn(it["name"], act, col_sql,
                                             list(it["paths"]), matched, reason))
                ctx["scanned"].update(it["paths"])
                ctx["decisions"].append(_decision(name, it, "included", act,
                                                  it["paths"], matched, reason))
        else:  # aggregate kind
            if not gks:
                raise InvalidProjectionError(
                    f"aggregate field '{it['name']}' requires a grouped dataset "
                    f"(declare group_keys)")
            if it["of"] is None:
                deferred_count.append(it)
                col_sql = "count(*)"
                columns.append(PlannedColumn(it["name"], "aggregate", col_sql, [],
                                             matched, reason))
                ctx["decisions"].append(_decision(name, it, "included", "aggregate",
                                                  [], matched, reason))
                continue
            d = per[0][1]
            of = it["of"]
            if d.action == "deny":
                ctx["forbidden"].add(of)
                ctx["excluded"].append(it["name"])
                ctx["decisions"].append(_decision(
                    name, it, "excluded", "deny", [of], matched,
                    reason + "; a denied field cannot be aggregated either"))
                continue
            if d.action == "mask":
                raise InvalidProjectionError(
                    f"cannot aggregate masked field '{of}' (field '{it['name']}')")
            if d.action == "aggregate":
                rule = d.rule
                if rule is None or it["stat"] not in rule.stats:
                    raise InvalidProjectionError(
                        f"stat '{it['stat']}' is not allowed for '{of}' by rule "
                        f"'{rule.rid if rule else None}'; allowed stats: "
                        f"{list(rule.stats) if rule else []}")
                if set(gks) != set(rule.group_keys):
                    raise InvalidProjectionError(
                        f"group keys {gks} do not match the group keys declared by "
                        f"rule '{rule.rid}': {list(rule.group_keys)}")
                agg_stat_grants.update(rule.stats)
            col_sql = f"{it['stat']}({it['sql']})"
            columns.append(PlannedColumn(it["name"], "aggregate", col_sql, [of],
                                         matched, reason))
            ctx["scanned"].update(it["paths"])
            ctx["decisions"].append(_decision(name, it, "included", "aggregate",
                                              [of], matched, reason))

    for it in deferred_count:
        if "count" not in agg_stat_grants:
            raise InvalidProjectionError(
                f"count(*) field '{it['name']}' in dataset '{name}' is not covered by "
                f"any rule's allowed stats")

    if not columns:
        raise InvalidProjectionError(
            f"dataset '{name}' has no publishable columns for consumer '{consumer}'")

    # ---- executable SQL ------------------------------------------------------
    selects = [f'{c.sql} AS "{c.name}"' for c in columns]
    sql = f'FROM "{base}" AS {base}'
    for r in join_order:
        sql += (f' LEFT JOIN "{r.to_table}" AS {r.to_table}'
                f' ON {r.from_table}."{r.from_column}" = {r.to_table}."{r.to_column}"')
    if gks:
        gk_exprs = []
        for gk in gks:
            col = next(c for c in columns if c.lineage == [gk])
            gk_exprs.append(col.sql)
        sql = "SELECT " + ", ".join(selects) + " " + sql
        sql += " GROUP BY " + ", ".join(gk_exprs)
    else:
        sql = "SELECT " + ", ".join(selects) + " " + sql

    return PlannedDataset(name, gks, base, [r.name for r in join_order], columns,
                          sql, table_chain), via


def _decision(ds_name, it, status, action, paths, matched, reason):
    return {
        "dataset": ds_name,
        "output": it["name"],
        "status": status,
        "action": action,
        "lineage": list(paths),
        "matched_rules": matched,
        "reason": reason,
    }


def compile_plan(schema: SourceSchema, policy_doc, lineage_doc, proj_doc, con=None) -> Plan:
    rules = load_rules(policy_doc)
    consumer = proj_doc.get("consumer") or policy_doc.get("consumer")
    if not consumer:
        raise InvalidProjectionError("no consumer given in projections or policy")
    purpose = proj_doc.get("purpose", "*")
    resolver = LineageResolver(lineage_doc, schema)

    ds_specs = proj_doc.get("datasets", [])
    names = [d["name"] for d in ds_specs]
    if len(set(names)) != len(names):
        raise OutputNameConflictError(f"duplicate dataset name in {names}")

    ctx = {"decisions": [], "forbidden": set(), "scanned": set(), "excluded": []}
    datasets = []
    table_via = {}
    for spec in ds_specs:
        ds, via = _compile_dataset(schema, rules, resolver, consumer,
                                   purpose, spec, ctx)
        datasets.append(ds)
        for t, v in via.items():
            table_via.setdefault(t, set()).update(v)

    # Global forbidden set: every source leaf that resolves to deny for this
    # consumer (in the join contexts this plan uses) -- not only the requested
    # ones. This is what the package proves absent.
    for p in schema.leaf_paths():
        fi = schema.field(p)
        d = resolve(rules, consumer, purpose, p, tags=fi.tags,
                    via_ctx=tuple(table_via.get(fi.table, ())))
        if d.action == "deny":
            ctx["forbidden"].add(p)

    if con is not None:  # validate generated SQL against the real source
        for ds in datasets:
            try:
                con.execute(f"SELECT * FROM ({ds.sql}) LIMIT 0")
            except Exception as e:  # binder/planner errors surface here
                raise InvalidProjectionError(
                    f"generated SQL for dataset '{ds.name}' failed validation: {e}") from e

    return Plan(
        consumer=consumer, purpose=purpose,
        source_version=schema.version, table_hashes=schema.table_hashes,
        policy_version=hash_obj(policy_doc),
        lineage_version=hash_obj(lineage_doc),
        projection_version=hash_obj(proj_doc),
        datasets=datasets,
        decisions=ctx["decisions"],
        forbidden_paths=sorted(ctx["forbidden"]),
        excluded_outputs=sorted(ctx["excluded"]),
        scanned_columns=sorted(ctx["scanned"]),
    )


def format_decisions(plan: Plan) -> str:
    """Human-readable per-field decision evidence (the 'preview' interface)."""
    lines = []
    for d in plan.decisions:
        mark = "OK " if d["status"] == "included" else "OUT"
        lines.append(f"[{mark}] {d['dataset']}.{d['output']}  action={d['action']}")
        lines.append(f"      lineage: {', '.join(d['lineage']) or '(none)'}")
        lines.append(f"      rules:   {', '.join(d['matched_rules']) or '(none)'}")
        lines.append(f"      reason:  {d['reason']}")
    return "\n".join(lines)
