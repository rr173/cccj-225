"""Tests for per-output-row lineage (row_lineage.jsonl).

Run from the repository root:

    python3 -m unittest discover -s tests -v

Coverage
--------
* ordinary join rows carry keys from every participating table;
* composite row keys are preserved whole;
* identical projected values from different source rows stay separately
  locatable (provenance is never reverse-looked-up from output values);
* an aggregate (region summary) row lists ALL and ONLY the contributing
  transactions and customers;
* expectations are enumerated by hand in pure Python (the production lineage
  code is not used to build the expected answer);
* missing/NULL/duplicate row keys are rejected before publication;
* tampering with keys, dropping/adding contributors, changing row_ref or
  leaking a denied field all fail the INDEPENDENT verifier;
* a copied package is fully usable after the source database is removed;
* regeneration and shuffled source insertion order give byte-stable lineage;
* large inputs are processed with bounded Python memory (streaming).
"""
from __future__ import annotations

import json
import shutil
import sys
import tempfile
import tracemalloc
import unittest
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from delivery_compiler import (SourceSchema, compile_plan, generate,
                               lookup_row_lineage, verify_package)
from delivery_compiler.common import canonical, hash_obj, load_json
from delivery_compiler.errors import RowKeyError
from delivery_compiler.row_lineage import read_entries

# ---------------------------------------------------------------------------
# tiny independent source: customer_profile + transactions + region, where
# region has a COMPOSITE key (country, region_code)
# ---------------------------------------------------------------------------

REGIONS = [
    ("US", "R1", "North"),
    ("US", "R2", "South"),
    ("CN", "R3", "East"),
]
# customers: c2 and c3 deliberately share the same (segment, region) projection
CUSTOMERS = [
    (1, "Alice", "A", "US", "R1"),
    (2, "Bob", "B", "US", "R1"),
    (3, "Carol", "B", "US", "R1"),   # duplicate projection of (B, US-R1)
    (4, "Dan", "C", "CN", "R3"),
]
TRANSACTIONS = [
    (101, 1, 10.0),
    (102, 1, 20.0),
    (103, 2, 30.0),
    (104, 3, 40.0),
    (105, 4, 50.0),
]

POLICY = {
    "consumer": "analyst",
    "rules": [
        {"id": "p1", "path": "customer_profile.secret_note", "action": "deny",
         "priority": 50, "reason": "secret never leaves the lake"},
    ],
}
LINEAGE_DOC = []
PROJECTIONS = {
    "consumer": "analyst",
    "purpose": "analytics",
    "datasets": [
        {"name": "customer_view",
         "group_keys": [],
         "fields": [
             {"name": "customer_id", "source": "customer_profile.customer_id"},
             {"name": "segment", "source": "customer_profile.segment"},
             {"name": "region_name", "source": "region.region_name"},
         ]},
        # projects NO unique key: different source rows can publish identical
        # value sets and must still be separately locatable
        {"name": "segment_view",
         "group_keys": [],
         "fields": [
             {"name": "segment", "source": "customer_profile.segment"},
             {"name": "region_name", "source": "region.region_name"},
         ]},
        {"name": "region_summary",
         "group_keys": ["customer_profile.region_code", "region.country"],
         "fields": [
             {"name": "region_code", "source": "customer_profile.region_code"},
             {"name": "country", "source": "region.country"},
             {"name": "txn_count", "kind": "aggregate", "stat": "count"},
             {"name": "total_amount", "kind": "aggregate", "stat": "sum",
              "of": "transactions.amount"},
         ]},
    ],
}

# a policy that allows count(*) for the aggregate dataset
AGG_POLICY = {
    "consumer": "analyst",
    "rules": [
        {"id": "p1", "path": "customer_profile.secret_note", "action": "deny",
         "priority": 50},
        {"id": "p2", "path": "transactions.amount", "action": "aggregate",
         "priority": 30,
         "group_keys": ["customer_profile.region_code", "region.country"],
         "stats": ["count", "sum", "avg"]},
    ],
}

RELATIONS = [
    ("txn_cust", "transactions", "customer_id", "customer_profile", "customer_id"),
    ("cust_region", "customer_profile", "region_code", "region", "region_code"),
]
ROW_KEYS = {
    "customer_profile": ["customer_id"],
    "transactions": ["txn_id"],
    "region": ["country", "region_code"],      # composite key
}


def _bulk_insert(con, table, columns, rows, chunk=50_000):
    """Fast bulk insert via multi-row VALUES (executemany is very slow here)."""
    def lit(v):
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        if isinstance(v, (int, float)):
            return str(v)
        return "'" + str(v).replace("'", "''") + "'"

    col_sql = ", ".join(columns)
    for start in range(0, len(rows), chunk):
        part = rows[start:start + chunk]
        values = ", ".join("(" + ", ".join(lit(v) for v in row) + ")"
                           for row in part)
        con.execute(f"INSERT INTO {table} ({col_sql}) VALUES {values}")


def build_source(path, *, regions=REGIONS, customers=CUSTOMERS,
                 transactions=TRANSACTIONS, row_keys=ROW_KEYS,
                 region_order=None, customer_order=None, txn_order=None,
                 extra_ddl=""):
    path = Path(path)
    if path.exists():
        path.unlink()
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE region (country VARCHAR, region_code VARCHAR, "
                "region_name VARCHAR)")
    con.execute("CREATE TABLE customer_profile (customer_id BIGINT, full_name "
                "VARCHAR, segment VARCHAR, region_code VARCHAR, country VARCHAR, "
                "secret_note VARCHAR)")
    con.execute("CREATE TABLE transactions (txn_id BIGINT, customer_id BIGINT, "
                "amount DOUBLE)")
    if extra_ddl:
        con.execute(extra_ddl)

    def rows_of(rows, order):
        return rows if order is None else [rows[i] for i in order]

    # bulk load when no shuffling is requested (large inputs); otherwise per-row
    if region_order is None and customer_order is None and txn_order is None:
        _bulk_insert(con, "region",
                     ["country", "region_code", "region_name"], regions)
        _bulk_insert(
            con, "customer_profile",
            ["customer_id", "full_name", "segment", "region_code", "country",
             "secret_note"],
            [(cid, name, seg, rc, country, f"SECRET-{cid}")
             for cid, name, seg, country, rc in customers])
        _bulk_insert(con, "transactions",
                     ["txn_id", "customer_id", "amount"], transactions)
    else:
        for row in rows_of(regions, region_order):
            con.execute("INSERT INTO region VALUES (?, ?, ?)", row)
        for cid, name, seg, country, rc in rows_of(customers, customer_order):
            con.execute(
                "INSERT INTO customer_profile (customer_id, full_name, segment, "
                "region_code, country, secret_note) VALUES (?, ?, ?, ?, ?, ?)",
                (cid, name, seg, rc, country, f"SECRET-{cid}"))
        for row in rows_of(transactions, txn_order):
            con.execute("INSERT INTO transactions VALUES (?, ?, ?)", row)

    con.execute("CREATE TABLE _snapshot (version VARCHAR)")
    con.execute("INSERT INTO _snapshot VALUES ('v1')")
    con.execute("CREATE TABLE _field_tags (path VARCHAR, tag VARCHAR)")
    con.execute("CREATE TABLE _relations (name VARCHAR, from_table VARCHAR, "
                "from_column VARCHAR, to_table VARCHAR, to_column VARCHAR)")
    con.executemany("INSERT INTO _relations VALUES (?, ?, ?, ?, ?)", RELATIONS)
    con.execute("CREATE TABLE _row_keys (table_name VARCHAR, column_name VARCHAR, "
                "key_ordinal INTEGER)")
    key_rows = [(t, c, i) for t, cols in row_keys.items()
                for i, c in enumerate(cols)]
    if key_rows:
        con.executemany("INSERT INTO _row_keys VALUES (?, ?, ?)", key_rows)
    con.close()


class LineageTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="lineage_test_"))
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.db = self.tmp / "source.duckdb"
        self.out = self.tmp / "packages"
        self.out.mkdir()
        build_source(self.db)
        self.schema = SourceSchema.load(self.db)
        self.con = duckdb.connect(str(self.db), read_only=True)
        self.addCleanup(self.con.close)
        self.plan = compile_plan(self.schema, AGG_POLICY, LINEAGE_DOC,
                                 PROJECTIONS, con=self.con)
        self.pkg, self.status = generate(self.plan, self.db, self.out)

    def lineage_entries(self, dataset):
        ds = next(d for d in json.loads(
            (self.pkg / "manifest.json").read_text())["datasets"]
            if d["name"] == dataset)
        return list(read_entries(self.pkg / ds["row_lineage"]["file"]))


class StructureAndJoinTests(LineageTestBase):
    def test_package_contains_lineage_for_every_dataset(self):
        manifest = json.loads((self.pkg / "manifest.json").read_text())
        files = sorted(p.name for p in self.pkg.iterdir())
        for ds in manifest["datasets"]:
            self.assertIn("row_lineage", ds)
            self.assertTrue((self.pkg / ds["row_lineage"]["file"]).is_file())
            self.assertEqual(ds["row_lineage"]["format"], "row-lineage/1")
            self.assertTrue(ds["row_lineage"]["sha256"])
        self.assertIn("customer_view.row_lineage.jsonl", files)
        self.assertIn("region_summary.row_lineage.jsonl", files)
        # manifest itself binds row-key declarations
        self.assertEqual(
            manifest["row_key_declarations"]["region"]["columns"],
            ["country", "region_code"])

    def test_join_rows_carry_all_table_keys_including_composite(self):
        entries = self.lineage_entries("customer_view")
        self.assertEqual(len(entries), len(CUSTOMERS))
        by_cust = {e["values"]["customer_id"]: e for e in entries}
        e1 = by_cust[1]
        tables = {s["table"] for s in e1["sources"]}
        self.assertEqual(tables, {"customer_profile", "region"})
        # single-key table
        self.assertEqual(
            next(s for s in e1["sources"] if s["table"] == "customer_profile")["key"],
            {"customer_id": 1})
        # composite key preserved whole, nothing dropped
        rkey = next(s for s in e1["sources"] if s["table"] == "region")["key"]
        self.assertEqual(rkey, {"country": "US", "region_code": "R1"})
        self.assertEqual(set(rkey), {"country", "region_code"})

    def test_only_key_fields_and_published_values_present(self):
        entries = self.lineage_entries("customer_view")
        raw = (self.pkg / "customer_view.row_lineage.jsonl").read_text()
        for e in entries:
            self.assertEqual(set(e), {"row_ref", "values", "sources"})
            for s in e["sources"]:
                self.assertEqual(set(s), {"table", "key"})
            self.assertEqual(set(e["values"]),
                             {"customer_id", "segment", "region_name"})
        # non-key / denied columns never appear anywhere in the file
        for forbidden in ("secret_note", "full_name", "amount"):
            self.assertNotIn(forbidden, raw)

    def test_duplicate_projection_values_remain_separately_locatable(self):
        # segment_view publishes no unique key: customers 2 and 3 produce the
        # exact same projected value set from different source rows
        entries = self.lineage_entries("segment_view")
        dup = [e for e in entries
               if e["values"] == {"segment": "B", "region_name": "North"}]
        self.assertEqual(len(dup), 2)
        custs = sorted(next(s["key"]["customer_id"]
                            for s in e["sources"] if s["table"] == "customer_profile")
                       for e in dup)
        self.assertEqual(custs, [2, 3])
        # distinct mapping identifiers despite identical published values
        self.assertNotEqual(dup[0]["row_ref"], dup[1]["row_ref"])
        self.assertEqual(dup[0]["values"], dup[1]["values"])
        # each is independently locatable offline by its row_ref
        for e in dup:
            hit = lookup_row_lineage(self.pkg, "segment_view",
                                     row_ref=e["row_ref"])
            self.assertEqual(hit[0]["row_ref"], e["row_ref"])
        # a value-based lookup returns BOTH, never conflating them
        hits = lookup_row_lineage(
            self.pkg, "segment_view",
            values={"segment": "B", "region_name": "North"})
        self.assertEqual(len(hits), 2)
        self.assertNotEqual(hits[0]["row_ref"], hits[1]["row_ref"])


class AggregateContributorTests(LineageTestBase):
    def test_aggregate_row_lists_all_and_only_contributors(self):
        entries = self.lineage_entries("region_summary")
        # groups present: R1 (US) and R3 (CN)
        self.assertEqual(len(entries), 2)
        r1 = next(e for e in entries
                  if e["values"]["region_code"] == "R1"
                  and e["values"]["country"] == "US")
        # published aggregate values are bound into the entry
        self.assertEqual(r1["values"]["txn_count"], 4)
        self.assertAlmostEqual(r1["values"]["total_amount"], 100.0)
        txn_ids = sorted(s["key"]["txn_id"] for s in r1["sources"]
                         if s["table"] == "transactions")
        cust_ids = sorted(s["key"]["customer_id"] for s in r1["sources"]
                          if s["table"] == "customer_profile")
        regs = [s for s in r1["sources"] if s["table"] == "region"]
        # ALL and ONLY the contributing transactions (customers 1,2,3)
        self.assertEqual(txn_ids, [101, 102, 103, 104])
        # contributing customers de-duplicated (customer 1 has two txns)
        self.assertEqual(cust_ids, [1, 2, 3])
        # the group's region (composite key)
        self.assertEqual(regs, [{"table": "region",
                                 "key": {"country": "US", "region_code": "R1"}}])
        # the other region's txn/customer must NOT be listed
        self.assertNotIn(105, txn_ids)
        self.assertNotIn(4, cust_ids)

        r3 = next(e for e in entries if e["values"]["region_code"] == "R3")
        self.assertEqual(
            sorted(s["key"]["txn_id"] for s in r3["sources"]
                   if s["table"] == "transactions"), [105])
        self.assertEqual(r3["values"]["txn_count"], 1)

    def test_sources_sorted_and_deduplicated(self):
        for e in self.lineage_entries("region_summary"):
            sig = [(s["table"], canonical(s["key"])) for s in e["sources"]]
            self.assertEqual(sig, sorted(sig))
            self.assertEqual(len(sig), len(set(sig)))


class HandComputedExpectationTests(unittest.TestCase):
    """Expectations enumerated by hand (pure Python); production lineage is
    never used to derive them."""

    @staticmethod
    def expected_customer_view():
        region_by_code = {(c, rc): name for c, rc, name in REGIONS}
        out = []
        for cid, _name, seg, country, rc in CUSTOMERS:
            out.append({
                "values": {"customer_id": cid, "segment": seg,
                           "region_name": region_by_code[(country, rc)]},
                "sources": [
                    {"table": "customer_profile", "key": {"customer_id": cid}},
                    {"table": "region",
                     "key": {"country": country, "region_code": rc}},
                ]})
        return out

    @staticmethod
    def expected_region_summary():
        cust = {cid: (country, rc) for cid, _, _, country, rc in CUSTOMERS}
        groups = {}
        for tid, cid, amt in TRANSACTIONS:
            country, rc = cust[cid]
            g = groups.setdefault((rc, country), {"txn": set(), "cust": set(),
                                                  "sum": 0.0})
            g["txn"].add(tid)
            g["cust"].add(cid)
            g["sum"] += amt
        out = []
        for (rc, country), g in sorted(groups.items()):
            sources = ([{"table": "transactions", "key": {"txn_id": t}}
                        for t in sorted(g["txn"])]
                       + [{"table": "customer_profile",
                           "key": {"customer_id": c}}
                          for c in sorted(g["cust"])]
                       + [{"table": "region",
                           "key": {"country": country, "region_code": rc}}])
            out.append({"group": (rc, country),
                        "values": {"region_code": rc, "country": country,
                                   "txn_count": len(g["txn"]),
                                   "total_amount": g["sum"]},
                        "sources": sources})
        return out

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="hand_test_"))
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.db = self.tmp / "source.duckdb"
        build_source(self.db)
        self.schema = SourceSchema.load(self.db)
        self.con = duckdb.connect(str(self.db), read_only=True)
        self.addCleanup(self.con.close)
        plan = compile_plan(self.schema, AGG_POLICY, LINEAGE_DOC, PROJECTIONS)
        self.pkg, _ = generate(plan, self.db, self.tmp / "packages")

    def _entries(self, dataset):
        ds = next(d for d in json.loads(
            (self.pkg / "manifest.json").read_text())["datasets"]
            if d["name"] == dataset)
        return list(read_entries(self.pkg / ds["row_lineage"]["file"]))

    def test_customer_view_matches_hand_enumeration(self):
        actual = sorted(
            ((e["values"], sorted((s["table"], canonical(s["key"]))
                                  for s in e["sources"]))
             for e in self._entries("customer_view")),
            key=lambda x: x[0]["customer_id"])
        expected = sorted(
            ((e["values"], sorted((s["table"], canonical(s["key"]))
                                  for s in e["sources"]))
             for e in self.expected_customer_view()),
            key=lambda x: x[0]["customer_id"])
        self.assertEqual(actual, expected)

    def test_region_summary_matches_hand_enumeration(self):
        got = { (e["values"]["region_code"], e["values"]["country"]): e
                for e in self._entries("region_summary")}
        for exp in self.expected_region_summary():
            e = got[exp["group"]]
            self.assertEqual(e["values"], exp["values"])
            self.assertEqual(
                sorted((s["table"], canonical(s["key"])) for s in e["sources"]),
                sorted((s["table"], canonical(s["key"])) for s in exp["sources"]))


class RowKeyValidationTests(unittest.TestCase):
    def _build_bad(self, row_keys, *, customers=None, extra_ddl=""):
        tmp = Path(tempfile.mkdtemp(prefix="badkey_"))
        self.addCleanup(shutil.rmtree, tmp, True)
        db = tmp / "bad.duckdb"
        build_source(db, row_keys=row_keys,
                     customers=customers if customers is not None else CUSTOMERS,
                     extra_ddl=extra_ddl)
        return db

    def test_missing_declaration_rejected(self):
        db = self._build_bad({})  # empty declarations for all tables
        with self.assertRaises(RowKeyError):
            SourceSchema.load(db)

    def test_incomplete_declaration_rejected(self):
        db = self._build_bad({"customer_profile": ["customer_id"],
                              "transactions": ["txn_id"]})  # region missing
        with self.assertRaises(RowKeyError):
            SourceSchema.load(db)

    def test_unknown_key_column_rejected(self):
        db = self._build_bad({"customer_profile": ["nope"],
                              "transactions": ["txn_id"],
                              "region": ["country", "region_code"]})
        with self.assertRaises(RowKeyError):
            SourceSchema.load(db)

    def test_null_key_rejected(self):
        # a row whose declared key column customer_id is NULL
        custs = CUSTOMERS + [(None, "Eve", "A", "US", "R1")]
        db = self._build_bad(ROW_KEYS, customers=custs)
        with self.assertRaises(RowKeyError):
            SourceSchema.load(db)

    def test_duplicate_composite_key_rejected(self):
        # repeat an existing (country, region_code) combination
        dup_regions = REGIONS + [("US", "R1", "Duplicate")]
        tmp = Path(tempfile.mkdtemp(prefix="dupkey_"))
        self.addCleanup(shutil.rmtree, tmp, True)
        db = tmp / "dup.duckdb"
        build_source(db, regions=dup_regions)
        with self.assertRaises(RowKeyError):
            SourceSchema.load(db)

    def test_no_row_keys_table_rejected(self):
        tmp = Path(tempfile.mkdtemp(prefix="notable_"))
        self.addCleanup(shutil.rmtree, tmp, True)
        db = tmp / "x.duckdb"
        build_source(db, row_keys={})
        con = duckdb.connect(str(db))
        con.execute("DROP TABLE _row_keys")
        con.close()
        with self.assertRaises(RowKeyError):
            SourceSchema.load(db)


def _write_configs(d, policy=AGG_POLICY):
    d = Path(d)
    d.mkdir(parents=True, exist_ok=True)
    pol = d / "policy.json"
    lin = d / "lineage.json"
    proj = d / "proj.json"
    pol.write_text(json.dumps(policy))
    lin.write_text(json.dumps(LINEAGE_DOC))
    proj.write_text(json.dumps(PROJECTIONS))
    return pol, lin, proj


class VerificationAndTamperTests(LineageTestBase):
    def _verify(self, pkg):
        pol, lin, proj = _write_configs(self.tmp / "cfg")
        return verify_package(pkg, self.db, pol, lin, proj)

    def test_clean_package_verifies(self):
        rep = self._verify(self.pkg)
        failed = [c.name for c in rep.checks if not c.ok]
        self.assertEqual(failed, [], rep.format())

    def _tampered_copy(self, name):
        dst = self.tmp / name
        shutil.copytree(self.pkg, dst)
        return dst

    def test_tampered_key_value_detected(self):
        dst = self._tampered_copy("tamper_key")
        f = dst / "customer_view.row_lineage.jsonl"
        lines = f.read_text().splitlines()
        e = json.loads(lines[0])
        for s in e["sources"]:
            if s["table"] == "customer_profile":
                s["key"]["customer_id"] = 999
        lines[0] = json.dumps(e, separators=(",", ":"))
        f.write_text("\n".join(lines) + "\n")
        rep = self._verify(dst)
        self.assertFalse(rep.ok)
        self.assertTrue(any("lineage_matches_independent_recompute" in c.name
                            and not c.ok for c in rep.checks))

    def test_missing_contributor_detected(self):
        dst = self._tampered_copy("tamper_drop")
        f = dst / "region_summary.row_lineage.jsonl"
        lines = f.read_text().splitlines()
        out = []
        for line in lines:
            e = json.loads(line)
            e["sources"] = [s for s in e["sources"]
                            if not (s["table"] == "transactions"
                                    and s["key"].get("txn_id") == 102)]
            out.append(json.dumps(e, separators=(",", ":")))
        f.write_text("\n".join(out) + "\n")
        rep = self._verify(dst)
        self.assertFalse(rep.ok)
        self.assertTrue(any(not c.ok for c in rep.checks
                            if "lineage_matches_independent_recompute" in c.name))

    def test_added_nonparticipating_row_detected(self):
        dst = self._tampered_copy("tamper_add")
        f = dst / "region_summary.row_lineage.jsonl"
        lines = f.read_text().splitlines()
        e = json.loads(lines[0])
        e["sources"].append({"table": "transactions", "key": {"txn_id": 105}})
        lines[0] = json.dumps(e, separators=(",", ":"))
        f.write_text("\n".join(lines) + "\n")
        rep = self._verify(dst)
        self.assertFalse(rep.ok)

    def test_changed_row_ref_detected(self):
        dst = self._tampered_copy("tamper_ref")
        f = dst / "customer_view.row_lineage.jsonl"
        lines = f.read_text().splitlines()
        e = json.loads(lines[0])
        e["row_ref"] = "rr_" + "0" * 32
        lines[0] = json.dumps(e, separators=(",", ":"))
        f.write_text("\n".join(lines) + "\n")
        rep = self._verify(dst)
        self.assertFalse(rep.ok)
        self.assertTrue(any(not c.ok for c in rep.checks
                            if c.name.endswith("lineage_structure_valid")))

    def test_denied_field_leak_detected(self):
        dst = self._tampered_copy("tamper_leak")
        f = dst / "customer_view.row_lineage.jsonl"
        with open(f, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({
                "row_ref": "rr_" + "1" * 32,
                "values": {"customer_id": 1, "segment": "A",
                           "region_name": "North"},
                "sources": [{"table": "customer_profile",
                             "key": {"customer_id": 1, "secret_note": "LEAK"}}]},
                separators=(",", ":")) + "\n")
        rep = self._verify(dst)
        self.assertFalse(rep.ok)
        self.assertTrue(any(not c.ok for c in rep.checks
                            if "no_forbidden_data" in c.name
                            or "lineage_structure_valid" in c.name))

    def test_digest_tampering_detected(self):
        # rewrite the lineage file without updating the manifest digest
        dst = self._tampered_copy("tamper_digest")
        f = dst / "customer_view.row_lineage.jsonl"
        f.write_text(f.read_text() + "")  # touch content unchanged -> change digest
        # instead append a benign whitespace line then manifest stays stale
        with open(f, "a") as fh:
            fh.write("\n")
        rep = self._verify(dst)
        self.assertFalse(rep.ok)
        self.assertTrue(any(not c.ok for c in rep.checks
                            if c.name.endswith("lineage_present_and_bound")))


class OfflineAndStabilityTests(LineageTestBase):
    def test_lookup_after_source_removed(self):
        # copy the package to an isolated dir and move the source DB away
        isolated = self.tmp / "isolated"
        shutil.copytree(self.pkg, isolated)
        hidden = self.tmp / "source_hidden.duckdb"
        shutil.move(self.db, hidden)
        try:
            hits = lookup_row_lineage(
                isolated, "region_summary",
                values={"region_code": "R3", "country": "CN",
                        "txn_count": 1, "total_amount": 50.0})
            self.assertEqual(len(hits), 1)
            tables = {s["table"] for s in hits[0]["sources"]}
            self.assertEqual(tables, {"transactions", "customer_profile", "region"})
        finally:
            shutil.move(hidden, self.db)

    def test_lineage_stable_across_regeneration_and_insertion_order(self):
        first = (self.pkg / "customer_view.row_lineage.jsonl").read_bytes()
        first_agg = (self.pkg / "region_summary.row_lineage.jsonl").read_bytes()
        # rebuild a second source with shuffled insertion order
        db2 = self.tmp / "source_shuffled.duckdb"
        build_source(db2,
                     region_order=list(range(len(REGIONS)))[::-1],
                     customer_order=list(range(len(CUSTOMERS)))[::-1],
                     txn_order=list(range(len(TRANSACTIONS)))[::-1])
        # identical snapshot content hashes despite different physical order
        s1, s2 = SourceSchema.load(self.db), SourceSchema.load(db2)
        self.assertEqual(s1.table_hashes, s2.table_hashes)
        plan2 = compile_plan(s2, AGG_POLICY, LINEAGE_DOC, PROJECTIONS)
        pkg2, _ = generate(plan2, db2, self.tmp / "packages2")
        self.assertEqual(pkg2.name, self.pkg.name)
        self.assertEqual(
            (pkg2 / "customer_view.row_lineage.jsonl").read_bytes(), first)
        self.assertEqual(
            (pkg2 / "region_summary.row_lineage.jsonl").read_bytes(), first_agg)


class LargeInputStreamingTests(unittest.TestCase):
    def test_bounded_python_memory_on_large_input(self):
        tmp = Path(tempfile.mkdtemp(prefix="large_"))
        self.addCleanup(shutil.rmtree, tmp, True)
        db = tmp / "large.duckdb"
        n_cust, n_txn = 500, 300_000
        big_customers = [
            (i + 1, f"C{i + 1}", "A", "US", "R1") for i in range(n_cust)]
        big_txn = [(1000 + i, i % n_cust + 1, float(i % 1000))
                   for i in range(n_txn)]
        build_source(db, customers=big_customers, transactions=big_txn)
        schema = SourceSchema.load(db)
        con = duckdb.connect(str(db), read_only=True)
        self.addCleanup(con.close)
        # tight DuckDB budget; spilling to disk proves the engine does not
        # rely on holding the source in memory either
        con.execute("PRAGMA memory_limit='128MB'")
        con.execute("PRAGMA threads=2")
        con.execute("SET preserve_insertion_order=false")
        con.execute(f"PRAGMA temp_directory='{tmp / 'spill'}'")
        (tmp / "spill").mkdir()
        plan = compile_plan(schema, AGG_POLICY, LINEAGE_DOC, PROJECTIONS)
        from delivery_compiler.generator import _stage_for
        from delivery_compiler.row_lineage import write_lineage
        ds = next(d for d in plan.datasets if d.name == "region_summary")
        stage = _stage_for(ds, schema)

        tracemalloc.start()
        stats = write_lineage(con, stage, schema.row_keys,
                              tmp / "big.row_lineage.jsonl", batch_size=256)
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        # The source contribution relation holds ~300k rows (tens of MB of
        # Python objects if materialised). A streaming implementation keeps
        # Python peak well below that -- finished JSONL lines are streamed,
        # and even a single giant group is assembled inside DuckDB rather than
        # accumulated as Python dicts. Assert a sub-50MB bound vs ~300k rows.
        self.assertLess(peak, 50 * 1024 * 1024,
                        f"peak Python memory {peak} too high; not streaming")
        # every transaction is represented somewhere (aggregate completeness)
        entries = list(read_entries(tmp / "big.row_lineage.jsonl"))
        total_refs = sum(
            sum(1 for s in e["sources"] if s["table"] == "transactions")
            for e in entries)
        self.assertEqual(total_refs, n_txn)
        self.assertEqual(stats["entries"], len(entries))


if __name__ == "__main__":
    unittest.main(verbosity=2)
