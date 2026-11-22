"""Re-runnable tests for the ``snapshot-correct`` command.

Run from the repository root:

    python3 -m unittest tests.test_snapshot_correct -v

Every test builds its own source snapshot (via the same builder as the demo)
and correction sheets in a fresh temp directory; no large database files are
committed. Coverage:

* the full move scenario (new region + new customer + moved transaction +
  retired region), with independent SQL and hand-computed ownership checks
  against both the source tables and a package generated from the new DB;
* reordering the same batch gives the same business result;
* retiring a region while an *unedited* customer still points at it is
  rejected (the check is whole-database, not edited-row only);
* retained vs explicitly cleared columns;
* duplicate table keys, bad types and baseline digest mismatch never publish;
* two different corrections racing for one target: exactly one complete
  result wins, resending the winner changes nothing;
* an interruption before final publication leaves no half result and a rerun
  completes cleanly;
* streaming keeps correction/source data out of Python memory.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import tracemalloc
import unittest
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from delivery_compiler import (CorrectionError, SourceDriftError, SourceSchema,
                               compile_plan, correct_snapshot, generate,
                               verify_package)
from delivery_compiler.common import load_json
from delivery_compiler.snapshot_correction import (BATCH_SIZE, MARKER_TABLE,
                                                   _atomic_publish)

DEMO_CFG = ROOT / "demo" / "configs"


def sha256_file(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_sheet(path, records):
    Path(path).write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n",
        encoding="utf-8")


def build_demo_source(path, version="v1"):
    """Build the standard demo source (reusing the demo builder)."""
    sys.path.insert(0, str(ROOT))
    from demo.make_source import build
    build(path, version)


class SnapshotCorrectBase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="snapcorr_"))
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.source = self.tmp / "source_v1.duckdb"
        build_demo_source(self.source, "v1")
        self.source_sha = sha256_file(self.source)
        self.target = self.tmp / "corrected.duckdb"

    def correct(self, records, *, version="v3-corrected", target=None,
                sheet_name="corrections.jsonl"):
        sheet = self.tmp / sheet_name
        write_sheet(sheet, records)
        return correct_snapshot(self.source, sheet, self.source_sha,
                                version, target or self.target)

    def open_new(self):
        con = duckdb.connect(str(self.target), read_only=True)
        self.addCleanup(con.close)
        return con

    def open_old(self):
        con = duckdb.connect(str(self.source), read_only=True)
        self.addCleanup(con.close)
        return con


# ---------------------------------------------------------------------------
# 1. the full move scenario
# ---------------------------------------------------------------------------

def move_records(*, new_region="R5", new_customer=13, moved_txn=1000,
                 retired_region="R4"):
    """New region/customer, one transaction moved over, old region retired.

    The v1 demo puts customers 4, 8 and 12 on R4. All three must migrate to
    the new region before R4 can be retired -- the batch deliberately lists
    child rows (the moved transaction) before the parents they will need.
    """
    return [
        # child listed BEFORE the parents it needs
        {"table": "transactions", "key": {"txn_id": moved_txn}, "op": "update",
         "values": {"customer_id": new_customer}},
        {"table": "customer_profile", "key": {"customer_id": 4},
         "op": "update", "values": {"region_code": new_region}},
        {"table": "customer_profile", "key": {"customer_id": 8},
         "op": "update", "values": {"region_code": new_region}},
        {"table": "customer_profile", "key": {"customer_id": 12},
         "op": "update", "values": {"region_code": new_region}},
        {"table": "customer_profile", "key": {"customer_id": new_customer},
         "op": "insert", "values": {
             "full_name": "Mona Moore",
             "contact": {"email": "user13@corp.net", "phone": "+1-555-1013"},
             "address": {"line1": "23 Main St", "city": "Newville",
                         "zip": "02100"},
             "id_number": "GOV-1013", "segment": "A",
             "region_code": new_region,
             "created_at": "2024-03-01T09:00:00"}},
        {"table": "region", "key": {"region_code": new_region}, "op": "insert",
         "values": {"region_name": "Northeast", "country": "US",
                    "tier": "T1"}},
        {"table": "region", "key": {"region_code": retired_region},
         "op": "delete"},
    ]


class FullMoveScenarioTest(SnapshotCorrectBase):
    def setUp(self):
        super().setUp()
        self.records = move_records()
        _, self.status = self.correct(self.records)

    def test_published_and_marker(self):
        self.assertEqual(self.status, "generated")
        self.assertTrue(self.target.is_file())
        con = self.open_new()
        self.assertEqual(
            con.execute("SELECT version FROM _snapshot").fetchall(),
            [("v3-corrected",)])
        marker = con.execute(
            f"SELECT source_sha256, corrections_sha256, new_version "
            f"FROM {MARKER_TABLE}").fetchall()
        self.assertEqual(len(marker), 1)
        self.assertEqual(marker[0][0], self.source_sha)
        self.assertEqual(marker[0][1],
                         sha256_file(self.tmp / "corrections.jsonl"))
        self.assertEqual(marker[0][2], "v3-corrected")

    def test_old_snapshot_untouched(self):
        self.assertEqual(SourceSchema.load(self.source).version, "v1")
        con = self.open_old()
        self.assertEqual(
            con.execute("SELECT count(*) FROM region").fetchone()[0], 4)
        self.assertEqual(sha256_file(self.source), self.source_sha)

    def test_business_rows_via_independent_sql(self):
        new = self.open_new()
        # new region exists; old region gone; the other regions remain
        self.assertEqual(
            new.execute("SELECT region_name, country, tier FROM region "
                        "WHERE region_code = 'R5'").fetchall(),
            [("Northeast", "US", "T1")])
        self.assertEqual(
            new.execute("SELECT count(*) FROM region WHERE region_code='R4'")
            .fetchone()[0], 0)
        self.assertEqual(
            new.execute("SELECT count(*) FROM region").fetchone()[0], 4)
        # new customer with the exact struct values supplied
        self.assertEqual(
            new.execute("SELECT full_name, contact, address, segment, "
                        "region_code FROM customer_profile "
                        "WHERE customer_id = 13").fetchall(),
            [("Mona Moore",
              {"email": "user13@corp.net", "phone": "+1-555-1013"},
              {"line1": "23 Main St", "city": "Newville", "zip": "02100"},
              "A", "R5")])
        # customers 4, 8, 12 migrated off R4
        for cid in (4, 8, 12):
            self.assertEqual(
                new.execute("SELECT region_code FROM customer_profile "
                            f"WHERE customer_id = {cid}").fetchone()[0], "R5")
        # transaction moved
        self.assertEqual(
            new.execute("SELECT customer_id FROM transactions "
                        "WHERE txn_id = 1000").fetchone()[0], 13)
        # row counts: +1 region -1 region, +1 customer, same txn count
        self.assertEqual(
            new.execute("SELECT count(*) FROM customer_profile").fetchone()[0],
            13)
        self.assertEqual(
            new.execute("SELECT count(*) FROM transactions").fetchone()[0], 40)

    def test_hand_computed_ownership(self):
        """Hand-compute every ownership edge and compare with the new DB."""
        new = self.open_new()
        # expected customer -> region map, computed by applying the sheet to
        # the old data in pure Python
        old = self.open_old()
        old_cust = dict((cid, rc) for cid, rc in old.execute(
            "SELECT customer_id, region_code FROM customer_profile").fetchall())
        old_txn = dict((tid, cid) for tid, cid in old.execute(
            "SELECT txn_id, customer_id FROM transactions").fetchall())
        old_regions = {r[0] for r in old.execute(
            "SELECT region_code FROM region").fetchall()}

        exp_cust = dict(old_cust)
        for cid in (4, 8, 12):
            exp_cust[cid] = "R5"
        exp_cust[13] = "R5"
        exp_txn = dict(old_txn)
        exp_txn[1000] = 13
        exp_regions = (old_regions - {"R4"}) | {"R5"}

        got_regions = {r[0] for r in new.execute(
            "SELECT region_code FROM region").fetchall()}
        self.assertEqual(got_regions, exp_regions)
        got_cust = dict(new.execute(
            "SELECT customer_id, region_code FROM customer_profile").fetchall())
        self.assertEqual(got_cust, exp_cust)
        got_txn = dict(new.execute(
            "SELECT txn_id, customer_id FROM transactions").fetchall())
        self.assertEqual(got_txn, exp_txn)

        # every edge exists and is unique; no NULLs
        orphan_cust = new.execute(
            "SELECT count(*) FROM customer_profile c WHERE NOT EXISTS ("
            "SELECT 1 FROM region r WHERE r.region_code = c.region_code)"
        ).fetchone()[0]
        self.assertEqual(orphan_cust, 0)
        orphan_txn = new.execute(
            "SELECT count(*) FROM transactions t WHERE NOT EXISTS ("
            "SELECT 1 FROM customer_profile c "
            "WHERE c.customer_id = t.customer_id)").fetchone()[0]
        self.assertEqual(orphan_txn, 0)
        null_fk = new.execute(
            "SELECT count(*) FROM customer_profile WHERE region_code IS NULL"
        ).fetchone()[0] + new.execute(
            "SELECT count(*) FROM transactions WHERE customer_id IS NULL"
        ).fetchone()[0]
        self.assertEqual(null_fk, 0)

    def test_schema_keys_tags_relations_preserved(self):
        old, new = self.open_old(), self.open_new()
        for table in ("region", "customer_profile", "transactions"):
            self.assertEqual(
                old.execute(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_name=? ORDER BY ordinal_position", [table]).fetchall(),
                new.execute(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_name=? ORDER BY ordinal_position", [table]).fetchall())
        for meta in ("_field_tags", "_relations", "_row_keys"):
            cols = [r[1] for r in old.execute(
                f'PRAGMA table_info("{meta}")').fetchall()]
            order = ", ".join(f'"{c}"' for c in cols)
            self.assertEqual(
                old.execute(f'SELECT * FROM "{meta}" ORDER BY {order}').fetchall(),
                new.execute(f'SELECT * FROM "{meta}" ORDER BY {order}').fetchall())
        # loads through the existing reader
        schema = SourceSchema.load(self.target)
        self.assertEqual(schema.version, "v3-corrected")
        self.assertEqual(sorted(schema.row_keys),
                         ["customer_profile", "region", "transactions"])

    def test_compile_generate_verify_on_new_db(self):
        schema = SourceSchema.load(self.target)
        policy = load_json(DEMO_CFG / "policy_marketing.json")
        lineage = load_json(DEMO_CFG / "lineage.json")
        proj = load_json(DEMO_CFG / "proj_marketing.json")
        plan = compile_plan(schema, policy, lineage, proj)
        pkg_root = self.tmp / "packages"
        pkg, gen_status = generate(plan, self.target, pkg_root)
        self.assertEqual(gen_status, "generated")
        # the marketing customers dataset must contain the new customer and
        # the moved transaction must appear under the new customer's region
        report = verify_package(
            pkg, self.target,
            DEMO_CFG / "policy_marketing.json",
            DEMO_CFG / "lineage.json",
            DEMO_CFG / "proj_marketing.json")
        self.assertTrue(report.ok, report.format())
        con = duckdb.connect()
        try:
            customers = con.execute(
                f"SELECT customer_id, region_label FROM read_parquet('{pkg}/customers.parquet')"
            ).fetchall()
        finally:
            con.close()
        self.assertIn((13, "R5-Northeast"), customers)
        # migrated customers' region label changed with the migration
        for cid in (4, 8, 12):
            self.assertIn((cid, "R5-Northeast"), customers)

    def test_reordered_batch_same_business_result(self):
        reordered = list(reversed(self.records))
        target2 = self.tmp / "reordered.duckdb"
        sheet2 = self.tmp / "reordered.jsonl"
        write_sheet(sheet2, reordered)
        _, st = correct_snapshot(self.source, sheet2, self.source_sha,
                                 "v3-corrected", target2)
        self.assertEqual(st, "generated")
        a, b = self.open_new(), duckdb.connect(str(target2), read_only=True)
        self.addCleanup(b.close)
        for table in ("region", "customer_profile", "transactions"):
            cols = [r[0] for r in a.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name=? ORDER BY ordinal_position", [table]).fetchall()]
            order = ", ".join(f'"{c}"' for c in cols)
            self.assertEqual(
                a.execute(f'SELECT {order} FROM "{table}" ORDER BY {order}').fetchall(),
                b.execute(f'SELECT {order} FROM "{table}" ORDER BY {order}').fetchall(),
                table)

    def test_retry_is_idempotent_and_does_not_rewrite(self):
        before = self.target.stat()
        _, st = self.correct(self.records)
        after = self.target.stat()
        self.assertEqual(st, "exists-identical")
        self.assertEqual(before.st_ino, after.st_ino)
        self.assertEqual(before.st_size, after.st_size)
        self.assertEqual(sha256_file(self.target),
                         hashlib.sha256(
                             self.target.read_bytes()).hexdigest())

    def test_different_request_same_target_conflicts(self):
        other = [{"table": "region", "key": {"region_code": "R8"},
                  "op": "insert",
                  "values": {"region_name": "Other", "country": "CN",
                             "tier": "T2"}}]
        with self.assertRaises(CorrectionError):
            self.correct(other, version="v4-other",
                         target=self.target, sheet_name="other.jsonl")
        # original file unchanged
        self.assertEqual(SourceSchema.load(self.target).version,
                         "v3-corrected")


# ---------------------------------------------------------------------------
# 2. unedited child blocks parent retirement
# ---------------------------------------------------------------------------

class UneditedChildIntegrityTest(SnapshotCorrectBase):
    def test_retire_region_with_unedited_customer_rejected(self):
        # R1 has three customers; move only one, then try to retire R1.
        records = [
            {"table": "customer_profile", "key": {"customer_id": 1},
             "op": "update", "values": {"region_code": "R2"}},
            {"table": "region", "key": {"region_code": "R1"},
             "op": "delete"},
        ]
        with self.assertRaises(CorrectionError) as ctx:
            self.correct(records)
        self.assertIn("cust_region", str(ctx.exception))
        self.assertFalse(self.target.exists())

    def test_delete_customer_with_unedited_transaction_rejected(self):
        records = [
            # customer 1 owns transactions 1000, 1011, ... (i % 11 + 1 == 1)
            {"table": "customer_profile", "key": {"customer_id": 1},
             "op": "delete"},
        ]
        with self.assertRaises(CorrectionError) as ctx:
            self.correct(records)
        self.assertIn("txn_cust", str(ctx.exception))
        self.assertFalse(self.target.exists())

    def test_null_foreign_key_rejected_even_on_insert(self):
        records = [
            {"table": "customer_profile", "key": {"customer_id": 99},
             "op": "insert", "values": {
                 "full_name": "Ghost",
                 "contact": {"email": "g@x.io", "phone": None},
                 "address": {"line1": None, "city": None, "zip": None},
                 "id_number": "G-X", "segment": "A", "region_code": None,
                 "created_at": "2024-03-01T09:00:00"}},
        ]
        with self.assertRaises(CorrectionError):
            self.correct(records)
        self.assertFalse(self.target.exists())


# ---------------------------------------------------------------------------
# 3. retained vs explicitly cleared columns
# ---------------------------------------------------------------------------

class RetainAndNullTest(SnapshotCorrectBase):
    def test_omitted_column_retained_explicit_null_clears(self):
        old = self.open_old()
        before = old.execute(
            "SELECT full_name, segment, id_number, contact FROM "
            "customer_profile WHERE customer_id = 1").fetchone()
        records = [
            # segment omitted -> retained; id_number explicitly nulled
            {"table": "customer_profile", "key": {"customer_id": 1},
             "op": "update", "values": {"id_number": None}},
            # whole-struct replacement with an explicit null member
            {"table": "customer_profile", "key": {"customer_id": 2},
             "op": "update", "values": {
                 "contact": {"email": "bob@example.com", "phone": None}}},
        ]
        _, st = self.correct(records)
        self.assertEqual(st, "generated")
        new = self.open_new()
        full_name, segment, id_number, contact = new.execute(
            "SELECT full_name, segment, id_number, contact FROM "
            "customer_profile WHERE customer_id = 1").fetchone()
        self.assertEqual(full_name, before[0])      # retained
        self.assertEqual(segment, before[1])        # retained
        self.assertIsNone(id_number)                # explicitly cleared
        # untouched struct on row 1 retained byte-for-byte
        self.assertEqual(
            new.execute("SELECT contact FROM customer_profile "
                        "WHERE customer_id = 1").fetchone()[0], before[3])
        # row 2: whole struct replaced, email changed, phone cleared
        self.assertEqual(
            new.execute("SELECT contact FROM customer_profile "
                        "WHERE customer_id = 2").fetchone()[0],
            {"email": "bob@example.com", "phone": None})


# ---------------------------------------------------------------------------
# 4. invalid corrections never publish
# ---------------------------------------------------------------------------

class InvalidCorrectionTest(SnapshotCorrectBase):
    def _expect_no_publish(self, records, *, sha=None, version="v3-bad",
                           error=CorrectionError):
        sheet = self.tmp / "bad.jsonl"
        write_sheet(sheet, records)
        target = self.tmp / "bad.duckdb"
        with self.assertRaises(error):
            correct_snapshot(self.source, sheet,
                             sha or self.source_sha, version, target)
        self.assertFalse(target.exists())
        # no staging directory left behind
        leftovers = [p for p in self.tmp.iterdir()
                     if p.name.startswith(".snapshot_correct_")]
        self.assertEqual(leftovers, [])

    def test_duplicate_table_key(self):
        rec = {"table": "region", "key": {"region_code": "R9"},
               "op": "insert",
               "values": {"region_name": "X", "country": "US", "tier": "T1"}}
        self._expect_no_publish([rec, rec])

    def test_duplicate_key_across_batches(self):
        # more than BATCH_SIZE lines so the duplicate is found via the DB
        # constraint, not only the in-process per-batch check
        recs = []
        for i in range(BATCH_SIZE + 5):
            recs.append({"table": "transactions",
                         "key": {"txn_id": 9000 + i}, "op": "delete"})
        recs.append({"table": "transactions", "key": {"txn_id": 9000},
                     "op": "delete"})
        self._expect_no_publish(recs)

    def test_numeric_string_rejected(self):
        self._expect_no_publish([
            {"table": "customer_profile", "key": {"customer_id": "77"},
             "op": "delete"}])

    def test_bad_timestamp(self):
        self._expect_no_publish([
            {"table": "transactions", "key": {"txn_id": 7777},
             "op": "update",
             "values": {"txn_time": "2024-13-40 99:99:99"}}])

    def test_baseline_digest_mismatch(self):
        rec = [{"table": "region", "key": {"region_code": "R9"},
                "op": "insert",
                "values": {"region_name": "X", "country": "US",
                           "tier": "T1"}}]
        self._expect_no_publish(rec, sha="00" * 32,
                                error=SourceDriftError)

    def test_unknown_table_column_op_and_key_change(self):
        self._expect_no_publish([
            {"table": "widgets", "key": {"id": 1}, "op": "delete"}])
        self._expect_no_publish([
            {"table": "region", "key": {"region_code": "R1"},
             "op": "update", "values": {"region_code": "R9"}}])
        self._expect_no_publish([
            {"table": "region", "key": {"region_code": "R9"},
             "op": "explode",
             "values": {"region_name": "X", "country": "US", "tier": "T1"}}])

    def test_struct_member_mismatch(self):
        self._expect_no_publish([
            {"table": "customer_profile", "key": {"customer_id": 1},
             "op": "update",
             "values": {"contact": {"email": "a@b.c"}}}])

    def test_insert_missing_column_and_delete_with_values(self):
        self._expect_no_publish([
            {"table": "region", "key": {"region_code": "R9"},
             "op": "insert",
             "values": {"region_name": "X", "country": "US"}}])
        self._expect_no_publish([
            {"table": "region", "key": {"region_code": "R1"},
             "op": "delete", "values": {"region_name": "X"}}])


# ---------------------------------------------------------------------------
# 5. publication boundary: races, idempotent resend, interruption
# ---------------------------------------------------------------------------

class PublicationBoundaryTest(SnapshotCorrectBase):
    def test_two_different_requests_race_one_target(self):
        sheet_a = self.tmp / "a.jsonl"
        sheet_b = self.tmp / "b.jsonl"
        write_sheet(sheet_a, [
            {"table": "region", "key": {"region_code": "RA"},
             "op": "insert",
             "values": {"region_name": "A", "country": "US", "tier": "T1"}},
            {"table": "customer_profile", "key": {"customer_id": 51},
             "op": "insert", "values": {
                 "full_name": "A", "contact": {"email": "a@x.io",
                                               "phone": "p"},
                 "address": {"line1": "l", "city": "c", "zip": "z"},
                 "id_number": "G-A", "segment": "A", "region_code": "RA",
                 "created_at": "2024-03-01T09:00:00"}}])
        write_sheet(sheet_b, [
            {"table": "region", "key": {"region_code": "RB"},
             "op": "insert",
             "values": {"region_name": "B", "country": "CN", "tier": "T2"}},
            {"table": "customer_profile", "key": {"customer_id": 52},
             "op": "insert", "values": {
                 "full_name": "B", "contact": {"email": "b@x.io",
                                               "phone": "p"},
                 "address": {"line1": "l", "city": "c", "zip": "z"},
                 "id_number": "G-B", "segment": "B", "region_code": "RB",
                 "created_at": "2024-03-01T09:00:00"}}])
        # launch several processes per request CONCURRENTLY so they genuinely
        # race for the same target path's atomic hard-link publish
        procs = []
        for _ in range(3):
            procs.append(subprocess.Popen(
                [sys.executable, "-m", "delivery_compiler", "snapshot-correct",
                 "--source", str(self.source), "--source-sha256", self.source_sha,
                 "--corrections", str(sheet_a), "--new-version", "vA",
                 "--out", str(self.target)],
                cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True))
            procs.append(subprocess.Popen(
                [sys.executable, "-m", "delivery_compiler", "snapshot-correct",
                 "--source", str(self.source), "--source-sha256", self.source_sha,
                 "--corrections", str(sheet_b), "--new-version", "vB",
                 "--out", str(self.target)],
                cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True))
        results = []
        for p in procs:
            rc = p.wait()
            stdout, stderr = p.stdout.read(), p.stderr.read()
            p.stdout.close()
            p.stderr.close()
            results.append((rc, stdout, stderr))
        successes = [r for r in results if r[0] == 0]
        failures = [r for r in results if r[0] != 0]
        self.assertTrue(successes)
        self.assertTrue(failures)
        versions = {json.loads(stdout)["new_version"]
                    for _, stdout, _ in successes}
        self.assertEqual(len(versions), 1)
        winner = next(iter(versions))
        for _, _, stderr in failures:
            self.assertIn("occupied", stderr)
        # one complete, valid database remains
        schema = SourceSchema.load(self.target)
        self.assertEqual(schema.version, winner)
        con = self.open_new()
        self.assertEqual(
            con.execute("SELECT count(*) FROM region").fetchone()[0], 5)
        if winner == "vA":
            self.assertTrue(con.execute(
                "SELECT 1 FROM region WHERE region_code='RA'").fetchone())
            self.assertFalse(con.execute(
                "SELECT 1 FROM region WHERE region_code='RB'").fetchone())
        else:
            self.assertTrue(con.execute(
                "SELECT 1 FROM region WHERE region_code='RB'").fetchone())
            self.assertFalse(con.execute(
                "SELECT 1 FROM region WHERE region_code='RA'").fetchone())

    def test_resend_winner_does_not_change_file(self):
        records = move_records()
        _, st1 = self.correct(records)
        self.assertEqual(st1, "generated")
        digest_before = sha256_file(self.target)
        inode_before = self.target.stat().st_ino
        _, st2 = self.correct(records)
        self.assertEqual(st2, "exists-identical")
        self.assertEqual(sha256_file(self.target), digest_before)
        self.assertEqual(self.target.stat().st_ino, inode_before)

    def test_interruption_before_publish_then_rerun_completes(self):
        import delivery_compiler.snapshot_correction as mod
        records = move_records()
        calls = {"n": 0}
        real_publish = mod._atomic_publish

        def flaky(staging, target, marker):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("simulated crash before publication")
            return real_publish(staging, target, marker)

        mod._atomic_publish = flaky
        try:
            with self.assertRaises(RuntimeError):
                self.correct(records)
        finally:
            mod._atomic_publish = real_publish
        # nothing visible at the boundary, no leftover staging dir
        self.assertFalse(self.target.exists())
        self.assertEqual(
            [p for p in self.tmp.iterdir()
             if p.name.startswith(".snapshot_correct_")], [])
        # rerun completes
        _, st = self.correct(records)
        self.assertEqual(st, "generated")
        schema = SourceSchema.load(self.target)
        self.assertEqual(schema.version, "v3-corrected")
        # and is immediately recognised on another retry
        _, st2 = self.correct(records)
        self.assertEqual(st2, "exists-identical")

    def test_publish_primitive_never_clobbers(self):
        staging = self.tmp / "stage.duckdb"
        shutil.copyfile(self.source, staging)
        con = duckdb.connect(str(staging))
        con.execute(f"CREATE TABLE {MARKER_TABLE} (source_sha256 VARCHAR, "
                    "corrections_sha256 VARCHAR, new_version VARCHAR)")
        con.execute(f"INSERT INTO {MARKER_TABLE} VALUES (?, ?, ?)",
                    ["a", "b", "v1"])
        con.close()
        target = self.tmp / "prim.duckdb"
        marker = {"source_sha256": "a", "corrections_sha256": "b",
                  "new_version": "v1"}
        self.assertEqual(_atomic_publish(staging, target, marker),
                         "generated")
        # identical marker -> recognised, no rewrite
        inode = target.stat().st_ino
        self.assertEqual(_atomic_publish(staging, target, marker),
                         "exists-identical")
        self.assertEqual(target.stat().st_ino, inode)
        # different marker -> conflict
        with self.assertRaises(CorrectionError):
            _atomic_publish(staging, target,
                            {**marker, "new_version": "v2"})


# ---------------------------------------------------------------------------
# 6. bounded memory: nothing accumulates in Python
# ---------------------------------------------------------------------------

class BoundedMemoryTest(SnapshotCorrectBase):
    def test_large_batch_streams_with_bounded_python_memory(self):
        # build a bigger source so sheet + source are well above the bound
        big_src = self.tmp / "big.duckdb"
        con = duckdb.connect(str(big_src))
        con.execute("CREATE TABLE region (region_code VARCHAR, region_name "
                    "VARCHAR, country VARCHAR, tier VARCHAR)")
        con.execute("CREATE TABLE customer_profile (customer_id BIGINT, "
                    "full_name VARCHAR, contact STRUCT(email VARCHAR, "
                    "phone VARCHAR), address STRUCT(line1 VARCHAR, city "
                    "VARCHAR, zip VARCHAR), id_number VARCHAR, segment "
                    "VARCHAR, region_code VARCHAR, created_at TIMESTAMP)")
        con.execute("CREATE TABLE transactions (txn_id BIGINT, customer_id "
                    "BIGINT, txn_time TIMESTAMP, amount DOUBLE, currency "
                    "VARCHAR, merchant_id VARCHAR, channel VARCHAR)")
        n_regions, n_customers, n_txns = 20, 2_000, 20_000
        con.executemany(
            "INSERT INTO region VALUES (?, ?, ?, ?)",
            [(f"R{i}", f"Region {i}", "US", "T1")
             for i in range(n_regions)])
        con.executemany(
            "INSERT INTO customer_profile VALUES (?, ?, "
            "{'email': ?, 'phone': ?}, {'line1': ?, 'city': ?, 'zip': ?}, "
            "?, ?, ?, ?::TIMESTAMP)",
            [(i, f"C{i}", f"u{i}@x.io", "+1", f"{i} Main", "City", "0",
              f"G{i}", "A", f"R{i % n_regions}", "2024-01-01 09:00:00")
             for i in range(n_customers)])
        con.executemany(
            "INSERT INTO transactions VALUES (?, ?, ?::TIMESTAMP, ?, ?, ?, ?)",
            [(1000 + i, i % n_customers, "2024-02-01 10:00:00",
              float(i), "USD", "M1", "web") for i in range(n_txns)])
        for ddl, rows in [
                ("CREATE TABLE _snapshot (version VARCHAR)", [("big-v1",)]),
                ("CREATE TABLE _field_tags (path VARCHAR, tag VARCHAR)", []),
                ("CREATE TABLE _relations (name VARCHAR, from_table VARCHAR, "
                 "from_column VARCHAR, to_table VARCHAR, to_column VARCHAR)",
                 [("txn_cust", "transactions", "customer_id",
                   "customer_profile", "customer_id"),
                  ("cust_region", "customer_profile", "region_code",
                   "region", "region_code")]),
                ("CREATE TABLE _row_keys (table_name VARCHAR, column_name "
                 "VARCHAR, key_ordinal INTEGER)",
                 [("customer_profile", "customer_id", 0),
                  ("transactions", "txn_id", 0),
                  ("region", "region_code", 0)])]:
            con.execute(ddl)
            if rows:
                con.executemany(
                    f"INSERT INTO {ddl.split()[2]} VALUES "
                    f"({', '.join('?' for _ in rows[0])})", rows)
        con.close()
        big_sha = sha256_file(big_src)

        # 5,000 deletes + 5,000 updates across the batch boundaries
        records = [{"table": "transactions", "key": {"txn_id": 1000 + i},
                    "op": "delete"} for i in range(5_000)]
        records += [
            {"table": "transactions", "key": {"txn_id": 15_000 + i},
             "op": "update",
             "values": {"channel": "pos"}} for i in range(5_000)]
        sheet = self.tmp / "big_corrections.jsonl"
        write_sheet(sheet, records)
        target = self.tmp / "big_corrected.duckdb"

        tracemalloc.start()
        _, st = correct_snapshot(big_src, sheet, big_sha, "big-v2", target)
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        self.assertEqual(st, "generated")
        # 10k correction records plus a 20k-row source would be hundreds of MB
        # if accumulated; streaming keeps the Python peak tiny.
        self.assertLess(peak, 40 * 1024 * 1024,
                        f"peak Python memory {peak} too high; not streaming")
        check = duckdb.connect(str(target), read_only=True)
        try:
            self.assertEqual(
                check.execute("SELECT count(*) FROM transactions").fetchone()[0],
                n_txns - 5_000)
            self.assertEqual(
                check.execute(
                    "SELECT count(*) FROM transactions WHERE "
                    "txn_id >= 15000 AND txn_id < 20000 AND channel = 'pos'"
                ).fetchone()[0], 5_000)
        finally:
            check.close()


if __name__ == "__main__":
    unittest.main()
