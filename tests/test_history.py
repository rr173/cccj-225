"""Re-runnable tests for the consumer-side customer history store.

Run from the repository root:

    python3 -m unittest discover -s tests -v

Every test builds its own delivery packages (manifest + parquet) in a fresh
temp directory; the history store only ever reads those packages, never the
source lake.
"""
from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from delivery_compiler import (HistoryError, history_connect, load_package,
                               show_history)

ALL_COLUMNS = [("customer_id", "BIGINT"), ("city", "VARCHAR"),
               ("segment", "VARCHAR"), ("region_label", "VARCHAR")]

# roster states used across the scenario
SH = {"present": True, "city": "Shanghai", "segment": "A", "region_label": "CN-E"}
BJ = {"present": True, "city": "Beijing", "segment": "A", "region_label": "CN-N"}
TJ = {"present": True, "city": "Tianjin", "segment": "B", "region_label": "CN-N2"}
NULLS = {"present": True, "city": None, "segment": None, "region_label": None}
ABSENT = {"present": False}


def iv(start, end, **state):
    return {"start": start, "end": end, **state}


def make_package(root, name, rows, *, fmt="delivery-package/1",
                 consumer="marketing", columns=ALL_COLUMNS, datasets=None,
                 write_parquet=True, parquet_name="customers.parquet"):
    """Fabricate an already-delivered package (manifest + parquet)."""
    pkg = Path(root) / name
    pkg.mkdir(parents=True)
    if write_parquet:
        con = duckdb.connect()
        try:
            cols = ", ".join(f"{c} {t}" for c, t in columns)
            con.execute(f"CREATE TABLE t ({cols})")
            if rows:
                marks = ", ".join("?" for _ in columns)
                con.executemany(f"INSERT INTO t VALUES ({marks})", rows)
            con.execute(f"COPY t TO '{pkg / parquet_name}' (FORMAT PARQUET)")
        finally:
            con.close()
    if datasets is None:
        datasets = [{"name": "customers", "file": parquet_name}]
    manifest = {"format": fmt, "package_id": f"pkg_{name}", "consumer": consumer,
                "purpose": "analytics", "datasets": datasets}
    (pkg / "manifest.json").write_text(json.dumps(manifest, indent=2),
                                       encoding="utf-8")
    return pkg


class TempDirTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="hist_test_"))
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.db = self.tmp / "history.duckdb"
        self.pkgs = self.tmp / "packages"
        self.pkgs.mkdir()

    def open(self, **kw):
        con = history_connect(self.db, **kw)
        self.addCleanup(con.close)
        return con


class ReportReproductionScenarioTest(TempDirTest):
    """The month-end report reproduction sample:

    ingest days 1/3/5, backfill day 2, then correct day 2.  Customer 1001
    gets a short interval cut into an old one, then merged again; customer
    1002 exercises "present with all-NULL attributes" vs "not in roster".
    """

    def test_scenario(self):
        con = self.open()
        W = ("2024-01-01", "2024-01-06")

        d1r1 = make_package(self.pkgs, "d1r1",
                            [(1001, "Shanghai", "A", "CN-E"),
                             (1002, None, None, None)])
        d3r1 = make_package(self.pkgs, "d3r1", [(1001, "Shanghai", "A", "CN-E")])
        d5r1 = make_package(self.pkgs, "d5r1",
                            [(1001, "Shanghai", "A", "CN-E"),
                             (1002, "Tianjin", "B", "CN-N2")])
        d2r1 = make_package(self.pkgs, "d2r1", [(1001, "Beijing", "A", "CN-N")])
        d2r2 = make_package(self.pkgs, "d2r2",
                            [(1001, "Shanghai", "A", "CN-E"),
                             (1002, None, None, None)])

        # -- ingest 1, 3, 5 -------------------------------------------------
        self.assertEqual(load_package(con, d1r1, "2024-01-01", 1)["seq"], 1)
        self.assertEqual(load_package(con, d3r1, "2024-01-03", 1)["seq"], 2)
        self.assertEqual(load_package(con, d5r1, "2024-01-05", 1)["seq"], 3)

        # before the backfill: one interval; unchanged days do not split it
        exp_1001_u3 = [iv("2024-01-01", "2024-01-06", **SH)]
        exp_1002_u3 = [iv("2024-01-01", "2024-01-03", **NULLS),
                       iv("2024-01-03", "2024-01-05", **ABSENT),
                       iv("2024-01-05", "2024-01-06", **TJ)]
        self.assertEqual(show_history(con, 1001, *W, upto=3)["intervals"],
                         exp_1001_u3)
        self.assertEqual(show_history(con, 1002, *W, upto=3)["intervals"],
                         exp_1002_u3)

        # -- backfill day 2 (out-of-order arrival) ---------------------------
        self.assertEqual(load_package(con, d2r1, "2024-01-02", 1)["seq"], 4)
        exp_1001_u4 = [iv("2024-01-01", "2024-01-02", **SH),
                       iv("2024-01-02", "2024-01-03", **BJ),
                       iv("2024-01-03", "2024-01-06", **SH)]
        exp_1002_u4 = [iv("2024-01-01", "2024-01-02", **NULLS),
                       iv("2024-01-02", "2024-01-05", **ABSENT),
                       iv("2024-01-05", "2024-01-06", **TJ)]
        self.assertEqual(show_history(con, 1001, *W, upto=4)["intervals"],
                         exp_1001_u4)
        self.assertEqual(show_history(con, 1002, *W, upto=4)["intervals"],
                         exp_1002_u4)
        # the backfill must not rewrite what older sequence numbers saw
        self.assertEqual(show_history(con, 1001, *W, upto=3)["intervals"],
                         exp_1001_u3)
        self.assertEqual(show_history(con, 1002, *W, upto=3)["intervals"],
                         exp_1002_u3)

        # -- correct day 2 (higher revision) ---------------------------------
        self.assertEqual(load_package(con, d2r2, "2024-01-02", 2)["seq"], 5)
        exp_1001_u5 = [iv("2024-01-01", "2024-01-06", **SH)]
        exp_1002_u5 = [iv("2024-01-01", "2024-01-03", **NULLS),
                       iv("2024-01-03", "2024-01-05", **ABSENT),
                       iv("2024-01-05", "2024-01-06", **TJ)]
        self.assertEqual(show_history(con, 1001, *W, upto=5)["intervals"],
                         exp_1001_u5)
        self.assertEqual(show_history(con, 1002, *W, upto=5)["intervals"],
                         exp_1002_u5)
        self.assertEqual(show_history(con, 1001, *W, upto=4)["intervals"],
                         exp_1001_u4)

        # -- a completely empty day package ----------------------------------
        d8r1 = make_package(self.pkgs, "d8r1", [])
        r = load_package(con, d8r1, "2024-01-08", 1)
        self.assertEqual((r["seq"], r["rows"]), (6, 0))
        self.assertEqual(
            show_history(con, 1001, "2024-01-06", "2024-01-10")["intervals"],
            [iv("2024-01-06", "2024-01-08", **SH),
             iv("2024-01-08", "2024-01-10", **ABSENT)])

        # -- save + reopen: queries need no original packages ----------------
        con.close()
        # keep d2r1 (resent below); delete the other packages to prove that
        # queries rely only on the history database
        for d in ("d1r1", "d3r1", "d5r1", "d2r2", "d8r1"):
            shutil.rmtree(self.pkgs / d)
        con = self.open(read_only=True)
        self.assertEqual(show_history(con, 1001, *W, upto=5)["intervals"],
                         exp_1001_u5)
        con.close()

        # resend the superseded revision byte-identically: original seq, no-op
        con = self.open()
        r = load_package(con, self.pkgs / "d2r1", "2024-01-02", 1)
        self.assertEqual((r["seq"], r["status"]), (4, "duplicate"))
        self.assertEqual(show_history(con, 1001, *W)["intervals"], exp_1001_u5)

        # a higher revision with duplicate primary keys is rejected
        d2r3_bad = make_package(self.pkgs, "d2r3",
                                [(1001, "Shanghai", "A", "CN-E"),
                                 (1001, "Shanghai", "A", "CN-E")])
        with self.assertRaises(HistoryError):
            load_package(con, d2r3_bad, "2024-01-02", 3)

        # the rejection consumed no sequence number and changed nothing
        d9r1 = make_package(self.pkgs, "d9r1", [(1001, "Shanghai", "A", "CN-E")])
        self.assertEqual(load_package(con, d9r1, "2024-01-09", 1)["seq"], 7)
        self.assertEqual(show_history(con, 1001, *W, upto=3)["intervals"],
                         exp_1001_u3)
        self.assertEqual(show_history(con, 1001, *W, upto=4)["intervals"],
                         exp_1001_u4)
        self.assertEqual(show_history(con, 1001, *W, upto=5)["intervals"],
                         exp_1001_u5)
        self.assertEqual(show_history(con, 1002, *W, upto=5)["intervals"],
                         exp_1002_u5)
        self.assertEqual(
            show_history(con, 1001, "2024-01-06", "2024-01-10")["intervals"],
            [iv("2024-01-06", "2024-01-08", **SH),
             iv("2024-01-08", "2024-01-09", **ABSENT),
             iv("2024-01-09", "2024-01-10", **SH)])
        self.assertEqual(show_history(con, 1002, *W)["upto"], 7)


class RevisionRulesTest(TempDirTest):
    def test_revision_conflicts(self):
        con = self.open()
        d1r2 = make_package(self.pkgs, "d1r2", [(1001, "Shanghai", "A", "CN-E")])
        self.assertEqual(load_package(con, d1r2, "2024-01-01", 2)["seq"], 1)

        # byte-identical resend returns the original seq and changes nothing
        again = load_package(con, d1r2, "2024-01-01", 2)
        self.assertEqual((again["seq"], again["status"]), (1, "duplicate"))

        # same revision, different content -> error
        d1r2_alt = make_package(self.pkgs, "d1r2_alt",
                                [(1001, "Beijing", "A", "CN-N")])
        with self.assertRaises(HistoryError):
            load_package(con, d1r2_alt, "2024-01-01", 2)

        # never-ingested revision below the current one -> error
        d1r1 = make_package(self.pkgs, "d1r1", [(1001, "Shanghai", "A", "CN-E")])
        with self.assertRaises(HistoryError):
            load_package(con, d1r1, "2024-01-01", 1)

        # a higher revision is accepted and gets the next seq
        d1r3 = make_package(self.pkgs, "d1r3", [(1002, "Tianjin", "B", "CN-N2")])
        self.assertEqual(load_package(con, d1r3, "2024-01-01", 3)["seq"], 2)
        self.assertEqual(
            show_history(con, 1001, "2024-01-01", "2024-01-03")["intervals"],
            [iv("2024-01-01", "2024-01-03", **ABSENT)])
        # ... but the old revision still explains cutoff 1
        self.assertEqual(
            show_history(con, 1001, "2024-01-01", "2024-01-03", upto=1)["intervals"],
            [iv("2024-01-01", "2024-01-03", **SH)])

    def test_bad_revision_and_date_arguments(self):
        con = self.open()
        pkg = make_package(self.pkgs, "p", [(1001, "Shanghai", "A", "CN-E")])
        for bad in (0, -1, "x"):
            with self.assertRaises(HistoryError):
                load_package(con, pkg, "2024-01-01", bad)
        for bad in ("2024-1-1", "20240101", "2024-02-30", "01/02/2024"):
            with self.assertRaises(HistoryError):
                load_package(con, pkg, bad, 1)
        # nothing was ingested
        self.assertEqual(show_history(con, 1001, "2024-01-01", "2024-01-02",
                                      upto=0)["intervals"],
                         [iv("2024-01-01", "2024-01-02", **ABSENT)])


class BindingAndColumnChecksTest(TempDirTest):
    """Only the checks this feature needs: bindings, files, actual column
    types, primary key.  Failures consume no sequence number."""

    def assert_rejected(self, pkg, date="2024-01-01", rev=1):
        con = self.open()
        with self.assertRaises(HistoryError):
            load_package(con, pkg, date, rev)
        # a subsequent valid load still gets seq 1: nothing was consumed
        good = make_package(self.pkgs, "good", [(1001, "Shanghai", "A", "CN-E")])
        self.assertEqual(load_package(con, good, "2024-01-02", 1)["seq"], 1)

    def test_bad_format(self):
        self.assert_rejected(make_package(self.pkgs, "p", [], fmt="bogus/9"))

    def test_wrong_consumer(self):
        self.assert_rejected(make_package(self.pkgs, "p", [], consumer="fraud"))

    def test_no_customers_dataset(self):
        self.assert_rejected(make_package(
            self.pkgs, "p", [], datasets=[{"name": "amount_by_region",
                                           "file": "customers.parquet"}]))

    def test_missing_parquet_file(self):
        self.assert_rejected(make_package(self.pkgs, "p", [], write_parquet=False))

    def test_wrong_customer_id_type(self):
        cols = [("customer_id", "INTEGER")] + ALL_COLUMNS[1:]
        self.assert_rejected(make_package(self.pkgs, "p", [(1, "A", "B", "C")],
                                          columns=cols))

    def test_missing_column(self):
        cols = [c for c in ALL_COLUMNS if c[0] != "city"]
        self.assert_rejected(make_package(self.pkgs, "p", [(1, "B", "C")],
                                          columns=cols))

    def test_null_customer_id(self):
        self.assert_rejected(make_package(self.pkgs, "p", [(None, "A", "B", "C")]))

    def test_duplicate_customer_id(self):
        self.assert_rejected(make_package(
            self.pkgs, "p", [(1001, "A", "B", "C"), (1001, "D", "E", "F")]))

    def test_other_datasets_are_never_read(self):
        # manifest lists a second dataset whose file does not even exist;
        # the load must succeed because only the customers parquet is read
        pkg = make_package(
            self.pkgs, "p", [(1001, "Shanghai", "A", "CN-E")],
            datasets=[{"name": "customers", "file": "customers.parquet"},
                      {"name": "amount_by_region",
                       "file": "amount_by_region.parquet"}])
        con = self.open()
        self.assertEqual(load_package(con, pkg, "2024-01-01", 1)["seq"], 1)


class EmptyPackageTest(TempDirTest):
    def test_empty_packages(self):
        con = self.open()
        # a completely empty package is legal, also as the very first one
        empty1 = make_package(self.pkgs, "e1", [])
        r = load_package(con, empty1, "2024-01-01", 1)
        self.assertEqual((r["seq"], r["rows"]), (1, 0))

        full = make_package(self.pkgs, "full", [(1001, "Shanghai", "A", "CN-E")])
        load_package(con, full, "2024-01-02", 1)
        # an empty higher revision empties the roster from that date on
        empty2 = make_package(self.pkgs, "e2", [])
        load_package(con, empty2, "2024-01-02", 2)
        self.assertEqual(
            show_history(con, 1001, "2024-01-01", "2024-01-04")["intervals"],
            [iv("2024-01-01", "2024-01-04", **ABSENT)])
        # ... while cutoff 2 still shows the customer present from day 2
        self.assertEqual(
            show_history(con, 1001, "2024-01-01", "2024-01-04", upto=2)["intervals"],
            [iv("2024-01-01", "2024-01-02", **ABSENT),
             iv("2024-01-02", "2024-01-04", **SH)])


class ShowBasicsTest(TempDirTest):
    def test_upto_zero_and_ranges(self):
        con = self.open()
        pkg = make_package(self.pkgs, "p", [(1001, "Shanghai", "A", "CN-E")])
        load_package(con, pkg, "2024-01-01", 1)
        # 0 means "nothing ingested yet"
        self.assertEqual(
            show_history(con, 1001, "2024-01-01", "2024-01-03", upto=0)["intervals"],
            [iv("2024-01-01", "2024-01-03", **ABSENT)])
        # clipping to a subrange
        self.assertEqual(
            show_history(con, 1001, "2024-01-02", "2024-01-04")["intervals"],
            [iv("2024-01-02", "2024-01-04", **SH)])
        # a customer that never appeared
        self.assertEqual(
            show_history(con, 9999, "2024-01-01", "2024-01-03")["intervals"],
            [iv("2024-01-01", "2024-01-03", **ABSENT)])
        # empty range
        self.assertEqual(
            show_history(con, 1001, "2024-01-02", "2024-01-02")["intervals"], [])
        with self.assertRaises(HistoryError):
            show_history(con, 1001, "2024-01-03", "2024-01-02")

    def test_show_on_uninitialised_or_missing_db(self):
        con = self.open()  # schema created, nothing ingested
        self.assertEqual(
            show_history(con, 1001, "2024-01-01", "2024-01-02")["intervals"],
            [iv("2024-01-01", "2024-01-02", **ABSENT)])
        with self.assertRaises(HistoryError):
            history_connect(self.tmp / "nope.duckdb", read_only=True)


class LocalityTest(TempDirTest):
    """Revising an old date must not replay history: the load may only read
    the new roster, the changed date's old roster and the adjacent dates'
    rosters, and may only point-touch history boundaries of affected
    customers at the changed and next dates.  Verified here against the
    actual SQL issued on the connection (see README for the same check)."""

    class Spy:
        def __init__(self, con):
            self._con, self.sql = con, []

        def execute(self, sql, params=None):
            self.sql.append(" ".join(sql.split()))
            return (self._con.execute(sql, params) if params
                    else self._con.execute(sql))

        def __getattr__(self, k):
            return getattr(self._con, k)

    def test_read_write_paths_stay_local(self):
        con = self.Spy(history_connect(self.db))
        for day in range(1, 11):  # ten effective dates to revise inside of
            pkg = make_package(self.pkgs, f"d{day}",
                               [(1001, "Shanghai", "A", "CN-E")])
            load_package(con, pkg, f"2024-01-{day:02d}", 1)

        # correct 2024-01-03 with a higher revision
        pkg = make_package(self.pkgs, "d3r2", [(1001, "Beijing", "A", "CN-N")])
        con.sql.clear()
        load_package(con, pkg, "2024-01-03", 2)

        roster_dates = {"2024-01-02", "2024-01-03", "2024-01-04"}
        boundary_dates = {"2024-01-03", "2024-01-04"}
        saw_roster_read = saw_boundary_write = False
        for stmt in con.sql:
            if "FROM rosters" in stmt:
                saw_roster_read = True
                # every roster read is filtered to one of the allowed dates
                for m in re.finditer(
                        r"FROM rosters WHERE business_date = DATE '([^']+)'",
                        stmt):
                    self.assertIn(m.group(1), roster_dates)
                self.assertNotRegex(stmt, r"FROM rosters(?!\s+WHERE)")
            if "UPDATE history" in stmt or "INTO history" in stmt:
                saw_boundary_write = True
                m = re.search(r"valid_from = DATE '([^']+)'", stmt)
                self.assertIsNotNone(m, stmt)
                self.assertIn(m.group(1), boundary_dates)
            # the load never scans the history table wholesale
            self.assertNotRegex(stmt, r"\bFROM history\b(?!.*valid_from)")
        self.assertTrue(saw_roster_read)
        self.assertTrue(saw_boundary_write)

        # show locates the customer's boundaries with a single point query
        con.sql.clear()
        show_history(con, 1001, "2024-01-01", "2024-02-01")
        reads = [s for s in con.sql if "FROM history" in s]
        self.assertEqual(len(reads), 1)
        self.assertIn("customer_id = ?", reads[0])
        self.assertFalse(any("rosters" in s for s in con.sql))


class CliTest(TempDirTest):
    def run_cli(self, *argv):
        return subprocess.run(
            [sys.executable, "-m", "delivery_compiler", *argv],
            cwd=ROOT, capture_output=True, text=True)

    def test_load_and_show_cli(self):
        pkg = make_package(self.pkgs, "p", [(1001, "Shanghai", "A", "CN-E")])
        r = self.run_cli("history-load", "--db", str(self.db),
                         "--package", str(pkg), "--date", "2024-01-01",
                         "--revision", "1")
        self.assertEqual(r.returncode, 0, r.stderr)
        out = json.loads(r.stdout)
        self.assertEqual((out["seq"], out["status"]), (1, "ingested"))

        r = self.run_cli("history-show", "--db", str(self.db),
                         "--customer-id", "1001", "--start", "2024-01-01",
                         "--end", "2024-01-03")
        self.assertEqual(r.returncode, 0, r.stderr)
        out = json.loads(r.stdout)
        self.assertEqual(out["intervals"],
                         [iv("2024-01-01", "2024-01-03", **SH)])

        # errors exit non-zero and print a typed error
        bad = make_package(self.pkgs, "bad", [(1001, None, None, None),
                                              (1001, None, None, None)])
        r = self.run_cli("history-load", "--db", str(self.db),
                         "--package", str(bad), "--date", "2024-01-02",
                         "--revision", "1")
        self.assertEqual(r.returncode, 2)
        self.assertIn("HistoryError", r.stderr)


class RealCompilerPackageTest(TempDirTest):
    """End-to-end: a package produced by the actual compiler pipeline loads
    into the history store (extra columns/datasets are ignored)."""

    def test_generated_package_loads(self):
        src = self.tmp / "source.duckdb"
        subprocess.run([sys.executable, "demo/make_source.py", "--version", "v1",
                        "--out", str(src)], cwd=ROOT, check=True,
                       capture_output=True, text=True)
        subprocess.run([sys.executable, "-m", "delivery_compiler", "generate",
                        "--source", str(src),
                        "--policy", "demo/configs/policy_marketing.json",
                        "--lineage", "demo/configs/lineage.json",
                        "--projections", "demo/configs/proj_marketing.json",
                        "--out", str(self.tmp / "packages")],
                       cwd=ROOT, check=True, capture_output=True, text=True)
        pkg = next((self.tmp / "packages").iterdir())
        con = self.open()
        r = load_package(con, pkg, "2024-03-01", 1)
        self.assertEqual(r["rows"], 12)
        got = show_history(con, 1, "2024-03-01", "2024-03-03")["intervals"]
        self.assertEqual(got, [iv("2024-03-01", "2024-03-03", present=True,
                                  city="Springfield", segment="B",
                                  region_label="R1-North")])


if __name__ == "__main__":
    unittest.main()
