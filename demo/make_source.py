"""Build the demo source lake: customer_profile, transactions, region + metadata.

Usage: python3 make_source.py --version v1 --out demo/source_v1.duckdb
v2 adds more rows (new customers + transactions) so content hashes change.
"""
import argparse
from pathlib import Path

import duckdb

REGIONS = [
    ("R1", "North", "US", "T1"),
    ("R2", "South", "US", "T2"),
    ("R3", "East", "CN", "T1"),
    ("R4", "West", "DE", "T2"),
]

TAGS = [
    ("customer_profile.full_name", "pii"),
    ("customer_profile.contact.email", "pii"),
    ("customer_profile.contact.phone", "pii"),
    ("customer_profile.address.line1", "pii"),
    ("customer_profile.id_number", "pii"),
    ("customer_profile.id_number", "gov_id"),
    ("transactions.amount", "financial"),
    ("region.tier", "internal"),
]

RELATIONS = [
    ("txn_cust", "transactions", "customer_id", "customer_profile", "customer_id"),
    ("cust_region", "customer_profile", "region_code", "region", "region_code"),
]

DOMAINS = ["gmail.com", "yahoo.com", "example.org", "corp.net"]
CITIES = {"R1": ("Springfield", "01101"), "R2": ("Austin", "73301"),
          "R3": ("Hangzhou", "310000"), "R4": ("Berlin", "10115")}
SEGMENTS = ["A", "B", "C"]
CHANNELS = ["web", "pos", "app"]
CURRENCIES = {"R1": "USD", "R2": "USD", "R3": "CNY", "R4": "EUR"}


def build(db_path, version):
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.exists():
        p.unlink()
    con = duckdb.connect(str(p))
    con.execute("CREATE TABLE region (region_code VARCHAR, region_name VARCHAR, "
                "country VARCHAR, tier VARCHAR)")
    con.execute("""
        CREATE TABLE customer_profile (
            customer_id BIGINT, full_name VARCHAR,
            contact STRUCT(email VARCHAR, phone VARCHAR),
            address STRUCT(line1 VARCHAR, city VARCHAR, zip VARCHAR),
            id_number VARCHAR, segment VARCHAR, region_code VARCHAR,
            created_at TIMESTAMP)""")
    con.execute("""
        CREATE TABLE transactions (
            txn_id BIGINT, customer_id BIGINT, txn_time TIMESTAMP, amount DOUBLE,
            currency VARCHAR, merchant_id VARCHAR, channel VARCHAR)""")

    con.executemany("INSERT INTO region VALUES (?, ?, ?, ?)", REGIONS)

    n_customers = 12 if version == "v1" else 14
    names = ["Alice Anderson", "Bob Brown", "Carol Clark", "David Davis",
             "Erin Evans", "Frank Foster", "Grace Green", "Henry Hall",
             "Ivy Ingram", "Jack Jones", "Karen King", "Liam Lewis",
             "Mona Moore", "Noah Nelson"]
    for i in range(1, n_customers + 1):
        region = REGIONS[(i - 1) % len(REGIONS)][0]
        city, zipc = CITIES[region]
        con.execute(
            "INSERT INTO customer_profile VALUES (?, ?, "
            "{'email': ?, 'phone': ?}, {'line1': ?, 'city': ?, 'zip': ?}, "
            "?, ?, ?, ?::TIMESTAMP)",
            [i, names[i - 1],
             f"user{i}@{DOMAINS[i % len(DOMAINS)]}", f"+1-555-{1000 + i}",
             f"{10 + i} Main St", city, zipc,
             f"GOV-{1000 + i}", SEGMENTS[i % len(SEGMENTS)], region,
             f"2024-01-{i:02d} 09:00:00"])

    n_txn = 40 if version == "v1" else 48
    rows = []
    for i in range(n_txn):
        cust = i % 11 + 1                      # customer 12+ never transacts
        amount = round((i * 137) % 2500 + 5 + (i % 7) * 0.5, 2)
        region = REGIONS[(cust - 1) % len(REGIONS)][0]
        rows.append((1000 + i, cust,
                     f"2024-02-{i % 28 + 1:02d} {10 + i % 8}:30:00",
                     amount, CURRENCIES[region], f"M{i % 6 + 1}",
                     CHANNELS[i % len(CHANNELS)]))
    con.executemany("INSERT INTO transactions VALUES (?, ?, ?::TIMESTAMP, ?, ?, ?, ?)",
                    rows)

    con.execute("CREATE TABLE _snapshot (version VARCHAR)")
    con.execute("INSERT INTO _snapshot VALUES (?)", [version])
    con.execute("CREATE TABLE _field_tags (path VARCHAR, tag VARCHAR)")
    con.executemany("INSERT INTO _field_tags VALUES (?, ?)", TAGS)
    con.execute("CREATE TABLE _relations (name VARCHAR, from_table VARCHAR, "
                "from_column VARCHAR, to_table VARCHAR, to_column VARCHAR)")
    con.executemany("INSERT INTO _relations VALUES (?, ?, ?, ?, ?)", RELATIONS)
    con.close()
    print(f"built {db_path} (version={version}, customers={n_customers}, "
          f"transactions={n_txn})")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", required=True, choices=["v1", "v2"])
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    build(a.out, a.version)
