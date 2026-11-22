"""报表复现小样本：消费者侧客户历史库（端到端演示）。

故事线：市场团队先收到 1、3、5 日的客户交付包，后来才补上 2 日，最后
上游又更正了 2 日。财务要复现"补齐前"报表看到的客户状态。

  * 客户 1001：补录在旧区间中间切出短区间，更正又使区间合并；
  * 客户 1002：对比"属性全空但在名单"与"不在名单"（漏行语义）；
  * 2024-01-08：一个完全空的日包；
  * 重开库后重送已被替代的旧修订（幂等），再投带重复主键的更高修订（拒绝）。

运行：python3 demo/history_demo.py
"""
from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from delivery_compiler import HistoryError, history_connect, load_package, \
    show_history

import duckdb


def make_package(pkg_dir, rows):
    """构造一个已交付的 delivery-package/1 marketing 包（manifest + parquet）。"""
    pkg_dir = Path(pkg_dir)
    pkg_dir.mkdir(parents=True)
    con = duckdb.connect()
    con.execute("CREATE TABLE t (customer_id BIGINT, city VARCHAR, "
                "segment VARCHAR, region_label VARCHAR)")
    if rows:
        con.executemany("INSERT INTO t VALUES (?, ?, ?, ?)", rows)
    con.execute(f"COPY t TO '{pkg_dir / 'customers.parquet'}' (FORMAT PARQUET)")
    con.close()
    manifest = {"format": "delivery-package/1",
                "package_id": f"pkg_{pkg_dir.name}",
                "consumer": "marketing", "purpose": "analytics",
                "datasets": [{"name": "customers", "file": "customers.parquet"},
                             {"name": "amount_by_region",
                              "file": "amount_by_region.parquet"}]}
    (pkg_dir / "manifest.json").write_text(json.dumps(manifest, indent=2),
                                           encoding="utf-8")
    return pkg_dir


def show(con, cid, start, end, upto, note):
    r = show_history(con, cid, start, end, upto)
    print(f"\n### {note}\n    customer={cid} [{start}, {end}) upto={r['upto']}")
    for iv in r["intervals"]:
        if iv["present"]:
            print(f"    [{iv['start']}, {iv['end']})  present  "
                  f"city={iv['city']} segment={iv['segment']} "
                  f"region_label={iv['region_label']}")
        else:
            print(f"    [{iv['start']}, {iv['end']})  NOT in roster")


def main():
    tmp = Path(tempfile.mkdtemp(prefix="history_demo_"))
    try:
        pkgs, db = tmp / "packages", tmp / "history.duckdb"
        pkgs.mkdir()
        W = ("2024-01-01", "2024-01-06")

        d1r1 = make_package(pkgs / "d1r1", [(1001, "Shanghai", "A", "CN-E"),
                                            (1002, None, None, None)])
        d3r1 = make_package(pkgs / "d3r1", [(1001, "Shanghai", "A", "CN-E")])
        d5r1 = make_package(pkgs / "d5r1", [(1001, "Shanghai", "A", "CN-E"),
                                            (1002, "Tianjin", "B", "CN-N2")])
        d2r1 = make_package(pkgs / "d2r1", [(1001, "Beijing", "A", "CN-N")])
        d2r2 = make_package(pkgs / "d2r2", [(1001, "Shanghai", "A", "CN-E"),
                                            (1002, None, None, None)])
        d8r1 = make_package(pkgs / "d8r1", [])          # 完全空的日包
        d2r3 = make_package(pkgs / "d2r3", [(1001, "Shanghai", "A", "CN-E"),
                                            (1001, "Shanghai", "A", "CN-E")])

        con = history_connect(db)
        print("== 先收 1、3、5 日 ==")
        for pkg, d in ((d1r1, "2024-01-01"), (d3r1, "2024-01-03"),
                       (d5r1, "2024-01-05")):
            r = load_package(con, pkg, d, 1)
            print(f"  load {d} rev1 -> seq {r['seq']} ({r['rows']} rows)")
        show(con, 1001, *W, 3, "补录前（财务要复现的视角）")
        show(con, 1002, *W, 3, "补录前：1002 属性全空但在名单 vs 不在名单")

        print("\n== 补录 2 日 rev1 ==")
        print(f"  load 2024-01-02 rev1 -> seq "
              f"{load_package(con, d2r1, '2024-01-02', 1)['seq']}")
        show(con, 1001, *W, 4, "补录后：旧区间中间被切出短区间")
        show(con, 1001, *W, 3, "同一窗口回看旧序号：结果不变")

        print("\n== 更正 2 日 rev2 ==")
        print(f"  load 2024-01-02 rev2 -> seq "
              f"{load_package(con, d2r2, '2024-01-02', 2)['seq']}")
        show(con, 1001, *W, 5, "更正后：区间重新合并")
        show(con, 1002, *W, 5, "更正后：1002 在 1、2 日合并为一段全空属性区间")

        print("\n== 完全空的日包（2024-01-08）==")
        r = load_package(con, d8r1, "2024-01-08", 1)
        print(f"  load 2024-01-08 rev1 -> seq {r['seq']} ({r['rows']} rows)")
        show(con, 1001, "2024-01-06", "2024-01-10", 6, "空包之后：全员不在名单")

        print("\n== 保存并重开库（不再需要原始交付包）==")
        con.close()
        # 只保留 d2r1 供后面重送；删除其余包以证明查询只依赖历史库
        for d in ("d1r1", "d3r1", "d5r1", "d2r2", "d8r1"):
            shutil.rmtree(pkgs / d)
        con = history_connect(db, read_only=True)
        show(con, 1001, *W, 5, "重开后只读查询（其余包已删除）")
        con.close()

        con = history_connect(db)
        r = load_package(con, pkgs / "d2r1", "2024-01-02", 1)
        print(f"\n== 重送已被替代的 2 日 rev1（字节相同）==\n"
              f"  -> 返回原序号 seq {r['seq']}（{r['status']}），状态不变")

        print("\n== 投带重复主键的 2 日 rev3 ==")
        try:
            load_package(con, d2r3, "2024-01-02", 3)
            print("  !! 未被拒绝")
        except HistoryError as e:
            print(f"  拒绝：{e}")
        d9r1 = make_package(pkgs / "d9r1", [(1001, "Shanghai", "A", "CN-E")])
        r = load_package(con, d9r1, "2024-01-09", 1)
        print(f"  下一条成功收录仍是 seq {r['seq']}（错误未消耗序号）")
        show(con, 1001, *W, 5, "拒绝后回看：与之前完全一致")
        con.close()
        print(f"\n演示目录（可删除）：{tmp}")
    except Exception:
        shutil.rmtree(tmp, True)
        raise


if __name__ == "__main__":
    sys.exit(main())
