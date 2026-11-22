# Declarative Delivery Compiler (Python + DuckDB)

从共享湖表为不同消费者编译可独立核验的数据产品包：同一源字段可按消费者对
不同人 allow / mask / aggregate / deny，交付包携带逐字段决策证据，并可被
**独立验证器**逐项复核。

## 结构

```
delivery_compiler/          # 库 + CLI
  schema.py                 # 源模式读取：表/结构体字段路径/标签/关系/版本与内容哈希/_row_keys 行键校验
  policy.py                 # 策略模型与确定性冲突消解（编译器侧）
  lineage.py                # 派生字段表达式解析、血缘闭包、循环检测、SQL 重写
  compiler.py               # 编译：校验 + 决策 + 计划（compile_plan / format_decisions）
  row_lineage.py            # 逐输出行来源清单（row_lineage.jsonl）：中性 SQL 构造、磁盘暂存、流式写入
  generator.py              # 生成：固定源版本、列投影流式执行、逐行来源、不可变包
  verifier.py               # 独立验证器（不导入编译器逻辑，自行重算一切，含逐行来源重算）
  reader.py                 # 读取接口（绑定消费者身份）+ 离线行来源查询（无需源库）
  history.py                # 消费者侧客户历史库（history-load / history-show）
  snapshot_correction.py    # 快照更正：JSONL 更正单 + 旧快照 -> 新源库（snapshot-correct）
  __main__.py               # CLI: compile | preview | generate | verify | read | lineage | history-load | history-show | snapshot-correct
demo/
  make_source.py            # 构造 customer_profile / transactions / region 三表（v1/v2，含 _row_keys 声明）
  configs/                  # 策略、血缘、投影（JSON）
  run_demo.py               # 端到端演示（含篡改攻击与版本变化）
  history_demo.py           # 报表复现小样本（客户历史库）
tests/
  test_history.py           # 客户历史库可复跑测试
  test_row_lineage.py       # 逐行来源：连接/复合键/重复投影/聚合全贡献者/手算预期/篡改/离线/有界内存
  test_snapshot_correct.py  # 快照更正：整库迁移/顺序无关/归属整库校验/保留与清空/坏类型不发布/竞态发布/中断重试/有界内存
```

## 语义要点

- **策略**：按消费者/用途/字段路径（前缀）或上游标签声明 `allow|mask|aggregate|deny`，
  可带 `via` 限定连接路径。冲突永不按文件顺序覆盖：同（优先级， 具体度）不同动作
  → 编译错误；更具体路径在同优先级覆盖父路径；**deny 只能被更高优先级的显式路径
  规则解除**（标签/通配规则、同优先级规则都不能解除）。
- **聚合**：aggregate 规则必须声明 `group_keys` 与允许的 `stats`；行级数据集中出现
  聚合态字段、分组数据集中出现非分组键普通投影、或未声明的统计量，都是编译错误——
  普通投影不能改名为聚合。
- **血缘**：派生字段表达式必须完全可解析（全限定源路径或其它派生字段），声明的
  inputs 必须与实际引用一致；派生字段继承所有输入中**最严格**的限制
  （deny > aggregate > mask > allow）；无法解析血缘的表达式不得发布。
- **编译校验**：悬空引用、循环血缘、同名输出、未被策略覆盖的敏感字段、被拒的连接
  键，全部在编译期拒绝。
- **运行时**：从固定源版本（表内容哈希钉住）读取；按消费者产出字段集合、变换表达
  式、行数/分组计数与决策日志；deny 字段不出现在数据、schema、统计中（生成查询
  根本不扫描它们）。执行按列投影 + DuckDB 流式 COPY（内存受限），不把敏感源值
  装入 Python 内存。
- **包身份**：`package_id = hash(源版本+表哈希, 策略版本, 血缘版本, 投影版本, 消费者)`。
  重复生成是 no-op；任一版本变化产生新的不可变包。
- **独立验证器**：只读源库、策略/血缘/投影原文与包 manifest，自行重算策略消解、
  期望列、期望数据（EXCEPT ALL 双向对比）、分组与禁止字段；**从不读取**编译器
  写出的 `compiler_attestation.self_check` 或决策日志结论。

## 使用

```bash
python3 demo/make_source.py --version v1 --out demo/source_v1.duckdb

# 编译（校验 + 计划）
python3 -m delivery_compiler compile --source demo/source_v1.duckdb \
  --policy demo/configs/policy_marketing.json --lineage demo/configs/lineage.json \
  --projections demo/configs/proj_marketing.json --plan-out plan.json

# 预览逐字段决策
python3 -m delivery_compiler preview --source demo/source_v1.duckdb \
  --policy demo/configs/policy_marketing.json --lineage demo/configs/lineage.json \
  --projections demo/configs/proj_marketing.json

# 生成不可变交付包
python3 -m delivery_compiler generate --source demo/source_v1.duckdb \
  --policy demo/configs/policy_marketing.json --lineage demo/configs/lineage.json \
  --projections demo/configs/proj_marketing.json --out demo/out/packages

# 独立验证（退出码非 0 即失败）
python3 -m delivery_compiler verify --source demo/source_v1.duckdb \
  --policy demo/configs/policy_marketing.json --lineage demo/configs/lineage.json \
  --projections demo/configs/proj_marketing.json \
  --package demo/out/packages/<pkg_id>

# 以绑定消费者身份读取
python3 -m delivery_compiler read --package demo/out/packages/<pkg_id> --consumer marketing
```

端到端演示（构建源、编译、生成、验证、篡改攻击、版本变化）：

```bash
python3 demo/run_demo.py
```

## 逐输出行来源清单（row_lineage.jsonl）

每个 dataset 除 Parquet 外还附带一份 `<dataset>.row_lineage.jsonl`，使分析人员在
**脱离源库**、只有交付包时仍能回答"这一行来自哪些源记录"。它只记录来源，不改变
字段策略、交付数据或既有包的读取结果（Parquet 的 schema、内容、顺序均不变）。

### 行键声明（`_row_keys`）

源库用一张元数据表声明每张**业务表**的稳定行键列（可复合）：

```sql
CREATE TABLE _row_keys (table_name VARCHAR, column_name VARCHAR, key_ordinal INTEGER);
-- 演示源：customer_profile -> customer_id
--         transactions     -> txn_id
--         region           -> (country, region_code) 复合键
```

编译与生成前 `SourceSchema` 强制校验：每张业务表都有非空声明；键列真实存在；键值
非 NULL；复合键组合在表内唯一。任一不满足即抛 `RowKeyError` 并拒绝发布。来源身份
**只允许**使用这些业务键——禁止 DuckDB 物理 `rowid`、扫描顺序或输出位置；身份仅在
manifest 钉住的源快照（表内容哈希）内有意义。被策略 deny 的列不能作为行键，否则编译
拒绝（保证来源清单本身不泄露禁止字段）。

### 清单格式

每行一条 JSON，绑定"该行实际发布的变换后字段值 + 唯一映射标识 `row_ref` + 来源集合"：

```json
{
  "row_ref": "rr_4647e8c23248d229536db905ff088490",
  "values": {"customer_id": 1, "city": "Springfield", "segment": "B", "region_label": "R1-North"},
  "sources": [
    {"table": "customer_profile", "key": {"customer_id": 1}},
    {"table": "region", "key": {"region_code": "R1"}}
  ]
}
```

- 来源按 `(table, 行键)` 分组、排序、去重。普通连接行列出实际参与该行的每张表记录；
  聚合行列出参与该组计算的**全部**源记录（不是计数、样例或任取一条）。
- 相同投影值由不同源行产生时，因 `row_ref = sha256(canonical(values + sources))` 同时
  依赖来源集合，映射标识仍不同，可分别定位；复合键、经连接路径重复出现的同一来源不丢键、
  不重复计数。
- 文件只含行键值与发布值：不含源行非键字段、deny 值、策略解释或未发布列。
- 身份沿计划的连接/分组计算传递（窗口函数 + 磁盘暂存），不靠输出值反查来源。
- manifest 为每个清单与 Parquet 记录 `sha256`，并固定 `row_key_declarations`；`verify`
  在源库可用时**独立重算**每个输出行的来源集合并逐项比对，不信任生成器写入的任何通过标记。

### 离线查询一条输出行的来源

复制包后即使移走源库，也能用包内 manifest + 清单定位源表与行键（仅读包，不打开源库）：

```bash
# 按唯一映射标识
python3 -m delivery_compiler lineage --package <pkg_dir> --dataset customers \
  --row-ref rr_4647e8c23248d229536db905ff088490

# 或按该行发布的完整字段值（重复投影值会返回全部匹配，各自带不同 row_ref）
python3 -m delivery_compiler lineage --package <pkg_dir> --dataset customers \
  --values '{"customer_id": 1, "city": "Springfield", "segment": "B", "region_label": "R1-North"}'
```

库接口：`lookup_row_lineage(pkg_dir, dataset, row_ref=...)` 或
`lookup_row_lineage(pkg_dir, dataset, values={...})`。

### 流式与有界内存

来源计算在 DuckDB 内以长表、排序、`DISTINCT`、窗口聚合和磁盘暂存完成，Python 以
`fetchmany` 分批拉取；聚合的来源 JSON 与 `row_ref` 也在 SQL 内组装，逐行流式落盘，
不会把全部源行或贡献关系累积进 Python 内存（`tests/test_row_lineage.py` 用 30 万交易的
运行时生成源 + `tracemalloc` 断言峰值有界）。包在 manifest、数据文件、来源清单全部写完并
交叉校验前只存在于 `*.building` 暂存目录，成功后原子改名；失败不会覆盖既有不可变包。

## 快照更正（snapshot-correct）

交易归属更正（新建客户、客户迁区、撤掉空区域等）不在交付包里手工改，而是用
一份更正单针对**已关闭、不再写入的旧快照**生成一个可直接用于下次交付的新源库；
旧库原样保留，只处理演示的 `region`、`customer_profile`、`transactions` 三张
业务表，不做模式变更，也不回放历史更正单。

```bash
python3 -m delivery_compiler snapshot-correct \
  --source demo/source_v1.duckdb \
  --source-sha256 "$(sha256sum demo/source_v1.duckdb | cut -d' ' -f1)" \
  --corrections corrections.jsonl \
  --new-version v3-corrected \
  --out demo/source_v3.duckdb
# => {"status": "generated", "out": ".../source_v3.duckdb", "new_version": "v3-corrected"}
```

### 更正单格式（UTF-8 JSONL，一行一条）

每行用 `table`、`key`、`op` 指定业务表、按 `_row_keys` 声明填写的键对象和操作：

```jsonl
{"table": "region", "key": {"region_code": "R5"}, "op": "insert", "values": {"region_name": "Northeast", "country": "US", "tier": "T1"}}
{"table": "customer_profile", "key": {"customer_id": 13}, "op": "insert", "values": {"full_name": "Mona Moore", "contact": {"email": "u13@corp.net", "phone": null}, "address": {"line1": null, "city": "Newville", "zip": "02100"}, "id_number": "GOV-1013", "segment": "A", "region_code": "R5", "created_at": "2024-03-01T09:00:00"}}
{"table": "transactions", "key": {"txn_id": 1000}, "op": "update", "values": {"customer_id": 13}}
{"table": "region", "key": {"region_code": "R4"}, "op": "delete"}
```

- **insert**：`values` 必须给齐全部非键列（可以显式写 `null`），键在旧表中必须
  不存在。
- **update**：`values` 只列需要替换的非键列；**没写的字段保留原值，不等于写
  NULL**。键只能出现在 `key` 中，出现在 `values` 里（改键）按未知列拒绝。
- **delete**：不带 `values`，键在旧表中必须存在。
- **STRUCT 整体替换**：以整个顶层列为单位，JSON 对象的成员必须与现有 struct
  成员完全一致（缺成员、多成员都拒绝）；成员可显式给 `null` 清空。
- 按原列类型严格校验：时间戳/日期用 ISO 字符串（时间戳必须带时间部分），
  数字字符串不能当数值，布尔不能当整数，`NaN`/`Infinity`、超范围整数、带时区
  时间戳等无法精确表示的值一律拒绝。

### 批次语义与整库校验

更正单是针对同一个旧快照的**一批更正，不是有先后顺序的命令脚本**：

- 同一个表键在一批中只允许出现一次；允许先列子行、后列它需要的新父行，调换
  各行先后得到完全相同的业务结果。
- 以施加整批更正后的库为准，检查**全部**行键（非 NULL、唯一）以及 `_relations`
  里的两条关系（每笔交易的客户、每位客户的区域）：外键必须非 NULL 且唯一匹配
  父行。因此删除父行时，任何**未被本批编辑**但仍引用它的子行都会导致整批拒绝；
  只校验被编辑行是不够的。
- 新库保持原表定义、`_row_keys` 键声明、`_field_tags` 字段标签和 `_relations`
  关系声明不变，只更新业务数据和 `_snapshot` 版本，并额外写入一张
  `_snapshot_correction` 表（旧库摘要、更正单原始字节 SHA-256、新版本），可被
  现有 `compile` / `generate` / `verify` 直接使用。
- 更正单逐行流式读取并暂存到 DuckDB，存在性与归属检查全部是集合 SQL，源数据
  不会攒进 Python 内存。

### 发布边界与幂等

目标路径就是发布边界：

- 源摘要不符、新版本与旧版本相同、任一更正无效或运行中断，都不会在目标位置
  留下半成品，也不会覆盖任何已发布文件；临时库只存在于目标旁的私有暂存目录。
- 发布通过**独占硬链接**原子完成，而不是"先看目标不存在、最后再覆盖"：两个进程
  争用同一目标时只有先发布的完整结果能留下。
- 相同请求（旧库摘要 + 更正单摘要 + 新版本三元组一致）在同一目标重试时认出
  已发布结果并返回 `exists-identical`，不重写文件；即使上一次发布后来不及回报，
  重试也能识别。请求不同而目标已被占用则报冲突。

## 消费者侧客户历史库（history-load / history-show）

市场团队每天收到一份客户交付包；包会迟到、乱序、被更正（同一业务日期的更高
修订）。历史库消费**已交付的包**（不重新编译策略、不访问源湖、不改动包），
既保留最新更正后的历史，也能按"当时已收到的批次"复现旧报表看到的客户状态。

- 只支持 `delivery-package/1` 的 `marketing` 包及其 `customers` 数据集；从
  manifest 定位 Parquet，其他数据集一律不读。只入库 `customer_id`（BIGINT，
  唯一且非 NULL）与 `city`、`segment`、`region_label`（可 NULL）。包已由上游
  验证，这里只检查本功能必要的绑定、文件、实际列类型与主键；空快照合法。
- 每个包是当天**完整名单**：已出现过的客户若缺席，表示他从当天起不在名单中
  （既不保留上次值，也不是"属性全空但在名单"——后者是合法且不同的状态）。
- 每次成功收录返回从 1 开始递增的入库序号。日期可乱序到达；同一天只有更高
  修订能替代有效名单。字节相同（manifest 与对应 Parquet）的重送返回原序号、
  不改变任何状态；复用旧修订却换内容、或提交未收录过且低于当前修订的请求，
  均报错。收录记录、当天生效名单、回看历史在同一事务提交；错误不消耗序号、
  不留部分更新。重开库后查询不再需要原始交付包。

```bash
# 收录一个交付包（库不存在则创建）
python3 -m delivery_compiler history-load --db history.duckdb \
  --package demo/out/packages/<pkg_id> --date 2024-01-02 --revision 1
# => {"seq": 4, "status": "ingested", ...}   重送同字节包 => {"seq": 4, "status": "duplicate"}

# 回看某客户在 [start, end) 的名单状态；--upto 为截止入库序号
# （默认最新；0 表示尚未收录任何包）
python3 -m delivery_compiler history-show --db history.duckdb \
  --customer-id 1001 --start 2024-01-01 --end 2024-01-06 --upto 3
```

`history-show` 仅用截至 `--upto` 各日期的最高已收录修订解释历史：某日的名单
持续有效至下一个已收录的业务日期，首份之前视作不在名单中。输出为按开始日期
排序、裁剪到请求范围并合并相邻相同状态的 JSON 区间；`present: false`（不在
名单）与 `present: true` 加三个属性严格区分，值没变的已收录日不会截断区间；
后收到的包不改变过去序号下的查询结果。

### 局部性：修订老日期不回放全部历史

每次入库只读取：本次名单、变更日的原有效名单、相邻前后有效日期的名单（行级
差异比较），再按受影响客户定点读写变更日与下一有效日的历史边界——不扫描其他
天的名单，不重建整段历史。回看则直接按 `(customer_id, valid_from)` 主键定位该
客户的历史边界，不逐日重读名单。

核对方式（无需新增诊断命令）：库函数接受外部连接，包一层记录每条 SQL 的连接
即可从实际读写路径核对——

```python
from delivery_compiler import history_connect, load_package, show_history

class Spy:
    def __init__(self, con): self._con, self.sql = con, []
    def execute(self, sql, params=None):
        self.sql.append(sql)
        return self._con.execute(sql, params) if params else self._con.execute(sql)
    def __getattr__(self, k): return getattr(self._con, k)

con = Spy(history_connect("history.duckdb"))
load_package(con, "path/to/package", "2024-01-02", 2)
# con.sql 中可见：read_parquet 只扫本次包；rosters 只以
# business_date ∈ {变更日, 前一有效日, 后一有效日} 过滤（diff 用变更日或前一日
# 的整份名单，前后邻日按 customer_id 点查）；history 的 UPDATE/INSERT 只落在
# valid_from ∈ {变更日, 后一有效日} 且 customer_id ∈ 受影响集合
show_history(con, 1001, "2024-01-01", "2024-01-06")
# con.sql 中可见：仅一条对 history 的 customer_id 点查，不读 rosters、不逐日扫描
```

也可用 DuckDB 自带 profiler 复核单条语句的实际扫描：`PRAGMA
enable_profiling='json'; PRAGMA profile_output='/tmp/show.json'` 后执行一次
`show_history`，profile 中只有一处对 `history` 表、带 `customer_id` 过滤的扫描。

### 测试与演示

```bash
python3 -m unittest discover -s tests -v   # 可复跑测试（含报表复现情形）
python3 demo/history_demo.py               # 小样本演示：先收 1/3/5 日、补 2 日、
                                           # 更正 2 日、空日包、重开库、幂等重送、
                                           # 重复主键拒绝
```
