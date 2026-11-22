# Declarative Delivery Compiler (Python + DuckDB)

从共享湖表为不同消费者编译可独立核验的数据产品包：同一源字段可按消费者对
不同人 allow / mask / aggregate / deny，交付包携带逐字段决策证据，并可被
**独立验证器**逐项复核。

## 结构

```
delivery_compiler/          # 库 + CLI
  schema.py                 # 源模式读取：表/结构体字段路径/标签/关系/版本与内容哈希
  policy.py                 # 策略模型与确定性冲突消解（编译器侧）
  lineage.py                # 派生字段表达式解析、血缘闭包、循环检测、SQL 重写
  compiler.py               # 编译：校验 + 决策 + 计划（compile_plan / format_decisions）
  generator.py              # 生成：固定源版本、列投影流式执行、不可变包
  verifier.py               # 独立验证器（不导入编译器逻辑，自行重算一切）
  reader.py                 # 读取接口（绑定消费者身份）
  history.py                # 消费者侧客户历史库（history-load / history-show）
  __main__.py               # CLI: compile | preview | generate | verify | read | history-load | history-show
demo/
  make_source.py            # 构造 customer_profile / transactions / region 三表（v1/v2）
  configs/                  # 策略、血缘、投影（JSON）
  run_demo.py               # 端到端演示（含篡改攻击与版本变化）
  history_demo.py           # 报表复现小样本（客户历史库）
tests/
  test_history.py           # 客户历史库可复跑测试
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
