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
  __main__.py               # CLI: compile | preview | generate | verify | read
demo/
  make_source.py            # 构造 customer_profile / transactions / region 三表（v1/v2）
  configs/                  # 策略、血缘、投影（JSON）
  run_demo.py               # 端到端演示（含篡改攻击与版本变化）
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
