# V17 - 最终生产部署指令 (Final Production Deployment Directives)

**文档状态**: 最终封版。已整合最终签收单中的所有阻塞项修复与“补焊”建议，是代码实现的最终指令。

---

## 第一部分：签收结论

**签收结论：通过（Go for Production）**

本方案已具备生产韧性，在合并以下最终补丁后，即可进入主干开发与部署。

---

## 第二部分：核心组件最终实现 (含最终修复)

### 1. `DRTransform` (修正导入路径)
```python
# src/transforms/dr.py
import polars as pl
from .base import BaseTransformer # 修正：同级相对导入
from ..config.enum_maps import LEADS_TYPE_MAP, CHANNEL_MAP_WHITELIST

class DRTransform(BaseTransformer):
    # ... 其余实现与 V15/V16 一致
```

### 2. `finalize.py` (增加中文名去重校验)
```python
# src/finalize.py
from .config.output_spec import FINAL_NAME_MAP, OUTPUT_ORDER

def finalize_output(df: pl.DataFrame) -> pl.DataFrame:
    # 新增：校验中文名映射值是否有重复
    cn_names = list(FINAL_NAME_MAP.values())
    if len(cn_names) != len(set(cn_names)):
        dups = {x for x in cn_names if cn_names.count(x) > 1}
        raise ValueError(f"FINAL_NAME_MAP has duplicate Chinese names: {sorted(dups)}")

    # ... 其余实现与 V15/V16 一致
```

### 3. `compute_settlement_cn` (时区可配置)
```python
# src/analysis/settlement.py
from ..config.constants import DEFAULT_TZ # 新增：从配置导入

def compute_settlement_cn(df: pl.DataFrame, dimension: str) -> pl.DataFrame:
    lf = df.lazy().with_columns([
        pl.when(pl.col("date").is_dtype(pl.Datetime))
          .then(pl.col("date").dt.convert_time_zone(DEFAULT_TZ).dt.date()) # 使用配置
          .otherwise(pl.col("date"))
          .alias("date")
    ]).with_columns(pl.col("date").dt.truncate("1mo").alias("month"))
    # ... 其余实现与 V15/V16 一致
```

### 4. `obs.py` (修正 API 并优化文案)
```python
# src/obs.py
def audit(step: str, lf_before: pl.LazyFrame, lf_after: pl.LazyFrame, key_cols=("nsc_code","date")):
    b = lf_before.select([*key_cols]).unique().collect().height
    a = lf_after.select([*key_cols]).unique().collect().height
    coverage_delta = a - b
    # 修正：优化日志文案，使其更贴近语义
    print(f"[audit] {step}: uniq_keys_before={b}, uniq_keys_after={a}, coverage_delta={coverage_delta}")
```

---

## 第三部分：流程固化与最终核对清单

### 1. 流程固化点
- **`account_base` 唯一性断言**: 在 `DataProcessor` 中，连接 `account_base` 前，必须调用 `_assert_account_base_unique(account_base)`。
- **CI Lint**: CI/CD 流程需加入规则，检查宽表阶段是否包含 `_sum/_rate/_ratio` 后缀的列名，若包含则构建**失败**。

### 2. 上线前最终核对清单
- [ ] `obs.audit` 输出 `uniq_keys_*` 正常，无异常 API。
- [ ] `account_base` 在 `nsc_code` 上唯一（或改为 `(nsc_code, month)` 快照规则，并同步 settlement 的 join key）。
- [ ] 宽表阶段无 `_sum/_rate/_ratio`（CI Lint 生效）。
- [ ] 金样 md5 与上次基线一致；若不一致，有变更说明。
- [ ] 计划图文本已生成；审计表存在本批次记录。
- [ ] `OUTPUT_ORDER` 无缺列/多余列告警；`FINAL_NAME_MAP` 告警为 0。

---

## 附录A：最终 Commit Message

```
V16: production go — enums standardized, typed dedup keys, lazy join collision-proofed, monthly T/T-1 with configurable timezone guard, output hard-gated (incl. CN dup check), plans/audits compatible.
```

---

此 V17 手册是最终的、不可再变更的开发指令。所有后续开发工作都将严格遵循本文档进行。