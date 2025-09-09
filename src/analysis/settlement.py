from __future__ import annotations
import logging
import os
from dataclasses import dataclass
from typing import Literal, Dict, List, Set, Tuple
import polars as pl

# --- Module-level Documentation & Configuration ---
"""
Settlement Metrics Calculation Engine

This module provides a unified, spec-driven engine for computing settlement metrics.

Core Design:
- Single Source of Truth (SSOT): All business logic is defined in registries.
- Unified Pipeline: A single pipeline handles different aggregation dimensions.
- Spec-Driven: The engine's behavior is controlled by specs, not hardcoded logic.

Key Business Logic Decisions Documented:
- '有效天数' (Effective Days): Aggregated using 'max' as it's considered a constant
  property of the period within a group.
- '有效直播场次' (Effective Live Sessions): Aggregated using 'sum' as it's a cumulative
  count over the period.
- Empty NSC Handling: For 'level' dimension, empty/null NSCs are excluded from the
  denominator ('level_nsc_count') by default. Whether they are excluded from the
  numerator sums is controlled by the 'LEVEL_SUM_EXCLUDE_EMPTY_NSC' env var.
"""

logger = logging.getLogger(__name__)

# --- Feature Flags & Helpers ---
def _is_on(name: str) -> bool:
    """Generic feature-flag reader."""
    v = (os.getenv(name) or "").strip().lower()
    return v in {"1", "true", "yes", "on"}

def _num(c: str) -> pl.Expr:
    return pl.col(c).cast(pl.Float64).fill_null(0.0).fill_nan(0.0)

def SAFE_DIV(a: pl.Expr, b: pl.Expr) -> pl.Expr:
    return pl.when((b.is_not_null()) & (b != 0)).then(a / b).otherwise(0.0)

def _with_period(names: List[str]) -> Set[str]:
    s = set(names); s.update(f"T月{x}" for x in names); s.update(f"T-1月{x}" for x in names); return s

# --- SSOT 1: Base Fields Registry ---
@dataclass(frozen=True)
class BaseFieldSpec:
    name: str
    agg: Literal["sum", "max"] = "sum"

def _get_base_fields_registry() -> list[BaseFieldSpec]:
    sum_fields = [
        "自然线索量", "付费线索量", "车云店付费线索量", "区域加码付费线索量", "本地线索量",
        "车云店+区域投放总金额", "直播时长", "超25分钟直播时长(分)", "直播线索量",
        "曝光人数", "场观", "锚点曝光量", "组件点击次数", "小风车点击次数",
        "组件留资人数（获取线索量）", "小风车留资量", "进私人数", "私信开口人数",
        "咨询留资人数", "短视频条数", "短视频播放量", "有效直播场次",
        "线索总量", "总付费线索", "超25分钟直播时长(小时)",
    ]
    max_fields = ["有效天数"]
    
    registry = []
    for field_list, agg_method in [(sum_fields, "sum"), (max_fields, "max")]:
        for name in field_list:
            registry.append(BaseFieldSpec(name, agg=agg_method))
            registry.append(BaseFieldSpec(f"T月{name}", agg=agg_method))
            registry.append(BaseFieldSpec(f"T-1月{name}", agg=agg_method))
    return registry

BASE_FIELDS_REGISTRY = _get_base_fields_registry()

# --- SSOT 2: Derived Specs Registry ---
@dataclass(frozen=True)
class DerivedSpec:
    name: str
    kind: Literal["ratio_total"] = "ratio_total"
    num: str | None = None
    den: str | None = None

def _mk_ratio_specs(base: str, num: str, den: str) -> list[DerivedSpec]:
    return [
        DerivedSpec(base, "ratio_total", num, den),
        DerivedSpec(f"T月{base}", "ratio_total", f"T月{num}", f"T月{den}"),
        DerivedSpec(f"T-1月{base}", "ratio_total", f"T-1月{num}", f"T-1月{den}"),
    ]

DERIVED_SPECS: list[DerivedSpec] = [
    DerivedSpec("车云店+区域综合CPL", "ratio_total", "车云店+区域投放总金额", "线索总量"),
    DerivedSpec("付费CPL（车云店+区域）", "ratio_total", "车云店+区域投放总金额", "总付费线索"),
    DerivedSpec("本地线索占比", "ratio_total", "本地线索量", "线索总量"),
]
DERIVED_SPECS += _mk_ratio_specs("直播付费CPL", "车云店+区域投放总金额", "总付费线索")
DERIVED_SPECS += _mk_ratio_specs("曝光进入率", "场观", "曝光人数")
DERIVED_SPECS += _mk_ratio_specs("小风车点击率", "小风车点击次数", "场观")
DERIVED_SPECS += _mk_ratio_specs("小风车点击留资率", "小风车留资量", "小风车点击次数")
DERIVED_SPECS += _mk_ratio_specs("组件点击率", "组件点击次数", "锚点曝光量")
DERIVED_SPECS += _mk_ratio_specs("组件留资率", "组件留资人数（获取线索量）", "组件点击次数")
DERIVED_SPECS += _mk_ratio_specs("私信咨询率", "私信开口人数", "进私人数")
DERIVED_SPECS += _mk_ratio_specs("咨询留资率", "咨询留资人数", "私信开口人数")
DERIVED_SPECS += _mk_ratio_specs("私信转化率", "咨询留资人数", "进私人数")
DERIVED_SPECS += _mk_ratio_specs("场均曝光人数", "曝光人数", "有效直播场次")
DERIVED_SPECS += _mk_ratio_specs("场均场观", "场观", "有效直播场次")
DERIVED_SPECS += _mk_ratio_specs("场均小风车留资量", "小风车留资量", "有效直播场次")
DERIVED_SPECS += _mk_ratio_specs("场均小风车点击次数", "小风车点击次数", "有效直播场次")
DERIVED_SPECS += _mk_ratio_specs("直播车云店+区域日均消耗", "车云店+区域投放总金额", "有效天数")
DERIVED_SPECS += _mk_ratio_specs("日均有效（25min以上）时长（h）", "超25分钟直播时长(小时)", "有效天数")
DERIVED_SPECS += _mk_ratio_specs("日均进私人数", "进私人数", "有效天数")
DERIVED_SPECS += _mk_ratio_specs("日均私信开口人数", "私信开口人数", "有效天数")
DERIVED_SPECS += _mk_ratio_specs("日均咨询留资人数", "咨询留资人数", "有效天数")

# --- SSOT 3: Normalizable Fields ---
NORMALIZABLE_BASE_FIELDS = [
    "自然线索量", "付费线索量", "车云店+区域投放总金额", "直播线索量", "锚点曝光量",
    "组件点击次数", "组件留资人数（获取线索量）", "短视频条数", "短视频播放量", "直播时长",
    "直播车云店+区域付费线索量",
]
NORMALIZABLE_FIELDS: Set[str] = _with_period(NORMALIZABLE_BASE_FIELDS)

DERIVED_LEVEL_NORMALIZE: Set[str] = _with_period([
    "直播车云店+区域日均消耗", "日均有效（25min以上）时长（h）", "日均进私人数",
    "日均私信开口人数", "日均咨询留资人数",
])

# --- SSOT 4: Alias Maps & Output Contract ---
ALIAS_FINAL_MAP = {
    # 业务口径别名（总量 → 契约名）
    "总付费线索": "直播车云店+区域付费线索量",
    "T月总付费线索": "T月直播车云店+区域付费线索量",
    "T-1月总付费线索": "T-1月直播车云店+区域付费线索量",

    # 门店名统一
    "门店名称": "门店名",

    # 三类比率列的契约名（含 T/T-1）
    "私信咨询率": "私信咨询率=开口/进私",
    "T月私信咨询率": "T月私信咨询率=开口/进私",
    "T-1月私信咨询率": "T-1月私信咨询率=开口/进私",

    "咨询留资率": "咨询留资率=留资/咨询",
    "T月咨询留资率": "T月咨询留资率=留资/咨询",
    "T-1月咨询留资率": "T-1月咨询留资率=留资/咨询",

    "私信转化率": "私信转化率=留资/进私",
    "T月私信转化率": "T月私信转化率=留资/进私",
    "T-1月私信转化率": "T-1月私信转化率=留资/进私",
}

OUTPUT_CONTRACT_ORDER: Dict[str, list[str]] = {
    "层级": [
        "层级", "自然线索量", "付费线索量", "车云店+区域投放总金额", "直播时长", "T月直播时长", "T-1月直播时长",
        "直播线索量", "T月直播线索量", "T-1月直播线索量", "锚点曝光量", "T月锚点曝光量", "T-1月锚点曝光量",
        "组件点击次数", "T月组件点击次数", "T-1月组件点击次数", "组件留资人数（获取线索量）", "T月组件留资人数（获取线索量）", "T-1月组件留资人数（获取线索量）",
        "短视频条数", "T月短视频条数", "T-1月短视频条数", "短视频播放量", "T月短视频播放量", "T-1月短视频播放量",
        "车云店+区域综合CPL", "付费CPL（车云店+区域）", "直播付费CPL", "T月直播付费CPL", "T-1月直播付费CPL",
        "直播车云店+区域日均消耗", "T月直播车云店+区域日均消耗", "T-1月直播车云店+区域日均消耗",
        "直播车云店+区域付费线索量", "T月直播车云店+区域付费线索量", "T-1月直播车云店+区域付费线索量",
        "日均有效（25min以上）时长（h）", "T月日均有效（25min以上）时长（h）", "T-1月日均有效（25min以上）时长（h）",
        "场均曝光人数", "T月场均曝光人数", "T-1月场均曝光人数", "曝光进入率", "T月曝光进入率", "T-1月曝光进入率",
        "场均场观", "T月场均场观", "T-1月场均场观", "小风车点击率", "T月小风车点击率", "T-1月小风车点击率",
        "小风车点击留资率", "T月小风车点击留资率", "T-1月小风车点击留资率", "场均小风车留资量", "T月场均小风车留资量", "T-1月场均小风车留资量",
        "场均小风车点击次数", "T月场均小风车点击次数", "T-1月场均小风车点击次数", "组件点击率", "T月组件点击率", "T-1月组件点击率",
        "组件留资率", "T月组件留资率", "T-1月组件留资率", "本地线索占比",
        "日均进私人数", "T月日均进私人数", "T-1月日均进私人数", "日均私信开口人数", "T月日均私信开口人数", "T-1月日均私信开口人数",
        "日均咨询留资人数", "T月日均咨询留资人数", "T-1月日均咨询留资人数",
        "私信咨询率", "T月私信咨询率", "T-1月私信咨询率", "咨询留资率", "T月咨询留资率", "T-1月咨询留资率",
        "私信转化率", "T月私信转化率", "T-1月私信转化率",
    ],
    "经销商ID": [],
}
OUTPUT_CONTRACT_ORDER["经销商ID"] = ["经销商ID", "门店名", "层级"] + OUTPUT_CONTRACT_ORDER["层级"][1:]

# --- Registry Validation ---
def _validate_registry_closure() -> None:
    """Ensures all derived dependencies are registered as base fields."""
    base = {s.name for s in BASE_FIELDS_REGISTRY}
    used: set[str] = set()
    for spec in DERIVED_SPECS:
        if spec.num: used.add(spec.num)
        if spec.den: used.add(spec.den)
    missing = sorted(c for c in used if c not in base)
    if missing:
        msg = f"Derived deps missing in BASE_FIELDS_REGISTRY: {missing}"
        if _is_on("SETTLEMENT_STRICT_REGISTRY"):
            raise RuntimeError(msg)
        logger.warning("Registry Closure Warning: %s", msg)

# --- Pipeline Stages ---

def _sanitize_keys(df: pl.DataFrame, *, dimension: str, nsc_key: str) -> pl.DataFrame:
    df = df.with_columns([
        pl.col("层级").cast(pl.Utf8).fill_null("未知"),
        pl.col(nsc_key).cast(pl.Utf8).str.strip_chars(),
    ])
    if dimension == "经销商ID":
        return df.filter(pl.col(nsc_key).is_not_null() & (pl.col(nsc_key) != ""))
    return df

def _prepare_source_data(df: pl.DataFrame) -> pl.DataFrame:
    """聚合前尽可能物化关键合成列；若缺总金额，用 日均×有效天数 回推；对 T月 / T-1月 同步生效。"""

    def _sum_available(cols: list[str], alias: str):
        present = [c for c in cols if c in df.columns]
        if not present:
            return None
        expr = None
        for c in present:
            expr = _num(c) if expr is None else (expr + _num(c))
        return expr.alias(alias)

    exprs: list[pl.Expr] = []

    # —— 基期（无前缀）——
    e = _sum_available(["自然线索量", "付费线索量"], "线索总量")
    if e is not None: exprs.append(e)

    e = _sum_available(["车云店付费线索量", "区域加码付费线索量"], "总付费线索")
    if e is not None: exprs.append(e)

    if "超25分钟直播时长(分)" in df.columns:
        exprs.append((_num("超25分钟直播时长(分)") / 60.0).alias("超25分钟直播时长(小时)"))

    if "车云店+区域投放总金额" not in df.columns:
        if ("直播车云店+区域日均消耗" in df.columns) and ("有效天数" in df.columns):
            exprs.append((_num("直播车云店+区域日均消耗") * _num("有效天数")).alias("车云店+区域投放总金额"))

    # —— 周期（T月 / T-1月）——
    for p in ["T月", "T-1月"]:
        e = _sum_available([f"{p}自然线索量", f"{p}付费线索量"], f"{p}线索总量")
        if e is not None: exprs.append(e)

        e = _sum_available([f"{p}车云店付费线索", f"{p}区域加码付费线索"], f"{p}总付费线索")
        if e is not None: exprs.append(e)

        if f"{p}超25分钟直播时长(分)" in df.columns:
            exprs.append((_num(f"{p}超25分钟直播时长(分)") / 60.0).alias(f"{p}超25分钟直播时长(小时)"))

        if f"{p}车云店+区域投放总金额" not in df.columns:
            if (f"{p}直播车云店+区域日均消耗" in df.columns) and (f"{p}有效天数" in df.columns):
                exprs.append((_num(f"{p}直播车云店+区域日均消耗") * _num(f"{p}有效天数")).alias(f"{p}车云店+区域投放总金额"))

    return df.with_columns(exprs) if exprs else df

def _build_agg_expr(spec: BaseFieldSpec, *, row_filter: pl.Expr | None = None) -> pl.Expr:
    base = _num(spec.name)
    if spec.agg == "sum":
        if row_filter is not None:
            base = pl.when(row_filter).then(base).otherwise(0)
        return base.sum().alias(f"{spec.name}__sum")
    if spec.agg == "max":
        return base.max().alias(f"{spec.name}__max")
    raise ValueError(f"Unsupported aggregation method: {spec.agg}")



def _compute_derived_metrics(df: pl.DataFrame) -> pl.DataFrame:
    agg_kind = {s.name: s.agg for s in BASE_FIELDS_REGISTRY}
    existing = set(df.columns)

    def _agg_col(name: str | None) -> Tuple[str | None, pl.Expr | None]:
        if name is None: return None, None
        kind = agg_kind.get(name, "sum")
        suffix = "__sum" if kind == "sum" else "__max"
        colname = f"{name}{suffix}"
        if colname in existing:
            return colname, pl.col(colname)
        return colname, None

    exprs: list[pl.Expr] = []
    skipped_count = 0
    for s in DERIVED_SPECS:
        num_colname, num_expr = _agg_col(s.num)
        den_colname, den_expr = _agg_col(s.den)

        missing = []
        if num_colname and num_expr is None: missing.append(num_colname)
        if den_colname and den_expr is None: missing.append(den_colname)

        if missing:
            logger.warning("Derived '%s' skipped: missing columns: %s", s.name, sorted(missing))
            skipped_count += 1
            continue
        
        if s.kind == "ratio_total":
            exprs.append(SAFE_DIV(num_expr, den_expr).alias(s.name))
        else:
            raise ValueError(f"Unknown derived kind: {s.kind}")

    if skipped_count > 0:
        logger.info("Total derived metrics skipped: %d", skipped_count)

    return df.with_columns(exprs) if exprs else df

def _aggregate_and_derive(df: pl.DataFrame, *, group_by_keys: list[str], nsc_key: str) -> pl.DataFrame:
    row_filter = None
    if "层级" in group_by_keys and _is_on("LEVEL_SUM_EXCLUDE_EMPTY_NSC"):
        ns = pl.col(nsc_key).cast(pl.Utf8).str.strip_chars()
        row_filter = ns.is_not_null() & (ns != "")

    agg_exprs = [_build_agg_expr(s, row_filter=row_filter) for s in BASE_FIELDS_REGISTRY if s.name in df.columns]
    
    if "层级" in group_by_keys:
        ns = pl.col(nsc_key).cast(pl.Utf8).str.strip_chars()
        agg_exprs.append(ns.filter(ns.is_not_null() & (ns != "")).n_unique().alias("level_nsc_count"))
    
    grouped = df.group_by(group_by_keys, maintain_order=True).agg(agg_exprs)
    grouped = _ensure_aggregates_presence(grouped)
    derived = _compute_derived_metrics(grouped)
    return derived

def _ensure_aggregates_presence(df: pl.DataFrame) -> pl.DataFrame:
    """对 BASE_FIELDS_REGISTRY 声明过的列，确保对应 __sum/__max 聚合产物存在；如缺则补 0。"""
    have = set(df.columns)
    exprs: list[pl.Expr] = []
    for spec in BASE_FIELDS_REGISTRY:
        suffix = "__sum" if spec.agg == "sum" else "__max"
        col = f"{spec.name}{suffix}"
        if col not in have:
            exprs.append(pl.lit(0.0).alias(col))
    return df.with_columns(exprs) if exprs else df

def _apply_level_normalization(df: pl.DataFrame) -> pl.DataFrame:
    if "level_nsc_count" not in df.columns: return df
    exprs = []
    # 1. Normalize base metrics from their sums
    for name in NORMALIZABLE_FIELDS:
        if f"{name}__sum" in df.columns:
            exprs.append(SAFE_DIV(pl.col(f"{name}__sum"), pl.col("level_nsc_count")).alias(name))
    # 2. Normalize derived daily-average metrics from their calculated values
    for name in DERIVED_LEVEL_NORMALIZE:
        if name in df.columns:
            exprs.append(SAFE_DIV(pl.col(name), pl.col("level_nsc_count")).alias(name))
            
    return df.with_columns(exprs) if exprs else df

def _materialize_public_from_sums(df: pl.DataFrame, *, order: list[str]) -> pl.DataFrame:
    exprs = []
    cols = set(df.columns)
    for name in order:
        src_sum, src_max = f"{name}__sum", f"{name}__max"
        if name not in cols:
            if src_sum in cols:
                exprs.append(pl.col(src_sum).alias(name))
            elif src_max in cols:
                exprs.append(pl.col(src_max).alias(name))
    return df.with_columns(exprs) if exprs else df

def _apply_final_aliases(df: pl.DataFrame) -> pl.DataFrame:
    renames = {src: dst for src, dst in ALIAS_FINAL_MAP.items() if src in df.columns and dst not in df.columns}
    return df.rename(renames)

def _select_in_contract(df: pl.DataFrame, *, order: list[str], expose_debug_cols: bool = False) -> pl.DataFrame:
    cols = [c for c in order if c in df.columns]
    missing_in_output = [c for c in order if c not in df.columns]
    if missing_in_output:
        logger.info("Contract check: columns in contract but missing from final output: %s", missing_in_output)

    if expose_debug_cols:
        debug_cols = sorted([c for c in df.columns if c.endswith(("__sum", "__max"))])
        if "level_nsc_count" in df.columns:
            debug_cols.append("level_nsc_count")
        cols.extend(c for c in debug_cols if c not in cols)
    return df.select(cols)

# --- Main Entry Point ---
def compute_settlement_cn(
    df: pl.DataFrame | pl.LazyFrame,
    dimension: str,
    *,
    expose_debug_cols: bool = False
) -> pl.DataFrame:
    DIM_ALIAS = {
        "level": "层级", "LEVEL": "层级",
        "nsccode": "经销商ID", "NSC_CODE": "经销商ID",
        "dealer": "经销商ID", "DEALER": "经销商ID",
    }
    dimension = DIM_ALIAS.get(dimension, dimension)
    if dimension not in {"层级", "经销商ID"}:
        raise ValueError("dimension must be '层级' or '经销商ID'")
    
    nsc_key = "经销商ID" if "经销商ID" in df.columns else ("NSC_CODE" if "NSC_CODE" in df.columns else None)
    if not nsc_key: raise ValueError("Missing key column: requires '经销商ID' or 'NSC_CODE'")
    
    if isinstance(df, pl.LazyFrame): df = df.collect()

    _validate_registry_closure()

    group_by_keys: List[str] = []
    if dimension == "层级":
        group_by_keys = ["层级"]
    elif dimension == "经销商ID":
        group_by_keys = [nsc_key]
        store_name_col = "门店名" if "门店名" in df.columns else ("门店名称" if "门店名称" in df.columns else None)
        if store_name_col:
            group_by_keys.append(store_name_col)
        if "层级" in df.columns:
            group_by_keys.append("层级")

    # --- Pipeline ---
    sanitized = _sanitize_keys(df, dimension=dimension, nsc_key=nsc_key)
    prepared = _prepare_source_data(sanitized)
    aggregated = _aggregate_and_derive(prepared, group_by_keys=group_by_keys, nsc_key=nsc_key)
    
    materialized = aggregated
    if dimension == "经销商ID":
        materialized = _materialize_public_from_sums(aggregated, order=OUTPUT_CONTRACT_ORDER["经销商ID"])

    normalized = materialized
    if dimension == "层级":
        if _is_on("LEVEL_NORMALIZE_BY_NSC"):
            normalized = _apply_level_normalization(materialized)
        else:
            normalized = _materialize_public_from_sums(materialized, order=OUTPUT_CONTRACT_ORDER["层级"])

    finalized = _apply_final_aliases(normalized)
    
    if nsc_key == "NSC_CODE" and "NSC_CODE" in finalized.columns:
        finalized = finalized.rename({"NSC_CODE": "经销商ID"})
    if "门店名称" in finalized.columns and "门店名" not in finalized.columns:
         finalized = finalized.rename({"门店名称": "门店名"})
        
    return _select_in_contract(
        finalized,
        order=OUTPUT_CONTRACT_ORDER[dimension],
        expose_debug_cols=expose_debug_cols
    )
