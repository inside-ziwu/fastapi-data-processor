"""Settlement (结算) computations on the final wide table (Chinese columns).

Input: 日级宽表（含 经销商ID、日期、period、T月有效天数、T-1月有效天数、各指标）
Output: 按经销商ID聚合后的结算表（两个月窗口 T + T-1）
"""

from __future__ import annotations

import logging
import os
import polars as pl
from typing import Iterable

METRICS_TO_NORMALIZE: list[str] = [
    "自然线索量", "付费线索量", "车云店+区域投放总金额",
    "直播车云店+区域日均消耗", "T月直播车云店+区域日均消耗", "T-1月直播车云店+区域日均消耗",
    "直播车云店+区域付费线索量", "T月直播车云店+区域付费线索量", "T-1月直播车云店+区域付费线索量",
    "日均有效（25min以上）时长（h）", "T月日均有效（25min以上）时长（h）", "T-1月日均有效（25min以上）时长（h）",
    "直播线索量", "T月直播线索量", "T-1月直播线索量",
    "锚点曝光量", "T月锚点曝光量", "T-1月锚点曝光量",
    "组件点击次数", "T月组件点击次数", "T-1月组件点击次数",
    "组件留资人数（获取线索量）", "T月组件留资人数（获取线索量）", "T-1月组件留资人数（获取线索量）",
    "日均进私人数", "T月日均进私人数", "T-1月日均进私人数",
    "日均私信开口人数", "T月日均私信开口人数", "T-1月日均私信开口人数",
    "日均咨询留资人数", "T月日均咨询留资人数", "T-1月日均咨询留资人数",
    "短视频条数", "T月短视频条数", "T-1月短视频条数",
    "短视频播放量", "T月短视频播放量", "T-1月短视频播放量",
    "直播时长", "T月直播时长", "T-1月直播时长",
]

def _is_level_normalization_enabled() -> bool:
    v = (os.getenv("LEVEL_NORMALIZE_BY_NSC") or "").lower()
    enabled = v in {"1", "true", "yes", "on"}
    if enabled:
        logging.getLogger(__name__).info("New 'level' normalization logic is ENABLED via environment variable.")
    return enabled

def _validate_and_clean_inputs(
    df: pl.DataFrame | pl.LazyFrame,
    metrics: Iterable[str],
) -> tuple[pl.DataFrame | pl.LazyFrame, str]:
    NSC_KEY = "经销商ID" if "经销商ID" in df.columns else ("NSC_CODE" if "NSC_CODE" in df.columns else None)
    if NSC_KEY is None:
        raise ValueError("Missing NSC key column: requires '经销商ID' or 'NSC_CODE'")

    present_metrics = [m for m in metrics if m in df.columns]
    required_present = {"层级", NSC_KEY, *present_metrics}

    missing = [c for c in required_present if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns for level normalization: {missing}")

    df = df.with_columns([
        pl.col("层级").cast(pl.Utf8).fill_null("未知"),
        pl.col(NSC_KEY).cast(pl.Utf8),
        *[pl.col(c).cast(pl.Float64).fill_null(0) for c in present_metrics],
    ])
    return df, NSC_KEY

def _compute_settlement_level_normalized(
    df: pl.DataFrame | pl.LazyFrame,
    *, # force keyword arguments
    metrics_to_normalize: list[str] = METRICS_TO_NORMALIZE,
    expose_debug_cols: bool = False,
) -> pl.DataFrame | pl.LazyFrame:

    df, NSC_KEY = _validate_and_clean_inputs(df, metrics_to_normalize)

    agg_counts = df.group_by("层级").agg(
        pl.col(NSC_KEY).n_unique().alias("level_nsc_count")
    )

    sum_exprs = [pl.col(c).sum().alias(f"{c}__sum") for c in metrics_to_normalize if c in df.columns]
    agg_sums = df.group_by("层级").agg(sum_exprs)

    merged = agg_sums.join(agg_counts, on="层级", how="inner")

    norm_exprs = [
        pl.when(pl.col("level_nsc_count") > 0)
          .then(pl.col(f"{c}__sum") / pl.col("level_nsc_count"))
          .otherwise(0.0)
          .alias(c)
        for c in metrics_to_normalize if f"{c}__sum" in merged.columns
    ]
    
    out = merged.with_columns(norm_exprs)

    def SUM(col_name: str) -> pl.Expr:
        return pl.col(f"{col_name}__sum")

    derived_exprs: list[pl.Expr] = []
    # Dynamically create derived expressions based on available columns
    if "组件点击次数__sum" in out.columns and "锚点曝光量__sum" in out.columns:
        derived_exprs.append(_safe_div(SUM("组件点击次数"), SUM("锚点曝光量")).alias("CTR_组件"))
    if "T月组件点击次数__sum" in out.columns and "T月锚点曝光量__sum" in out.columns:
        derived_exprs.append(_safe_div(SUM("T月组件点击次数"), SUM("T月锚点曝光量")).alias("CTR_组件_T"))
    if "T-1月组件点击次数__sum" in out.columns and "T-1月锚点曝光量__sum" in out.columns:
        derived_exprs.append(_safe_div(SUM("T-1月组件点击次数"), SUM("T-1月锚点曝光量")).alias("CTR_组件_T_1"))
    
    if "组件留资人数（获取线索量）__sum" in out.columns and "组件点击次数__sum" in out.columns:
        derived_exprs.append(_safe_div(SUM("组件留资人数（获取线索量）"), SUM("组件点击次数")).alias("组件留资率"))

    if out.columns and derived_exprs:
        out = out.with_columns(derived_exprs)

    # Final column selection
    final_cols = ["层级"]
    final_cols.extend([c for c in metrics_to_normalize if c in out.columns])
    final_cols.extend([e.meta.output_name() for e in derived_exprs])

    if expose_debug_cols:
        final_cols.append("level_nsc_count")
        final_cols.extend([f"{c}__sum" for c in metrics_to_normalize if f"{c}__sum" in out.columns])

    return out.select(final_cols)

def _sum_period(col: str, tag: str) -> pl.Expr:
    if tag == "both":
        return (
            pl.when(pl.col("period").is_in(["T", "T-1"]))
            .then(pl.col(col))
            .otherwise(0)
            .sum()
            .alias(col)
        )
    elif tag == "T":
        return (
            pl.when(pl.col("period") == "T").then(pl.col(col)).otherwise(0).sum()
        )
    elif tag == "T-1":
        return (
            pl.when(pl.col("period") == "T-1").then(pl.col(col)).otherwise(0).sum()
        )
    else:
        raise ValueError("tag must be one of: both, T, T-1")


def _safe_div(num: pl.Expr, den: pl.Expr) -> pl.Expr:
    numf = num.cast(pl.Float64)
    denf = den.cast(pl.Float64)
    return pl.when((denf != 0) & denf.is_not_null()).then(numf / denf).otherwise(0.0)


def compute_settlement_cn(df: pl.DataFrame, dimension: str | None = None) -> pl.DataFrame:
    if df.is_empty():
        return df

    dim = (dimension or "经销商ID").strip().lower()
    if dim in {"nsc_code", "nsc", "id", "经销商id", "经销商ID".lower()}:
        id_col = "经销商ID"
        group_mode = "id"
    elif dim in {"level", "层级"}:
        id_col = "层级"
        group_mode = "level"
    else:
        id_col = "经销商ID"
        group_mode = "id"

    if group_mode == "level" and _is_level_normalization_enabled():
        return _compute_settlement_level_normalized(df)

    if id_col not in df.columns:
        if group_mode == "level":
            logging.getLogger(__name__).warning("按层级聚合但缺少'层级'列，已跳过该聚合")
            return pl.DataFrame()
        else:
            raise ValueError(f"缺少列: {id_col}")

    if group_mode == "level":
        if "层级" not in df.columns:
            logging.getLogger(__name__).warning("按层级聚合但缺少'层级'列，已跳过该聚合")
            return pl.DataFrame()
        df = df.with_columns(
            pl.when(
                pl.col("层级").is_null() | (pl.col("层级").cast(pl.Utf8).str.strip_chars() == "")
            )
            .then(pl.lit("未知"))
            .otherwise(pl.col("层级"))
            .alias("层级")
        )

    def pick(*cands: str) -> str | None:
        for c in cands:
            if c in df.columns:
                return c
        return None

    spend_src = pick("spending_net", "Spending(Net)")
    if spend_src is None:
        avail = ", ".join(df.columns)
        raise ValueError(
            f"结算失败：缺少投放金额列。需存在 'spending_net'（推荐）或 'Spending(Net)'。Available: [{avail}]"
        )

    logger = logging.getLogger(__name__)

    def _warn_enabled() -> bool:
        val = os.getenv("PROCESSOR_WARN_OPTIONAL_FIELDS", "1").strip().lower()
        return val in {"1", "true", "yes", "on"}

    def warn_missing(label: str, cand_display: str, src_col: str | None) -> None:
        if not _warn_enabled():
            return
        if src_col is None or src_col not in df.columns:
            logger.warning(
                f"结算提示：缺失{label}来源列({cand_display})。相关指标将按0计算。"
            )

    metrics_both = {
        "自然线索量": pick("natural_leads", "自然线索"),
        "付费线索量": pick("paid_leads", "付费线索"),
        "车云店+区域投放总金额": spend_src,
        "直播时长": pick("live_effective_hours", "直播有效时长(小时)"),
        "直播线索量": pick("live_leads", "直播线索量"),
        "锚点曝光量": pick("anchor_exposure", "锚点曝光量"),
        "组件点击次数": pick("component_clicks", "组件点击次数"),
        "组件留资人数（获取线索量）": pick("short_video_leads", "组件留资人数（获取线索量）"),
        "短视频条数": pick("short_video_count", "短视频条数"),
        "短视频播放量": pick("short_video_plays", "短视频播放量"),
        "有效直播场次(总)": pick("effective_live_sessions", "有效直播场次"),
        "曝光人数(总)": pick("exposures", "曝光人数"),
        "场观(总)": pick("viewers", "场观"),
        "小风车点击次数(总)": pick("small_wheel_clicks", "小风车点击次数"),
        "小风车留资量(总)": pick("small_wheel_leads", "小风车留资量"),
        "进私人数(总)": pick("enter_private_count", "进私人数"),
        "私信开口人数(总)": pick("private_open_count", "私信开口人数"),
        "咨询留资人数(总)": pick("private_leads_count", "咨询留资人数"),
        "超25分钟直播时长(分)(总)": pick("over25_min_live_mins", "超25分钟直播时长(分)"),
        "车云店付费线索(总)": pick("store_paid_leads", "车云店付费线索"),
        "区域加码付费线索(总)": pick("area_paid_leads", "区域加码付费线索"),
        "本地线索量(总)": pick("local_leads", "本地线索量"),
    }
    metrics_T = {
        "T月直播时长": pick("live_effective_hours", "直播有效时长(小时)"),
        "T月直播线索量": pick("live_leads", "直播线索量"),
        "T月锚点曝光量": pick("anchor_exposure", "锚点曝光量"),
        "T月组件点击次数": pick("component_clicks", "组件点击次数"),
        "T月组件留资人数（获取线索量）": pick("short_video_leads", "组件留资人数（获取线索量）"),
        "T月短视频条数": pick("short_video_count", "短视频条数"),
        "T月短视频播放量": pick("short_video_plays", "短视频播放量"),
        "T月 车云店+区域投放总金额": spend_src,
        "T月有效直播场次(总)": pick("effective_live_sessions", "有效直播场次"),
        "T月曝光人数(总)": pick("exposures", "曝光人数"),
        "T月场观(总)": pick("viewers", "场观"),
        "T月小风车点击次数(总)": pick("small_wheel_clicks", "小风车点击次数"),
        "T月小风车留资量(总)": pick("small_wheel_leads", "小风车留资量"),
        "T月超25分钟直播时长(分)(总)": pick("over25_min_live_mins", "超25分钟直播时长(分)"),
        "T月 车云店付费线索": pick("store_paid_leads", "车云店付费线索"),
        "T月 区域加码付费线索": pick("area_paid_leads", "区域加码付费线索"),
        "T月进私人数(总)": pick("enter_private_count", "进私人数"),
        "T月私信开口人数(总)": pick("private_open_count", "私信开口人数"),
        "T月咨询留资人数(总)": pick("private_leads_count", "咨询留资人数"),
    }
    metrics_T1 = {
        "T-1月直播时长": pick("live_effective_hours", "直播有效时长(小时)"),
        "T-1月直播线索量": pick("live_leads", "直播线索量"),
        "T-1月锚点曝光量": pick("anchor_exposure", "锚点曝光量"),
        "T-1月组件点击次数": pick("component_clicks", "组件点击次数"),
        "T-1月组件留资人数（获取线索量）": pick("short_video_leads", "组件留资人数（获取线索量）"),
        "T-1月短视频条数": pick("short_video_count", "短视频条数"),
        "T-1月短视频播放量": pick("short_video_plays", "短视频播放量"),
        "T-1月 车云店+区域投放总金额": spend_src,
        "T-1月有效直播场次(总)": pick("effective_live_sessions", "有效直播场次"),
        "T-1月曝光人数(总)": pick("exposures", "曝光人数"),
        "T-1月场观(总)": pick("viewers", "场观"),
        "T-1月小风车点击次数(总)": pick("small_wheel_clicks", "小风车点击次数"),
        "T-1月小风车留资量(总)": pick("small_wheel_leads", "小风车留资量"),
        "T-1月超25分钟直播时长(分)(总)": pick("over25_min_live_mins", "超25分钟直播时长(分)"),
        "T-1月 车云店付费线索": pick("store_paid_leads", "车云店付费线索"),
        "T-1月 区域加码付费线索": pick("area_paid_leads", "区域加码付费线索"),
        "T-1月进私人数(总)": pick("enter_private_count", "进私人数"),
        "T-1月私信开口人数(总)": pick("private_open_count", "私信开口人数"),
        "T-1月咨询留资人数(总)": pick("private_leads_count", "咨询留资人数"),
    }
    agg_exprs: list[pl.Expr] = []
    for out, src in metrics_both.items():
        if src in df.columns:
            agg_exprs.append(_sum_period(src, "both").alias(out))
    for out, src in metrics_T.items():
        if src in df.columns:
            agg_exprs.append(_sum_period(src, "T").alias(out))
    for out, src in metrics_T1.items():
        if src in df.columns:
            agg_exprs.append(_sum_period(src, "T-1").alias(out))
    eff_cols = []
    if "T月有效天数" in df.columns:
        eff_cols.append(pl.col("T月有效天数").max().alias("T月有效天数"))
    if "T-1月有效天数" in df.columns:
        eff_cols.append(pl.col("T-1月有效天数").max().alias("T-1月有效天数"))
    extra_dims: list[pl.Expr] = []
    if group_mode == "id":
        if "门店名" in df.columns:
            extra_dims.append(pl.col("门店名").first().alias("门店名"))
        if "层级" in df.columns:
            extra_dims.append(pl.col("层级").first().alias("层级"))
    grouped = df.group_by(id_col).agg(extra_dims + agg_exprs + eff_cols)
    def col(name: str) -> pl.Expr:
        return pl.col(name) if name in grouped.columns else pl.lit(0.0)
    total_eff_days = col("T月有效天数") + col("T-1月有效天数")
    result = grouped.with_columns(
        _safe_div(col("车云店+区域投放总金额"), col("自然线索量") + col("付费线索量")).alias("车云店+区域综合CPL"),
        _safe_div(col("车云店+区域投放总金额"), col("车云店付费线索(总)") + col("区域加码付费线索(总)")).alias("付费CPL（车云店+区域）"),
        _safe_div(col("车云店+区域投放总金额"), col("车云店付费线索(总)") + col("区域加码付费线索(总)")).alias("直播付费CPL"),
        _safe_div(col("T月 车云店+区域投放总金额"), col("T月 车云店付费线索") + col("T月 区域加码付费线索")).alias("T月直播付费CPL"),
        _safe_div(col("T-1月 车云店+区域投放总金额"), col("T-1月 车云店付费线索") + col("T-1月 区域加码付费线索")).alias("T-1月直播付费CPL"),
        _safe_div(col("本地线索量(总)"), col("自然线索量") + col("付费线索量")).alias("本地线索占比"),
        _safe_div(col("车云店+区域投放总金额"), total_eff_days).alias("直播车云店+区域日均消耗"),
        _safe_div(col("T月 车云店+区域投放总金额"), col("T月有效天数")).alias("T月直播车云店+区域日均消耗"),
        _safe_div(col("T-1月 车云店+区域投放总金额"), col("T-1月有效天数")).alias("T-1月直播车云店+区域日均消耗"),
        _safe_div(col("车云店付费线索(总)") + col("区域加码付费线索(总)"), total_eff_days).alias("直播车云店+区域付费线索量日均"),
        _safe_div(col("T月 车云店付费线索") + col("T月 区域加码付费线索"), col("T月有效天数")).alias("T月直播车云店+区域付费线索量日均"),
        _safe_div(col("T-1月 车云店付费线索") + col("T-1月 区域加码付费线索"), col("T-1月有效天数")).alias("T-1月直播车云店+区域付费线索量日均"),
        _safe_div((col("超25分钟直播时长(分)(总)") / 60.0), total_eff_days).alias("日均有效（25min以上）时长（h）"),
        _safe_div((col("T月超25分钟直播时长(分)(总)") / 60.0), col("T月有效天数")).alias("T月日均有效（25min以上）时长（h）"),
        _safe_div((col("T-1月超25分钟直播时长(分)(总)") / 60.0), col("T-1月有效天数")).alias("T-1月日均有效（25min以上）时长（h）"),
        _safe_div(col("曝光人数(总)"), col("有效直播场次(总)")).alias("场均曝光人数"),
        _safe_div(col("T月曝光人数(总)"), col("T月有效直播场次(总)")).alias("T月场均曝光人数"),
        _safe_div(col("T-1月曝光人数(总)"), col("T-1月有效直播场次(总)")).alias("T-1月场均曝光人数"),
        _safe_div(col("场观(总)"), col("曝光人数(总)")).alias("曝光进入率"),
        _safe_div(col("T月场观(总)"), col("T月曝光人数(总)")).alias("T月曝光进入率"),
        _safe_div(col("T-1月场观(总)"), col("T-1月曝光人数(总)")).alias("T-1月曝光进入率"),
        _safe_div(col("场观(总)"), col("有效直播场次(总)")).alias("场均场观"),
        _safe_div(col("T月场观(总)"), col("T月有效直播场次(总)")).alias("T月场均场观"),
        _safe_div(col("T-1月场观(总)"), col("T-1月有效直播场次(总)")).alias("T-1月场均场观"),
        _safe_div(col("小风车点击次数(总)"), col("场观(总)")).alias("小风车点击率"),
        _safe_div(col("T月小风车点击次数(总)"), col("T月场观(总)")).alias("T月小风车点击率"),
        _safe_div(col("T-1月小风车点击次数(总)"), col("T-1月场观(总)")).alias("T-1月小风车点击率"),
        _safe_div(col("小风车留资量(总)"), col("小风车点击次数(总)")).alias("小风车点击留资率"),
        _safe_div(col("T月小风车留资量(总)"), col("T月小风车点击次数(总)")).alias("T月小风车点击留资率"),
        _safe_div(col("T-1月小风车留资量(总)"), col("T-1月小风车点击次数(总)")).alias("T-1月小风车点击留资率"),
        _safe_div(col("小风车留资量(总)"), col("有效直播场次(总)")).alias("场均小风车留资量"),
        _safe_div(col("T月小风车留资量(总)"), col("T月有效直播场次(总)")).alias("T月场均小风车留资量"),
        _safe_div(col("T-1月小风车留资量(总)"), col("T-1月有效直播场次(总)")).alias("T-1月场均小风车留资量"),
        _safe_div(col("小风车点击次数(总)"), col("有效直播场次(总)")).alias("场均小风车点击次数"),
        _safe_div(col("T月小风车点击次数(总)"), col("T月有效直播场次(总)")).alias("T月场均小风车点击次数"),
        _safe_div(col("T-1月小风车点击次数(总)"), col("T-1月有效直播场次(总)")).alias("T-1月场均小风车点击次数"),
        _safe_div(col("组件点击次数"), col("锚点曝光量")).alias("组件点击率"),
        _safe_div(col("T月组件点击次数"), col("T月锚点曝光量")).alias("T月组件点击率"),
        _safe_div(col("T-1月组件点击次数"), col("T-1月锚点曝光量")).alias("T-1月组件点击率"),
        _safe_div(col("组件留资人数（获取线索量）"), col("锚点曝光量")).alias("组件留资率"),
        _safe_div(col("T月组件留资人数（获取线索量）"), col("T月锚点曝光量")).alias("T月组件留资率"),
        _safe_div(col("T-1月组件留资人数（获取线索量）"), col("T-1月锚点曝光量")).alias("T-1月组件留资率"),
        _safe_div(col("进私人数(总)"), total_eff_days).alias("日均进私人数"),
        _safe_div(col("T月进私人数(总)"), col("T月有效天数")).alias("T月日均进私人数"),
        _safe_div(col("T-1月进私人数(总)"), col("T-1月有效天数")).alias("T-1月日均进私人数"),
        _safe_div(col("私信开口人数(总)"), total_eff_days).alias("日均私信开口人数"),
        _safe_div(col("T月私信开口人数(总)"), col("T月有效天数")).alias("T月日均私信开口人数"),
        _safe_div(col("T-1月私信开口人数(总)"), col("T-1月有效天数")).alias("T-1月日均私信开口人数"),
        _safe_div(col("咨询留资人数(总)"), total_eff_days).alias("日均咨询留资人数"),
        _safe_div(col("T月咨询留资人数(总)"), col("T月有效天数")).alias("T月日均咨询留资人数"),
        _safe_div(col("T-1月咨询留资人数(总)"), col("T-1月有效天数")).alias("T-1月日均咨询留资人数"),
        _safe_div(col("私信开口人数(总)"), col("进私人数(总)")).alias("私信咨询率"),
        _safe_div(col("T月私信开口人数(总)"), col("T月进私人数(总)")).alias("T月私信咨询率"),
        _safe_div(col("T-1月私信开口人数(总)"), col("T-1月进私人数(总)")).alias("T-1月私信咨询率"),
        _safe_div(col("咨询留资人数(总)"), col("私信开口人数(总)")).alias("咨询留资率"),
        _safe_div(col("T月咨询留资人数(总)"), col("T月私信开口人数(总)")).alias("T月咨询留资率"),
        _safe_div(col("T-1月咨询留资人数(总)"), col("T-1月私信开口人数(总)")).alias("T-1月咨询留资率"),
        _safe_div(col("咨询留资人数(总)"), col("进私人数(总)")).alias("私信转化率"),
        _safe_div(col("T月咨询留资人数(总)"), col("T月进私人数(总)")).alias("T月私信转化率"),
        _safe_div(col("T-1月咨询留资人数(总)"), col("T-1月进私人数(总)")).alias("T-1月私信转化率"),
    )
    result = result.with_columns(
        (col("车云店付费线索(总)") + col("区域加码付费线索(总)")).alias("直播车云店+区域付费线索量"),
        (col("T月 车云店付费线索") + col("T月 区域加码付费线索")).alias("T月直播车云店+区域付费线索量"),
        (col("T-1月 车云店付费线索") + col("T-1月 区域加码付费线索")).alias("T-1月直播车云店+区域付费线索量"),
    )
    ensure_zero_cols = []
    expected_cols = list(metrics_both.keys()) + list(metrics_T.keys()) + list(metrics_T1.keys()) + [
        "车云店+区域综合CPL", "付费CPL（车云店+区域）", "直播付费CPL", "T月直播付费CPL", "T-1月直播付费CPL",
        "本地线索占比",
        "直播车云店+区域日均消耗", "T月直播车云店+区域日均消耗", "T-1月直播车云店+区域日均消耗",
        "直播车云店+区域付费线索量", "T月直播车云店+区域付费线索量", "T-1月直播车云店+区域付费线索量",
        "直播车云店+区域付费线索量日均", "T月直播车云店+区域付费线索量日均", "T-1月直播车云店+区域付费线索量日均",
        "日均有效（25min以上）时长（h）", "T月日均有效（25min以上）时长（h）", "T-1月日均有效（25min以上）时长（h）",
        "场均曝光人数", "T月场均曝光人数", "T-1月场均曝光人数",
        "曝光进入率", "T月曝光进入率", "T-1月曝光进入率",
        "场均场观", "T月场均场观", "T-1月场均场观",
        "小风车点击率", "T月小风车点击率", "T-1月小风车点击率",
        "小风车点击留资率", "T月小风车点击留资率", "T-1月小风车点击留资率",
        "场均小风车留资量", "T月场均小风车留资量", "T-1月场均小风车留资量",
        "场均小风车点击次数", "T月场均小风车点击次数", "T-1月场均小风车点击次数",
        "组件点击率", "T月组件点击率", "T-1月组件点击率",
        "组件留资率", "T月组件留资率", "T-1月组件留资率",
    ]
    for cname in expected_cols:
        if cname not in result.columns:
            ensure_zero_cols.append(pl.lit(0.0).alias(cname))
    if ensure_zero_cols:
        result = result.with_columns(ensure_zero_cols)

    if group_mode == "level" and "层级" in result.columns:
        result = (
            result
            .with_columns(
                pl.when(pl.col("层级") == "未知").then(1).otherwise(0).alias("_unk_sort")
            )
            .sort(by=["_unk_sort", "层级"], descending=[False, True])
            .drop(["_unk_sort"])
        )

    key_cols = [id_col]
    if group_mode == "id":
        if "门店名" in result.columns:
            key_cols.append("门店名")
        if "层级" in result.columns:
            key_cols.append("层级")
    else:
        if id_col == "层级" and "层级" not in key_cols:
            key_cols = ["层级"]

    ordered_metrics = [
        "自然线索量", "付费线索量", "车云店+区域投放总金额",
        "直播时长", "T月直播时长", "T-1月直播时长",
        "直播线索量", "T月直播线索量", "T-1月直播线索量",
        "锚点曝光量", "T月锚点曝光量", "T-1月锚点曝光量",
        "组件点击次数", "T月组件点击次数", "T-1月组件点击次数",
        "组件留资人数（获取线索量）", "T月组件留资人数（获取线索量）", "T-1月组件留资人数（获取线索量）",
        "短视频条数", "T月短视频条数", "T-1月短视频条数",
        "短视频播放量", "T月短视频播放量", "T-1月短视频播放量",
        "车云店+区域综合CPL", "付费CPL（车云店+区域）",
        "直播车云店+区域日均消耗", "T月直播车云店+区域日均消耗", "T-1月直播车云店+区域日均消耗",
        "直播车云店+区域付费线索量", "T月直播车云店+区域付费线索量", "T-1月直播车云店+区域付费线索量",
        "直播车云店+区域付费线索量日均", "T月直播车云店+区域付费线索量日均", "T-1月直播车云店+区域付费线索量日均",
        "直播付费CPL", "T月直播付费CPL", "T-1月直播付费CPL",
        "日均有效（25min以上）时长（h）", "T月日均有效（25min以上）时长（h）", "T-1月日均有效（25min以上）时长（h）",
        "场均曝光人数", "T月场均曝光人数", "T-1月场均曝光人数",
        "曝光进入率", "T月曝光进入率", "T-1月曝光进入率",
        "场均场观", "T月场均场观", "T-1月场均场观",
        "小风车点击率", "T月小风车点击率", "T-1月小风车点击率",
        "小风车点击留资率", "T月小风车点击留资率", "T-1月小风车点击留资率",
        "场均小风车留资量", "T月场均小风车留资量", "T-1月场均小风车留资量",
        "场均小风车点击次数", "T月场均小风车点击次数", "T-1月场均小风车点击次数",
        "组件点击率", "T月组件点击率", "T-1月组件点击率",
        "组件留资率", "T月组件留资率", "T-1月组件留资率",
        "本地线索占比",
        "日均进私人数", "T月日均进私人数", "T-1月日均进私人数",
        "日均私信开口人数", "T月日均私信开口人数", "T-1月日均私信开口人数",
        "日均咨询留资人数", "T月日均咨询留资人数", "T-1月日均咨询留资人数",
        "私信咨询率", "T月私信咨询率", "T-1月私信咨询率",
        "咨询留资率", "T月咨询留资率", "T-1月咨询留资率",
        "私信转化率", "T月私信转化率", "T-1月私信转化率",
    ]

    final_cols = key_cols + [c for c in ordered_metrics if c in result.columns]
    final_df = result.select(final_cols)

    if group_mode == "level" and "层级" in final_df.columns:
        try:
            num_cols = [
                c for c in final_df.columns
                if final_df.schema[c] in (pl.Float32, pl.Float64, pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64)
                and c != "层级"
            ]
            if num_cols:
                final_df = final_df.with_columns(
                    pl.sum_horizontal([pl.col(c).cast(pl.Float64, strict=False) for c in num_cols]).alias("__row_sum__")
                )
                final_df = final_df.filter(~((pl.col("层级") == "未知") & (pl.col("__row_sum__") <= 0.0)))
                final_df = final_df.drop("__row_sum__")
        except Exception:
            pass
        final_df = (
            final_df
            .with_columns(
                pl.when(pl.col("层级") == "未知").then(1).otherwise(0).alias("_unk_sort")
            )
            .sort(by=["_unk_sort", "层级"], descending=[False, True])
            .drop(["_unk_sort"])
        )
    return final_df