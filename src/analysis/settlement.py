"""Settlement (结算) computations on the final wide table (Chinese columns).

Input: 日级宽表（含 经销商ID、日期、period、T月有效天数、T-1月有效天数、各指标）
Output: 按经销商ID聚合后的结算表（两个月窗口 T + T-1）
"""

from __future__ import annotations

import logging
import os
import polars as pl


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
    # Cast to Float64 to avoid integer-division surprises and ensure stable dtype
    numf = num.cast(pl.Float64)
    denf = den.cast(pl.Float64)
    return pl.when((denf != 0) & denf.is_not_null()).then(numf / denf).otherwise(0.0)


def compute_settlement_cn(df: pl.DataFrame, dimension: str | None = None) -> pl.DataFrame:
    """Compute settlement metrics on the final wide table.

    dimension:
      - 'NSC_CODE' / '经销商ID' / 'id' -> 按经销商ID聚合，并补充 门店名、层级 两列
      - 'level' / '层级' -> 按层级聚合
      - None -> 默认按经销商ID
    """
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

    if id_col not in df.columns:
        # 当按层级聚合但缺少层级列时，按规范创建“未知”层级，保证不崩溃且有兜底分组
        if group_mode == "level":
            df = df.with_columns(pl.lit("未知").alias("层级"))
        else:
            raise ValueError(f"缺少列: {id_col}")

    # 层级兜底：仅将空/缺失置为“未知”，不限制取值集合，不改动非空原值
    if group_mode == "level":
        if "层级" not in df.columns:
            df = df.with_columns(pl.lit("未知").alias("层级"))
        else:
            df = df.with_columns(
                pl.when(pl.col("层级").is_null() | (pl.col("层级").cast(pl.Utf8).str.strip_chars() == ""))
                .then(pl.lit("未知"))
                .otherwise(pl.col("层级"))
                .alias("层级")
            )

    # Resolve source columns: prefer clean english, fallback to Chinese UI names
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

    # Optional sources detection and warnings
    logger = logging.getLogger(__name__)

    def _warn_enabled() -> bool:
        """Env switch: PROCESSOR_WARN_OPTIONAL_FIELDS controls whether to emit warnings.

        Accepts: 1/true/yes/on to enable; 0/false/no/off to disable. Default: enabled.
        """
        val = os.getenv("PROCESSOR_WARN_OPTIONAL_FIELDS", "1").strip().lower()
        return val in {"1", "true", "yes", "on"}

    def warn_missing(label: str, cand_display: str, src_col: str | None) -> None:
        if not _warn_enabled():
            return
        if src_col is None or src_col not in df.columns:
            logger.warning(
                f"结算提示：缺失{label}来源列({cand_display})。相关指标将按0计算。"
            )

    small_wheel_src = pick("small_wheel_clicks", "小风车点击次数")
    warn_missing("小风车点击", "small_wheel_clicks/小风车点击次数", small_wheel_src)

    exposures_src = pick("exposures", "曝光人数")
    warn_missing("曝光人数", "exposures/曝光人数", exposures_src)

    viewers_src = pick("viewers", "场观")
    warn_missing("场观", "viewers/场观", viewers_src)

    eff_sessions_src = pick("effective_live_sessions", "有效直播场次")
    warn_missing("有效直播场次", "effective_live_sessions/有效直播场次", eff_sessions_src)

    comp_clicks_src = pick("component_clicks", "组件点击次数")
    warn_missing("组件点击次数", "component_clicks/组件点击次数", comp_clicks_src)

    anchor_exp_src = pick("anchor_exposure", "锚点曝光量")
    warn_missing("锚点曝光量", "anchor_exposure/锚点曝光量", anchor_exp_src)

    comp_leads_src = pick("short_video_leads", "组件留资人数（获取线索量）")
    warn_missing("组件留资人数（获取线索量）", "short_video_leads/组件留资人数（获取线索量）", comp_leads_src)

    small_wheel_leads_src = pick("small_wheel_leads", "小风车留资量")
    warn_missing("小风车留资量", "small_wheel_leads/小风车留资量", small_wheel_leads_src)

    over25_src = pick("over25_min_live_mins", "超25分钟直播时长(分)")
    warn_missing("超25分钟直播时长(分)", "over25_min_live_mins/超25分钟直播时长(分)", over25_src)

    store_paid_src = pick("store_paid_leads", "车云店付费线索")
    area_paid_src = pick("area_paid_leads", "区域加码付费线索")
    if _warn_enabled():
        if (store_paid_src is None or store_paid_src not in df.columns) or (
            area_paid_src is None or area_paid_src not in df.columns
        ):
            logger.warning(
                "结算提示：缺失车云店/区域付费线索来源列(store_paid_leads/车云店付费线索 或 area_paid_leads/区域加码付费线索)。相关付费CPL与分项日均可能按0计算。"
            )

    natural_src = pick("natural_leads", "自然线索")
    paid_src = pick("paid_leads", "付费线索")
    if _warn_enabled():
        if (natural_src is None or natural_src not in df.columns) or (
            paid_src is None or paid_src not in df.columns
        ):
            logger.warning(
                "结算提示：缺失自然/付费线索来源列(natural_leads/自然线索 或 paid_leads/付费线索)。综合CPL与本地线索占比可能按0计算。"
            )

    local_src = pick("local_leads", "本地线索量")
    warn_missing("本地线索量", "local_leads/本地线索量", local_src)

    # 有效天数仅在某些数据流中提供；若缺失将影响日均类指标
    if _warn_enabled():
        if ("T月有效天数" not in df.columns) and ("T-1月有效天数" not in df.columns):
            logger.warning("结算提示：缺失有效天数列(T月有效天数/T-1月有效天数)。相关日均指标将按0计算。")

    # Diagnostics: pre-aggregation period distribution and key metric sums (env-controlled)
    def _diag_enabled() -> bool:
        val = os.getenv("PROCESSOR_DIAG", "1").strip().lower()
        return val in {"1", "true", "yes", "on"}

    # 诊断从核心移除：如需查看分子/分母合计，请使用 diagnostics 模块在调用处显式触发

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
        # Live session and related
        "有效直播场次(总)": pick("effective_live_sessions", "有效直播场次"),
        "曝光人数(总)": pick("exposures", "曝光人数"),
        "场观(总)": pick("viewers", "场观"),
        "小风车点击次数(总)": small_wheel_src,
        "小风车留资量(总)": pick("small_wheel_leads", "小风车留资量"),
        # Message
        "进私人数(总)": pick("enter_private_count", "进私人数"),
        "私信开口人数(总)": pick("private_open_count", "私信开口人数"),
        "咨询留资人数(总)": pick("private_leads_count", "咨询留资人数"),
        # 25min mins
        "超25分钟直播时长(分)(总)": pick("over25_min_live_mins", "超25分钟直播时长(分)"),
        # DR paid split
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
        "T月小风车点击次数(总)": small_wheel_src,
        "T月小风车留资量(总)": pick("small_wheel_leads", "小风车留资量"),
        "T月超25分钟直播时长(分)(总)": pick("over25_min_live_mins", "超25分钟直播时长(分)"),
        # DR paid split
        "T月 车云店付费线索": pick("store_paid_leads", "车云店付费线索"),
        "T月 区域加码付费线索": pick("area_paid_leads", "区域加码付费线索"),
        # Message
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
        "T-1月小风车点击次数(总)": small_wheel_src,
        "T-1月小风车留资量(总)": pick("small_wheel_leads", "小风车留资量"),
        "T-1月超25分钟直播时长(分)(总)": pick("over25_min_live_mins", "超25分钟直播时长(分)"),
        # DR paid split
        "T-1月 车云店付费线索": pick("store_paid_leads", "车云店付费线索"),
        "T-1月 区域加码付费线索": pick("area_paid_leads", "区域加码付费线索"),
        # Message
        "T-1月进私人数(总)": pick("enter_private_count", "进私人数"),
        "T-1月私信开口人数(总)": pick("private_open_count", "私信开口人数"),
        "T-1月咨询留资人数(总)": pick("private_leads_count", "咨询留资人数"),
    }

    agg_exprs: list[pl.Expr] = []
    # both months sums
    for out, src in metrics_both.items():
        if src in df.columns:
            agg_exprs.append(_sum_period(src, "both").alias(out))
    # T
    for out, src in metrics_T.items():
        if src in df.columns:
            agg_exprs.append(_sum_period(src, "T").alias(out))
    # T-1
    for out, src in metrics_T1.items():
        if src in df.columns:
            agg_exprs.append(_sum_period(src, "T-1").alias(out))

    # Effective days per NSC_CODE
    eff_cols = []
    if "T月有效天数" in df.columns:
        eff_cols.append(pl.col("T月有效天数").max().alias("T月有效天数"))
    if "T-1月有效天数" in df.columns:
        eff_cols.append(pl.col("T-1月有效天数").max().alias("T-1月有效天数"))

    extra_dims: list[pl.Expr] = []
    if group_mode == "id":
        # 补充门店名、层级
        if "门店名" in df.columns:
            extra_dims.append(pl.col("门店名").first().alias("门店名"))
        if "层级" in df.columns:
            extra_dims.append(pl.col("层级").first().alias("层级"))

    grouped = df.group_by(id_col).agg(extra_dims + agg_exprs + eff_cols)

    # Derived ratios and daily averages
    def col(name: str) -> pl.Expr:
        return pl.col(name) if name in grouped.columns else pl.lit(0.0)

    # col_any removed by design — enforce canonical column names upstream

    total_eff_days = col("T月有效天数") + col("T-1月有效天数")

    result = grouped.with_columns(
        # CPLs
        _safe_div(col("车云店+区域投放总金额"), col("自然线索量") + col("付费线索量")).alias("车云店+区域综合CPL"),
        _safe_div(
            col("车云店+区域投放总金额"), col("车云店付费线索(总)") + col("区域加码付费线索(总)")
        ).alias("付费CPL（车云店+区域）"),
        _safe_div(col("车云店+区域投放总金额"), col("车云店付费线索(总)") + col("区域加码付费线索(总)")).alias("直播付费CPL"),
        _safe_div(
            col("T月 车云店+区域投放总金额"), col("T月 车云店付费线索") + col("T月 区域加码付费线索")
        ).alias("T月直播付费CPL"),
        _safe_div(
            col("T-1月 车云店+区域投放总金额"), col("T-1月 车云店付费线索") + col("T-1月 区域加码付费线索")
        ).alias("T-1月直播付费CPL"),

        # 本地线索占比
        _safe_div(col("本地线索量(总)"), col("自然线索量") + col("付费线索量")).alias("本地线索占比"),

        # 日均消耗（车云店+区域）
        _safe_div(col("车云店+区域投放总金额"), total_eff_days).alias("直播车云店+区域日均消耗"),
        _safe_div(col("T月 车云店+区域投放总金额"), col("T月有效天数")).alias("T月直播车云店+区域日均消耗"),
        _safe_div(col("T-1月 车云店+区域投放总金额"), col("T-1月有效天数")).alias("T-1月直播车云店+区域日均消耗"),

        # 付费线索量日均（车云店+区域）
        _safe_div(col("车云店付费线索(总)") + col("区域加码付费线索(总)"), total_eff_days).alias("直播车云店+区域付费线索量日均"),
        _safe_div(col("T月 车云店付费线索") + col("T月 区域加码付费线索"), col("T月有效天数")).alias("T月直播车云店+区域付费线索量日均"),
        _safe_div(col("T-1月 车云店付费线索") + col("T-1月 区域加码付费线索"), col("T-1月有效天数")).alias("T-1月直播车云店+区域付费线索量日均"),

        # 日均有效（25min以上）时长（h）
        _safe_div((col("超25分钟直播时长(分)(总)") / 60.0), total_eff_days).alias("日均有效（25min以上）时长（h）"),
        _safe_div((col("T月超25分钟直播时长(分)(总)") / 60.0), col("T月有效天数")).alias("T月日均有效（25min以上）时长（h）"),
        _safe_div((col("T-1月超25分钟直播时长(分)(总)") / 60.0), col("T-1月有效天数")).alias("T-1月日均有效（25min以上）时长（h）"),

        # 场均指标与率（总/T/T-1）
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

        _safe_div(
            col("组件点击次数"),
            col("锚点曝光量"),
        ).alias("组件点击率"),
        _safe_div(
            col("T月组件点击次数"),
            col("T月锚点曝光量"),
        ).alias("T月组件点击率"),
        _safe_div(
            col("T-1月组件点击次数"),
            col("T-1月锚点曝光量"),
        ).alias("T-1月组件点击率"),

        _safe_div(
            col("组件留资人数（获取线索量）"),
            col("锚点曝光量"),
        ).alias("组件留资率"),
        _safe_div(
            col("T月组件留资人数（获取线索量）"),
            col("T月锚点曝光量"),
        ).alias("T月组件留资率"),
        _safe_div(
            col("T-1月组件留资人数（获取线索量）"),
            col("T-1月锚点曝光量"),
        ).alias("T-1月组件留资率"),

        # Message daily averages
        _safe_div(col("进私人数(总)"), total_eff_days).alias("日均进私人数"),
        _safe_div(col("T月进私人数(总)"), col("T月有效天数")).alias("T月日均进私人数"),
        _safe_div(col("T-1月进私人数(总)"), col("T-1月有效天数")).alias("T-1月日均进私人数"),

        _safe_div(col("私信开口人数(总)"), total_eff_days).alias("日均私信开口人数"),
        _safe_div(col("T月私信开口人数(总)"), col("T月有效天数")).alias("T月日均私信开口人数"),
        _safe_div(col("T-1月私信开口人数(总)"), col("T-1月有效天数")).alias("T-1月日均私信开口人数"),

        _safe_div(col("咨询留资人数(总)"), total_eff_days).alias("日均咨询留资人数"),
        _safe_div(col("T月咨询留资人数(总)"), col("T月有效天数")).alias("T月日均咨询留资人数"),
        _safe_div(col("T-1月咨询留资人数(总)"), col("T-1月有效天数")).alias("T-1月日均咨询留资人数"),

        # Message rates
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

    # 合成“直播车云店+区域付费线索量”及分月版
    result = result.with_columns(
        (col("车云店付费线索(总)") + col("区域加码付费线索(总)")).alias("直播车云店+区域付费线索量"),
        (col("T月 车云店付费线索") + col("T月 区域加码付费线索")).alias("T月直播车云店+区域付费线索量"),
        (col("T-1月 车云店付费线索") + col("T-1月 区域加码付费线索")).alias("T-1月直播车云店+区域付费线索量"),
    )

    # Ensure missing metrics exist as zeros to match strict header
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

    # 层级排序：降序，且“未知”置底
    if group_mode == "level" and "层级" in result.columns:
        result = (
            result
            .with_columns(
                pl.when(pl.col("层级") == "未知").then(1).otherwise(0).alias("_unk_sort")
            )
            .sort(by=["_unk_sort", "层级"], descending=[False, True])
            .drop(["_unk_sort"])
        )

    # Final selection: strict header as requested
    key_cols = [id_col]
    if group_mode == "id":
        if "门店名" in result.columns:
            key_cols.append("门店名")
        if "层级" in result.columns:
            key_cols.append("层级")
    else:
        # ensure 层级 is present for grouping result
        if id_col == "层级" and "层级" not in key_cols:
            key_cols = ["层级"]

    ordered_metrics = [
        # 线索与投放
        "自然线索量", "付费线索量", "车云店+区域投放总金额",
        # 直播
        "直播时长", "T月直播时长", "T-1月直播时长",
        "直播线索量", "T月直播线索量", "T-1月直播线索量",
        # 短视频
        "锚点曝光量", "T月锚点曝光量", "T-1月锚点曝光量",
        "组件点击次数", "T月组件点击次数", "T-1月组件点击次数",
        "组件留资人数（获取线索量）", "T月组件留资人数（获取线索量）", "T-1月组件留资人数（获取线索量）",
        "短视频条数", "T月短视频条数", "T-1月短视频条数",
        "短视频播放量", "T月短视频播放量", "T-1月短视频播放量",
        # CPL/比率相关
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
        # 私信日均与比率
        "日均进私人数", "T月日均进私人数", "T-1月日均进私人数",
        "日均私信开口人数", "T月日均私信开口人数", "T-1月日均私信开口人数",
        "日均咨询留资人数", "T月日均咨询留资人数", "T-1月日均咨询留资人数",
        "私信咨询率", "T月私信咨询率", "T-1月私信咨询率",
        "咨询留资率", "T月咨询留资率", "T-1月咨询留资率",
        "私信转化率", "T月私信转化率", "T-1月私信转化率",
    ]

    # Keep only available ordered metrics (some may be absent if sources missing)
    final_cols = key_cols + [c for c in ordered_metrics if c in result.columns]
    final_df = result.select(final_cols)

    # 最终选择后再次按层级排序（若分组为层级），确保输出稳定：降序，未知置底
    if group_mode == "level" and "层级" in final_df.columns:
        final_df = (
            final_df
            .with_columns(
                pl.when(pl.col("层级") == "未知").then(1).otherwise(0).alias("_unk_sort")
            )
            .sort(by=["_unk_sort", "层级"], descending=[False, True])
            .drop(["_unk_sort"])
        )
    return final_df
