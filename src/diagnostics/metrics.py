"""Diagnostics helpers (opt-in via PROCESSOR_DIAG).

Keep diagnostics separate from core logic. Never mutate inputs.
Failures are logged as errors and do not raise.
"""

from __future__ import annotations

import logging
import os
from typing import Iterable

import polars as pl


def _diag_enabled() -> bool:
    val = os.getenv("PROCESSOR_DIAG", "1").strip().lower()
    return val in {"1", "true", "yes", "on"}


def log_settlement_inputs(df: pl.DataFrame) -> None:
    """Log period distribution and key metric sums on daily wide table (Chinese columns).

    Expects columns like: period, 自然线索, 付费线索, 车云店付费线索, 区域加码付费线索,
    本地线索量, 组件点击次数, 锚点曝光量, 组件留资人数（获取线索量）, 进私人数, 私信开口人数, 咨询留资人数, Spending(Net).
    """
    if not _diag_enabled():
        return
    logger = logging.getLogger(__name__)
    try:
        total_rows = int(df.height)
        t_rows = int(df.filter(pl.col("period") == "T").height) if "period" in df.columns else 0
        t1_rows = int(df.filter(pl.col("period") == "T-1").height) if "period" in df.columns else 0

        # candidate sources by Chinese header after rename_for_output
        sources = {
            "自然线索量": _pick(df, ["自然线索", "自然线索量"]),
            "付费线索量": _pick(df, ["付费线索", "付费线索量"]),
            "车云店付费线索": _pick(df, ["车云店付费线索"]),
            "区域加码付费线索": _pick(df, ["区域加码付费线索"]),
            "本地线索量": _pick(df, ["本地线索量"]),
            "组件点击次数": _pick(df, ["组件点击次数"]),
            "锚点曝光量": _pick(df, ["锚点曝光量"]),
            "组件留资人数（获取线索量）": _pick(df, ["组件留资人数（获取线索量）"]),
            "进私人数": _pick(df, ["进私人数"]),
            "私信开口人数": _pick(df, ["私信开口人数"]),
            "咨询留资人数": _pick(df, ["咨询留资人数"]),
            "投放金额": _pick(df, ["Spending(Net)", "车云店+区域投放总金额"]),
        }

        def sum_for(tag: str, col: str | None) -> float:
            if not col or col not in df.columns:
                return 0.0
            if "period" not in df.columns:
                return float(df.select(pl.col(col).sum()).to_series(0)[0] or 0.0)
            if tag == "both":
                expr = (
                    pl.when(pl.col("period").is_in(["T", "T-1"]))
                    .then(pl.col(col))
                    .otherwise(0)
                    .sum()
                )
            elif tag == "T":
                expr = pl.when(pl.col("period") == "T").then(pl.col(col)).otherwise(0).sum()
            else:
                expr = pl.when(pl.col("period") == "T-1").then(pl.col(col)).otherwise(0).sum()
            return float(df.select(expr).to_series(0)[0] or 0.0)

        sums: dict[str, dict[str, float]] = {}
        for name, col in sources.items():
            sums[name] = {
                "both": sum_for("both", col),
                "T": sum_for("T", col),
                "T-1": sum_for("T-1", col),
            }

        logger.info(
            f"Settlement diag: period rows total={total_rows}, T={t_rows}, T-1={t1_rows}; metric sums={sums}"
        )
    except Exception:
        logger.error("Settlement diagnostics failed", exc_info=True)


def log_level_distribution(df: pl.DataFrame) -> None:
    """Log level distribution by unique NSC_CODE (accepts 'level' or '层级')."""
    if not _diag_enabled():
        return
    logger = logging.getLogger(__name__)
    try:
        lvl = "level" if "level" in df.columns else ("层级" if "层级" in df.columns else None)
        if lvl is None or "NSC_CODE" not in df.columns:
            return
        pairs = df.select(["NSC_CODE", lvl]).drop_nulls("NSC_CODE").unique()
        if pairs.is_empty():
            return
        dist = pairs.group_by(lvl).count().sort(lvl)
        payload = {str(lv): int(ct) for lv, ct in zip(dist[lvl].to_list(), dist["count"].to_list())}
        logger.info(f"Level distribution (by NSC): {payload}")
    except Exception:
        logger.error("Level distribution diagnostics failed", exc_info=True)


def log_suffix_masking(df: pl.DataFrame, source_keys: Iterable[str]) -> None:
    """Log suffixed overlapping columns detection based on source keys (best-effort)."""
    if not _diag_enabled():
        return
    logger = logging.getLogger(__name__)
    try:
        cols = df.columns
        hits: dict[str, int] = {}
        for key in source_keys:
            suffix = f"_{key}"
            cnt = sum(1 for c in cols if c.endswith(suffix))
            if cnt:
                hits[key] = cnt
        if hits:
            logger.info(f"Join suffix masking detected: {hits}")
    except Exception:
        logger.error("Suffix masking diagnostics failed", exc_info=True)


def _pick(df: pl.DataFrame, cands: list[str]) -> str | None:
    for c in cands:
        if c in df.columns:
            return c
    return None


def log_account_base_conflicts(df: pl.DataFrame) -> None:
    """Detect multiple values per NSC_CODE for level/store_name and warn once.

    Accepts either English or Chinese column names after output renaming.
    """
    if not _diag_enabled():
        return
    logger = logging.getLogger(__name__)
    try:
        if "NSC_CODE" not in df.columns:
            return

        lvl_col = _pick(df, ["level", "层级"])
        name_col = _pick(df, ["store_name", "门店名"]) 

        def _conflicts(col: str) -> tuple[int, list[str]]:
            # Normalize: cast to string; empty -> null
            sub = df.select(["NSC_CODE", col]).with_columns(
                pl.when(
                    pl.col(col).is_null() | (pl.col(col).cast(pl.Utf8).str.strip_chars() == "")
                )
                .then(None)
                .otherwise(pl.col(col).cast(pl.Utf8))
                .alias(col)
            )
            if sub.is_empty():
                return (0, [])
            g = (
                sub.group_by("NSC_CODE")
                .agg(pl.col(col).drop_nulls().n_unique().alias("uniq"))
                .filter(pl.col("uniq") > 1)
                .select("NSC_CODE")
            )
            if g.is_empty():
                return (0, [])
            codes = g["NSC_CODE"].cast(pl.Utf8).to_list()
            return (len(codes), codes[:10])

        msgs: list[str] = []
        if lvl_col:
            cnt, sample = _conflicts(lvl_col)
            if cnt:
                msgs.append(f"level 冲突 NSC 数量={cnt}, 示例={sample}")
        if name_col:
            cnt, sample = _conflicts(name_col)
            if cnt:
                msgs.append(f"store_name 冲突 NSC 数量={cnt}, 示例={sample}")

        if msgs:
            logger.warning("Account base conflicts detected: " + "; ".join(msgs))
    except Exception:
        logger.error("Account base conflict diagnostics failed", exc_info=True)


def log_message_date_distribution(df: pl.DataFrame) -> None:
    """Log message columns date coverage and distribution.

    Works on the daily wide table after rename_for_output but before settlement.
    """
    if not _diag_enabled():
        return
    logger = logging.getLogger(__name__)
    try:
        if "date" not in df.columns:
            logger.info("Message diag: no date column present in wide table")
            return
        c_enter = _pick(df, ["进私人数"]) 
        c_open = _pick(df, ["私信开口人数"]) 
        c_leads = _pick(df, ["咨询留资人数"]) 
        if not any([c_enter, c_open, c_leads]):
            logger.info("Message diag: message columns not present in wide table")
            return

        # Totals (ignoring period)
        def _sum(col: str | None) -> float:
            if not col or col not in df.columns:
                return 0.0
            return float(df.select(pl.col(col).sum()).to_series(0)[0] or 0.0)

        totals = {
            "进私人数": _sum(c_enter),
            "私信开口人数": _sum(c_open),
            "咨询留资人数": _sum(c_leads),
        }

        # Monthly distribution
        ym = (
            df.select([
                pl.col("date").cast(pl.Date).dt.strftime("%Y-%m").alias("_ym"),
                *(pl.col(c).alias(c) for c in [c_enter, c_open, c_leads] if c),
            ])
            .group_by("_ym")
            .agg([pl.col(c).sum().alias(c) for c in [c_enter, c_open, c_leads] if c])
            .sort("_ym")
        )
        ym_records = [{"_ym": r[0], **{k: r[i+1] for i, k in enumerate([c for c in [c_enter, c_open, c_leads] if c])}} for r in ym.iter_rows()]

        # Date min/max and null ratio
        min_d = df.select(pl.col("date").min()).to_series(0)[0]
        max_d = df.select(pl.col("date").max()).to_series(0)[0]
        nulls = int(df.select(pl.col("date").is_null().sum()).to_series(0)[0])
        ratio = (nulls / df.height) if df.height else 0.0

        logger.info(
            f"Message date diag: totals={totals}, month_bins={ym_records[-6:]}, date_range=({min_d},{max_d}), null_ratio={ratio:.4f}"
        )
    except Exception:
        logger.error("Message date diagnostics failed", exc_info=True)
