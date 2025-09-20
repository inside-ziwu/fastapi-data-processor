"""Final output column naming (Chinese standardization)."""

from __future__ import annotations

import os

import polars as pl

# Map internal standardized names -> final Chinese output names
OUTPUT_NAME_MAP: dict[str, str] = {
    # Keys
    "NSC_CODE": "经销商ID",
    "NSC_Code": "经销商ID",  # unify alias to the same output name
    "date": "日期",
    # Period helpers
    "month": "month",
    "day": "day",
    "period": "period",
    "T_effective_days": "T月有效天数",
    "T_minus_1_effective_days": "T-1月有效天数",

    # Video
    "anchor_exposure": "锚点曝光量",
    "component_clicks": "组件点击次数",
    "short_video_count": "短视频条数",
    "short_video_leads": "组件留资人数（获取线索量）",

    # Live
    "over25_min_live_mins": "超25分钟直播时长(分)",
    "live_effective_hours": "直播有效时长(小时)",
    "effective_live_sessions": "有效直播场次",
    "exposures": "曝光人数",
    "viewers": "场观",
    "small_wheel_clicks": "小风车点击次数",

    # Message
    "enter_private_count": "进私人数",
    "private_open_count": "私信开口人数",
    "private_leads_count": "咨询留资人数",

    # Leads
    "small_wheel_leads": "小风车留资量",

    # DR (keep english metric names that are domain-specific)
    "leads_type": "leads_type",
    "mkt_second_channel_name": "mkt_second_channel_name",
    "send2dealer_id": "send2dealer_id",
    # DR daily counts
    "natural_leads": "自然线索",
    "paid_leads": "付费线索",
    "store_paid_leads": "车云店付费线索",
    "area_paid_leads": "区域加码付费线索",
    "local_leads": "本地线索量",

    # Spending
    "spending_net": "Spending(Net)",

    # Account BI
    "live_leads": "直播线索量",
    "short_video_plays": "短视频播放量",

    # Account base
    "level": "层级",
    "store_name": "门店名",
}

# Primary key columns that must survive join-suffix normalization intact
KEY_BASES: set[str] = {"NSC_CODE", "date", "month", "day"}
PERIOD_KEY = "period"


def normalize_join_suffixes(df: pl.DataFrame, valid_suffixes: set[str] | None = None) -> pl.DataFrame:
    """Normalize suffixed columns produced by outer joins back to their base names.

    Responsibility: ONLY handle join suffixes like `<base>_<source_key>` where:
    - `<base>` is a known internal name present in OUTPUT_NAME_MAP.
    - `<source_key>` is explicitly provided via valid_suffixes (e.g., input file keys).

    This keeps rename_for_output dumb and precise.
    """
    if df.is_empty():
        return df

    if not valid_suffixes:
        return df

    suffixes = {s.strip().lower() for s in valid_suffixes if s and isinstance(s, str)}
    # Collect candidate columns per base
    existing = set(df.columns)
    base_to_cols: dict[str, list[str]] = {}
    for c in df.columns:
        lower = c.lower()
        matched = False
        for suf in suffixes:
            token = f"_{suf}"
            if lower.endswith(token):
                base = c[: -len(token)]
                if base and base in OUTPUT_NAME_MAP:
                    base_to_cols.setdefault(base, []).append(c)
                matched = True
                break
        if matched:
            continue

    if not base_to_cols:
        return df

    # For each base: if base already exists and is textual (store_name/level), coalesce; else merge/rename.
    TEXT_BASES = {"store_name", "level"}
    def _clean_text_expr(colname: str) -> pl.Expr:
        return (
            pl.when(pl.col(colname).is_null() | (pl.col(colname).cast(pl.Utf8).str.strip_chars() == ""))
            .then(None)
            .otherwise(pl.col(colname).cast(pl.Utf8))
        )
    # First, normalize mandatory key columns to guarantee clean join keys downstream
    def _expr_for(colname: str, target: str) -> pl.Expr:
        expr = pl.col(colname)
        if target == "NSC_CODE":
            cleaned = expr.cast(pl.Utf8, strict=False).str.strip_chars()
            return pl.when(cleaned == "").then(None).otherwise(cleaned)
        if target in TEXT_BASES:
            cleaned = expr.cast(pl.Utf8, strict=False).str.strip_chars()
            return pl.when(cleaned == "").then(None).otherwise(cleaned)
        if target == "date":
            return expr.cast(pl.Date, strict=False)
        if target in {"month", "day"}:
            return expr.cast(pl.Int64, strict=False)
        return expr

    def _coalesce_columns(target: str, cols: list[str]) -> None:
        nonlocal df
        if not cols and target not in df.columns:
            return

        expressions: list[pl.Expr] = []
        if target in df.columns:
            expressions.append(_expr_for(target, target))
        expressions.extend(_expr_for(c, target) for c in cols)
        if not expressions:
            return

        df = df.with_columns(pl.coalesce(expressions).alias(target))
        existing.add(target)
        if target == "NSC_CODE":
            df = df.with_columns(
                pl.when(
                    pl.col(target).cast(pl.Utf8, strict=False).str.strip_chars() == ""
                )
                .then(None)
                .otherwise(pl.col(target).cast(pl.Utf8, strict=False).str.strip_chars())
                .alias(target)
            )
        elif target == "date":
            df = df.with_columns(pl.col(target).cast(pl.Date, strict=False).alias(target))
        elif target in {"month", "day"}:
            df = df.with_columns(pl.col(target).cast(pl.Int64, strict=False).alias(target))
        elif target in TEXT_BASES:
            df = df.with_columns(
                pl.when(
                    pl.col(target).cast(pl.Utf8, strict=False).str.strip_chars() == ""
                )
                .then(None)
                .otherwise(pl.col(target).cast(pl.Utf8, strict=False).str.strip_chars())
                .alias(target)
            )

        for c in cols:
            if c in df.columns:
                df = df.drop(c)
            existing.discard(c)

    for key in KEY_BASES:
        suffixed = base_to_cols.pop(key, [])
        _coalesce_columns(key, suffixed)

    # period 允许缺失：仅做合并与类型收敛
    period_cols = base_to_cols.pop(PERIOD_KEY, [])
    if period_cols or PERIOD_KEY in df.columns:
        _coalesce_columns(PERIOD_KEY, period_cols)
        if PERIOD_KEY in df.columns:
            df = df.with_columns(pl.col(PERIOD_KEY).cast(pl.Utf8, strict=False).alias(PERIOD_KEY))

    for base, cols in base_to_cols.items():
        if base in existing:
            if base in TEXT_BASES:
                try:
                    # Coalesce base with suffixed textual columns; treat empty string as null
                    co = pl.coalesce([_clean_text_expr(base)] + [_clean_text_expr(c) for c in cols]).alias(base)
                    df = df.with_columns(co).drop(cols)
                    for c in cols:
                        existing.discard(c)
                except Exception:
                    pass
            else:
                # Even when base exists, ensure suffix data back-fills null slots
                try:
                    co = pl.coalesce([pl.col(base)] + [pl.col(c) for c in cols]).alias(base)
                    df = df.with_columns(co).drop(cols)
                except Exception:
                    pass
            continue
        # base not in existing yet
        if base in TEXT_BASES:
            try:
                co = pl.coalesce([_clean_text_expr(c) for c in cols]).alias(base)
                df = df.with_columns(co).drop(cols)
                existing.add(base)
                for c in cols:
                    existing.discard(c)
            except Exception:
                continue
        else:
            if len(cols) == 1:
                try:
                    df = df.rename({cols[0]: base})
                    existing.add(base)
                    existing.discard(cols[0])
                except Exception:
                    continue
            else:
                # Merge multiple suffixed columns into one base: sum numerics, coalesce non-numerics (first non-null as string)
                try:
                    num_sum = pl.sum_horizontal([
                        pl.col(c).cast(pl.Float64, strict=False) for c in cols
                    ]).alias(f"__{base}__sum")
                    df = df.with_columns(num_sum)
                    # Prefer numeric sum when any numeric present, else coalesce as string
                    if df[f"__{base}__sum"].null_count() < df.height:
                        df = df.rename({f"__{base}__sum": base})
                    else:
                        # Fallback: coalesce as string (treat empty as null)
                        co = pl.coalesce([_clean_text_expr(c) for c in cols]).alias(base)
                        df = df.drop(f"__{base}__sum").with_columns(co)
                    df = df.drop(cols)
                    existing.add(base)
                    for c in cols:
                        existing.discard(c)
                except Exception:
                    try:
                        df = df.drop(f"__{base}__sum")
                    except Exception:
                        pass
                    continue
    _assert_key_integrity(df)
    return df


def _should_assert_keys() -> bool:
    val = os.getenv("PROCESSOR_ASSERT_KEYS", "0").strip().lower()
    return val in {"1", "true", "yes", "on", "debug"}


def _assert_key_integrity(df: pl.DataFrame) -> None:
    if not _should_assert_keys():
        return
    for key in KEY_BASES:
        if key not in df.columns:
            continue
        nulls = int(df.select(pl.col(key).is_null().sum()).to_series(0)[0])
        assert nulls == 0, f"Key column '{key}' contains nulls after normalization!"
        if key == "NSC_CODE":
            dtype = df.schema[key]
            assert dtype in {pl.Utf8, pl.Categorical}, (
                f"Column '{key}' has unexpected dtype: {dtype}"
            )
    if PERIOD_KEY in df.columns:
        # period 可空，但 dtype 必须是字符串可比
        dtype = df.schema[PERIOD_KEY]
        assert dtype in {
            pl.Utf8,
            pl.Categorical,
            pl.Null,
        }, f"Column '{PERIOD_KEY}' has unexpected dtype: {dtype}"


def rename_for_output(df: pl.DataFrame) -> pl.DataFrame:
    """Rename columns to Chinese output names and drop duplicates after renaming.

    - Applies OUTPUT_NAME_MAP to current columns.
    - If multiple input columns map to the same output name, keeps the first occurrence
      in the existing column order and drops the rest to avoid duplicates.
    """
    if df.is_empty():
        return df

    # Build rename map for present columns only (exact match ONLY)
    # Avoid creating duplicates: if target name already exists (either currently in df or planned), skip.
    rename_map: dict[str, str] = {}
    planned_targets: set[str] = set()
    existing: set[str] = set(df.columns)
    for c in df.columns:
        if c in OUTPUT_NAME_MAP:
            target = OUTPUT_NAME_MAP[c]
            if (target in existing) or (target in planned_targets):
                # Skip to prevent duplicate target columns during rename
                continue
            rename_map[c] = target
            planned_targets.add(target)
    if not rename_map:
        return df

    renamed = df.rename(rename_map)

    # Drop duplicate columns by output name, keeping the first occurrence
    seen: set[str] = set()
    final_cols: list[str] = []
    for c in renamed.columns:
        if c in seen:
            continue
        seen.add(c)
        final_cols.append(c)

    if len(final_cols) != len(renamed.columns):
        renamed = renamed.select(final_cols)

    return renamed


def add_feishu_rate_aliases(df: pl.DataFrame) -> pl.DataFrame:
    """Add alias columns for Feishu fields that use explanatory suffixes.

    This does not change existing columns; it only creates additional columns with
    names like '私信咨询率=开口|进私' so Feishu schema can match them exactly.
    """
    if df.is_empty():
        return df

    alias_pairs = {
        # Private message rates
        "私信咨询率": "私信咨询率=开口|进私",
        "T月私信咨询率": "T月私信咨询率=开口|进私",
        "T-1月私信咨询率": "T-1月私信咨询率=开口|进私",
        "咨询留资率": "咨询留资率=留资|咨询",
        "T月咨询留资率": "T月咨询留资率=留资|咨询",
        "T-1月咨询留资率": "T-1月咨询留资率=留资|咨询",
        "私信转化率": "私信转化率=留资|进私",
        "T月私信转化率": "T月私信转化率=留资|进私",
        "T-1月私信转化率": "T-1月私信转化率=留资|进私",
        # Component rates (common Feishu naming variants)
        "组件点击率": "组件点击率=点击|曝光",
        "T月组件点击率": "T月组件点击率=点击|曝光",
        "T-1月组件点击率": "T-1月组件点击率=点击|曝光",
        "组件留资率": "组件留资率=留资|曝光",
        "T月组件留资率": "T月组件留资率=留资|曝光",
        "T-1月组件留资率": "T-1月组件留资率=留资|曝光",
    }

    new_cols = []
    for src, tgt in alias_pairs.items():
        if (src in df.columns) and (tgt not in df.columns):
            new_cols.append(pl.col(src).alias(tgt))

    if new_cols:
        df = df.with_columns(new_cols)
    return df
