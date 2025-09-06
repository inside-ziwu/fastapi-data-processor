"""Final output column naming (Chinese standardization)."""

from __future__ import annotations

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
        if "_" not in c:
            continue
        base, suf = c.rsplit("_", 1)
        if suf.strip().lower() not in suffixes:
            continue
        if base not in OUTPUT_NAME_MAP:
            continue
        base_to_cols.setdefault(base, []).append(c)

    if not base_to_cols:
        return df

    # For each base: if base already exists, skip. If one candidate -> rename. If multiple -> sum horizontally.
    for base, cols in base_to_cols.items():
        if base in existing:
            continue
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
                # Prefer numeric sum when any numeric present, else coalesce first non-null as string
                if df[f"__{base}__sum"].null_count() < df.height:
                    df = df.rename({f"__{base}__sum": base})
                else:
                    # Fallback: coalesce as string
                    co = pl.coalesce([pl.col(c).cast(pl.Utf8) for c in cols]).alias(base)
                    df = df.drop(f"__{base}__sum").with_columns(co)
                df = df.drop(cols)
                existing.add(base)
                for c in cols:
                    existing.discard(c)
            except Exception:
                # Best effort: if merge fails, keep original columns
                try:
                    df = df.drop(f"__{base}__sum")
                except Exception:
                    pass
                continue
    return df


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
    }

    new_cols = []
    for src, tgt in alias_pairs.items():
        if (src in df.columns) and (tgt not in df.columns):
            new_cols.append(pl.col(src).alias(tgt))

    if new_cols:
        df = df.with_columns(new_cols)
    return df
