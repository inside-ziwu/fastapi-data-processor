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


def rename_for_output(df: pl.DataFrame) -> pl.DataFrame:
    """Rename columns to Chinese output names and drop duplicates after renaming.

    - Applies OUTPUT_NAME_MAP to current columns.
    - If multiple input columns map to the same output name, keeps the first occurrence
      in the existing column order and drops the rest to avoid duplicates.
    """
    if df.is_empty():
        return df

    # Build rename map for present columns only
    rename_map = {c: OUTPUT_NAME_MAP[c] for c in df.columns if c in OUTPUT_NAME_MAP}
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
