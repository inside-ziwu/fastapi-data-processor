"""DR data transformation (register/leads records)."""

import polars as pl
from typing import Any, Dict, List, Optional
from .base import BaseTransform
from ..config import DR_MAP


class DRTransform(BaseTransform):
    """Transform DR data to standardized format."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.mapping = DR_MAP
        # DR 指标：在提取阶段构造 0/1 指示列，聚合阶段对这些列求和
        self.sum_columns: List[str] = [
            "natural_leads",
            "paid_leads",
            "store_paid_leads",
            "area_paid_leads",
            "local_leads",
        ]

    def get_required_columns(self) -> List[str]:
        return list(self.mapping.keys())

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        df = self._rename_columns(df, self.mapping)
        df = self._normalize_nsc_code(df)
        # DR date column candidates
        df = self._ensure_date_column(df, ["date", "register_time", "register date", "日期"])

        # 构造 0/1 指示列，聚合阶段对其按 NSC_CODE+date 求和
        leads_type = pl.col("leads_type").cast(pl.Utf8)
        chan = pl.col("mkt_second_channel_name").cast(pl.Utf8)
        nsc = pl.col("NSC_CODE").cast(pl.Utf8)
        send2 = pl.col("send2dealer_id").cast(pl.Utf8)

        df = df.with_columns(
            (
                (leads_type == "自然").cast(pl.Int64)
            ).alias("natural_leads"),
            (
                (leads_type == "付费").cast(pl.Int64)
            ).alias("paid_leads"),
            (
                ((leads_type == "付费") & chan.is_in(["抖音车云店_BMW_本市_LS直发", "抖音车云店_LS直发"]))
                .cast(pl.Int64)
            ).alias("store_paid_leads"),
            (
                ((leads_type == "付费") & (chan == "抖音车云店_BMW_总部BDT_LS直发"))
                .cast(pl.Int64)
            ).alias("area_paid_leads"),
            (
                (send2 == nsc).cast(pl.Int64)
            ).alias("local_leads"),
        )

        # 仅保留聚合所需列
        keep_cols = ["NSC_CODE", "date"] + self.sum_columns
        present = [c for c in keep_cols if c in df.columns]
        df = df.select(present)

        return df
