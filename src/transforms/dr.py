"""DR data transformation (register/leads records)."""

import polars as pl
import os
import logging
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

        # 规范化 leads_type（NFKC、去空白、统一大小写）；未知值一次性告警（由环境变量控制）
        def _norm_py(v: Any) -> str:
            try:
                import unicodedata, re
                s = "" if v is None else str(v)
                s = unicodedata.normalize("NFKC", s)
                s = s.strip().lower()
                s = re.sub(r"[\s\u200b\u200c\u200d\ufeff\u00a0]+", "", s)
                return s
            except Exception:
                return "" if v is None else str(v).strip().lower()

        df = df.with_columns(
            pl.col("leads_type").map_elements(_norm_py, return_dtype=pl.Utf8).alias("_leads_type_norm")
        )

        # 允许的规范化取值："自然"、"付费"（规范化后仍是中文，不做同义词）
        # 将其它值视为未知
        df = df.with_columns(
            pl.when(pl.col("_leads_type_norm") == "自然")
            .then(pl.lit("自然"))
            .when(pl.col("_leads_type_norm").is_in(["付费", "广告"]))
            .then(pl.lit("付费"))
            .otherwise(pl.lit("未知")).alias("leads_type_norm")
        )

        # 一次性告警：存在未知取值
        try:
            warn_env = os.getenv("PROCESSOR_WARN_OPTIONAL_FIELDS", "1").strip().lower() in {"1", "true", "yes", "on"}
            if warn_env:
                unknown_cnt = df.select((pl.col("leads_type_norm") == "未知").sum().alias("cnt"))[0, "cnt"]
                if unknown_cnt and int(unknown_cnt) > 0:
                    logging.getLogger(__name__).warning(
                        f"DR提示：存在未识别的leads_type取值，共 {int(unknown_cnt)} 条；将按‘未知’处理，不计入‘自然/付费’分类。"
                    )
        except Exception:
            pass

        # 构造 0/1 指示列，聚合阶段对其按 NSC_CODE+date 求和
        leads_type = pl.col("leads_type_norm")
        chan = pl.col("mkt_second_channel_name").cast(pl.Utf8)
        nsc = pl.col("NSC_CODE").cast(pl.Utf8)
        send2 = pl.col("send2dealer_id").cast(pl.Utf8)

        df = df.with_columns(
            (leads_type == "自然").cast(pl.Int64).alias("natural_leads"),
            (leads_type == "付费").cast(pl.Int64).alias("paid_leads"),
            (
                (leads_type == "付费")
                & chan.is_in(["抖音车云店_BMW_本市_LS直发", "抖音车云店_LS直发"])
            )
            .cast(pl.Int64)
            .alias("store_paid_leads"),
            (
                (leads_type == "付费") & (chan == "抖音车云店_BMW_总部BDT_LS直发")
            )
            .cast(pl.Int64)
            .alias("area_paid_leads"),
            (send2 == nsc).cast(pl.Int64).alias("local_leads"),
        )

        # 仅保留聚合所需列
        keep_cols = ["NSC_CODE", "date"] + self.sum_columns
        present = [c for c in keep_cols if c in df.columns]
        df = df.select(present)

        return df
