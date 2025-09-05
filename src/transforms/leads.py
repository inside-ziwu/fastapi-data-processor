"""Leads data transformation."""

import polars as pl
from typing import Any, Dict, List, Optional
from .base import BaseTransform
from .utils import _field_match
from ..config import LEADS_MAP


class LeadsTransform(BaseTransform):
    """Transform leads data to standardized format."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.mapping = LEADS_MAP
        # Focus on small_wheel_leads as a key metric
        self.sum_columns: List[str] = ["small_wheel_leads"]

    def get_required_columns(self) -> List[str]:
        return list(self.mapping.keys())

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        # 严格映射：仅接受这三列，确保准确性
        required_exact = {
            "主机厂经销商id列表": "NSC_CODE",
            "留资日期": "date",
            "直播间表单提交商机量(去重)": "small_wheel_leads",
        }
        missing = [k for k in required_exact if k not in df.columns]
        if missing:
            raise ValueError(f"leads 缺少必要列: {missing}")

        df = df.rename(required_exact)
        df = self._normalize_nsc_code(df)
        df = self._ensure_date_column(df, ["date", "留资日期", "日期"]) 

        df = self._cast_numeric_columns(df, self.sum_columns)
        # Extraction-only — 仅输出 NSC_CODE, date, 小风车留资量
        df = df.select(["NSC_CODE", "date", "small_wheel_leads"]) 
        return df

    # 严格口径：不做 NSC_CODE 的“智能兜底”
