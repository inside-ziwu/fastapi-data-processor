"""Leads data transformation."""

import polars as pl
from typing import Any, Dict, List, Optional
from .base import BaseTransform
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
        # Direct, explicit renames for critical keys to avoid fuzzy-matching surprises
        direct_keys = {"主机厂经销商id列表": "NSC_CODE", "留资日期": "date"}
        present_direct = {k: v for k, v in direct_keys.items() if k in df.columns}
        if present_direct:
            df = df.rename(present_direct)

        df = self._rename_columns(df, self.mapping)
        df = self._normalize_nsc_code(df)
        df = self._ensure_date_column(df, ["date", "留资日期", "日期"]) 

        df = self._cast_numeric_columns(df, self.sum_columns)
        df = self._aggregate_data(df, ["NSC_CODE", "date"], self.sum_columns)
        return df
