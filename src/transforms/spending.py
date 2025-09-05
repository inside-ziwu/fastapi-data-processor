"""Spending data transformation."""

import polars as pl
from typing import Any, Dict, List, Optional
from .base import BaseTransform
from ..config import SPENDING_MAP


class SpendingTransform(BaseTransform):
    """Transform ad spending data to standardized format."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.mapping = SPENDING_MAP
        self.sum_columns: List[str] = ["spending_net"]

    def get_required_columns(self) -> List[str]:
        return list(self.mapping.keys())

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        df = self._rename_columns(df, self.mapping)
        df = self._normalize_nsc_code(df)
        # Spending date candidates (EN/CH)
        df = self._ensure_date_column(df, ["date", "Date", "日期"]) 

        # numeric cleanup for spending
        df = self._cast_numeric_columns(df, self.sum_columns)
        df = self._aggregate_data(df, ["NSC_CODE", "date"], self.sum_columns)
        return df

