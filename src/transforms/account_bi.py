"""Account BI data transformation."""

import polars as pl
from typing import Any, Dict, List, Optional
from .base import BaseTransform
from ..config import ACCOUNT_BI_MAP


class AccountBITransform(BaseTransform):
    """Transform account-level BI metrics to standardized format."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.mapping = ACCOUNT_BI_MAP
        # Keep a couple of known numeric fields if present
        self.sum_columns: List[str] = [
            col
            for col in ["live_leads", "short_video_plays"]
            if col in set(self.mapping.values())
        ]

    def get_required_columns(self) -> List[str]:
        return list(self.mapping.keys())

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        df = self._rename_columns(df, self.mapping)
        df = self._normalize_nsc_code(df)
        df = self._ensure_date_column(df, ["date", "日期"]) 

        if self.sum_columns:
            df = self._cast_numeric_columns(df, self.sum_columns)
            df = self._aggregate_data(df, ["NSC_CODE", "date"], self.sum_columns)
        else:
            df = df.unique(subset=[c for c in ["NSC_CODE", "date"] if c in df.columns])

        return df

