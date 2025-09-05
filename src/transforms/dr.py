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
        # Typically DR is event-level; we don't aggregate many numeric fields here by default
        self.sum_columns: List[str] = []

    def get_required_columns(self) -> List[str]:
        return list(self.mapping.keys())

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        df = self._rename_columns(df, self.mapping)
        df = self._normalize_nsc_code(df)
        # DR date column candidates
        df = self._ensure_date_column(df, ["date", "register_time", "register date", "日期"])

        if self.sum_columns:
            df = self._cast_numeric_columns(df, self.sum_columns)
            df = self._aggregate_data(df, ["NSC_CODE", "date"], self.sum_columns)
        else:
            # keep unique rows for keys to avoid duplication
            df = df.unique(subset=[c for c in ["NSC_CODE", "date"] if c in df.columns])

        return df

