"""Live streaming data transformation."""

import polars as pl
from typing import Any, Dict, List, Optional
from .base import BaseTransform
from ..config import LIVE_MAP


class LiveTransform(BaseTransform):
    """Transform live streaming data to standardized format."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.mapping = LIVE_MAP
        self.sum_columns = [
            "over25_min_live_mins",
            "live_effective_hours",
            "effective_live_sessions",
            "exposures",
            "viewers",
            "small_wheel_clicks",
        ]

    def get_required_columns(self) -> List[str]:
        """Required columns for live transformation."""
        return list(self.mapping.keys())

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform live data to standardized format."""
        # Step 1: Rename columns using mapping
        df = self._rename_columns(df, self.mapping)

        # Step 2: Normalize NSC_CODE column
        df = self._normalize_nsc_code(df)

        # Step 3: Ensure date column exists
        df = self._ensure_date_column(df, ["日期", "date", "time", "开播日期", "直播日期"])

        # Step 4: Cast numeric columns
        df = self._cast_numeric_columns(df, self.sum_columns)

        # Step 5: Extraction-only — no aggregation at this stage
        wanted = ["NSC_CODE", "date"] + self.sum_columns
        present = [c for c in wanted if c in df.columns]
        df = df.select(present)

        return df
        
