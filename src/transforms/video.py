"""Video data transformation."""

import polars as pl
from typing import Any, Dict, List, Optional
from .base import BaseTransform
from ..config import VIDEO_MAP


class VideoTransform(BaseTransform):
    """Transform video data to standardized format."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.mapping = VIDEO_MAP
        self.sum_columns = [
            "anchor_exposure",
            "component_clicks",
            "short_video_count",
            "short_video_leads",
        ]

    def get_required_columns(self) -> List[str]:
        """Required columns for video transformation."""
        return list(self.mapping.keys())

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform video data to standardized format."""
        # Step 1: Rename columns using mapping
        df = self._rename_columns(df, self.mapping)

        # Step 2: Normalize NSC_CODE column
        df = self._normalize_nsc_code(df)

        # Step 3: Ensure date column exists
        df = self._ensure_date_column(df)

        # Step 4: Cast numeric columns
        df = self._cast_numeric_columns(df, self.sum_columns)

        # Step 5: Extraction-only — no aggregation at this stage
        wanted = ["NSC_CODE", "date"] + self.sum_columns
        present = [c for c in wanted if c in df.columns]
        df = df.select(present)

        return df
        
