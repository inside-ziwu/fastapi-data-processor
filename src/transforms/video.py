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

        # Step 5: Group by key columns and aggregate
        df = self._aggregate_data(df, ["NSC_CODE", "date"], self.sum_columns)

        # Step 6: Add computed columns
        df = self.add_computed_columns(df)

        return df

    def add_computed_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add video-specific computed columns."""
        # Add conversion rate if we have the required columns
        if (
            "component_clicks" in df.columns
            and "anchor_exposure" in df.columns
            and df["anchor_exposure"].sum() > 0
        ):

            df = df.with_columns(
                (pl.col("component_clicks") / pl.col("anchor_exposure")).alias(
                    "click_through_rate"
                )
            )

        # Add leads per video if we have the required columns
        if (
            "short_video_leads" in df.columns
            and "short_video_count" in df.columns
            and df["short_video_count"].sum() > 0
        ):

            df = df.with_columns(
                (
                    pl.col("short_video_leads") / pl.col("short_video_count")
                ).alias("leads_per_video")
            )

        return df