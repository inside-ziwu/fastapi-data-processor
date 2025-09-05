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

        # Step 5: Group by key columns and aggregate
        df = self._aggregate_data(df, ["NSC_CODE", "date"], self.sum_columns)

        # Step 6: Add computed columns
        df = self.add_computed_columns(df)

        return df

    def add_computed_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add live-specific computed columns."""
        # Add viewer engagement rate if we have the required columns
        if (
            "viewers" in df.columns
            and "exposures" in df.columns
            and df["exposures"].sum() > 0
        ):

            df = df.with_columns(
                (pl.col("viewers") / pl.col("exposures")).alias(
                    "viewer_engagement_rate"
                )
            )

        # Add average session duration if we have the required columns
        if (
            "live_effective_hours" in df.columns
            and "effective_live_sessions" in df.columns
            and df["effective_live_sessions"].sum() > 0
        ):

            df = df.with_columns(
                (
                    pl.col("live_effective_hours")
                    / pl.col("effective_live_sessions")
                ).alias("avg_session_duration_hours")
            )

        # Add small wheel conversion rate if we have the required columns
        if (
            "small_wheel_clicks" in df.columns
            and "viewers" in df.columns
            and df["viewers"].sum() > 0
        ):

            df = df.with_columns(
                (pl.col("small_wheel_clicks") / pl.col("viewers")).alias(
                    "small_wheel_click_rate"
                )
            )

        return df