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
        df = self._rename_columns(df)

        # Step 2: Normalize NSC_CODE column
        df = self._normalize_nsc_code(df)

        # Step 3: Ensure date column exists
        df = self._ensure_date_column(df)

        # Step 4: Cast numeric columns
        df = self._cast_numeric_columns(df)

        # Step 5: Group by key columns and aggregate
        df = self._aggregate_data(df)

        return df

    def _rename_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Rename columns based on mapping."""
        rename_map = {}
        for src_col, expected_col in self.mapping.items():
            # Find matching column in actual data
            for actual_col in df.columns:
                if self._field_match(src_col, actual_col):
                    rename_map[actual_col] = expected_col
                    break

        if rename_map:
            df = df.rename(rename_map)

        # Validate NSC_CODE exists after renaming
        if "NSC_CODE" not in df.columns:
            raise ValueError(
                f"NSC_CODE column not found after renaming. Available: {df.columns}"
            )

        return df

    def _field_match(self, src: str, col: str) -> bool:
        """Check if column matches source field name."""
        # Normalize strings for comparison
        src_norm = src.replace(" ", "").lower()
        col_norm = col.replace(" ", "").lower()

        # Check for exact match or substring match
        return (
            src_norm == col_norm
            or src_norm in col_norm
            or col_norm in src_norm
        )

    def _normalize_nsc_code(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize NSC_CODE column."""
        if "NSC_CODE" not in df.columns:
            return df

        # Cast to string
        df = df.with_columns(pl.col("NSC_CODE").cast(pl.Utf8))

        # Handle multiple NSC codes in single cell (comma-separated)
        df = df.with_columns(
            pl.when(pl.col("NSC_CODE").str.contains(r"[,|]"))
            .then(pl.col("NSC_CODE").str.split(","))
            .otherwise(pl.col("NSC_CODE").str.split(""))
            .alias("_nsc_list")
        )

        # Explode multiple NSC codes into separate rows
        df = df.explode("_nsc_list")

        # Clean up NSC code
        df = df.with_columns(
            pl.col("_nsc_list").str.strip_chars().alias("NSC_CODE")
        ).drop("_nsc_list")

        # Filter out empty NSC codes
        df = df.filter(
            pl.col("NSC_CODE").is_not_null() & (pl.col("NSC_CODE") != "")
        )

        if df.height == 0:
            raise ValueError(
                "No valid NSC_CODE values found after normalization"
            )

        return df

    def _ensure_date_column(self, df: pl.DataFrame) -> pl.DataFrame:
        """Ensure date column exists and is properly formatted."""
        if "date" not in df.columns:
            # Try to find date column with different names
            date_candidates = ["日期", "date", "time", "日期时间"]
            for candidate in date_candidates:
                if candidate in df.columns:
                    df = df.rename({candidate: "date"})
                    break

        if "date" not in df.columns:
            raise ValueError("No date column found")

        # Convert to date format
        if df["date"].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col("date")
                .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                .alias("date")
            )

        return df

    def _cast_numeric_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Cast sum columns to numeric types."""
        for col in self.sum_columns:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .cast(pl.Utf8)
                    .str.replace_all(",", "")
                    .cast(pl.Float64)
                    .alias(col)
                )

        return df

    def _aggregate_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Group by NSC_CODE and date, aggregate numeric columns."""
        group_cols = ["NSC_CODE", "date"]

        # Build aggregation expressions
        agg_exprs = []
        for col in self.sum_columns:
            if col in df.columns:
                agg_exprs.append(pl.col(col).sum().alias(col))

        if agg_exprs:
            df = df.group_by(group_cols).agg(agg_exprs)
        else:
            df = df.unique(subset=group_cols)

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


class VideoFormatter:
    """Format video data for output."""

    @staticmethod
    def format_for_export(df: pl.DataFrame) -> pl.DataFrame:
        """Format video data for export."""
        # Round numeric columns to appropriate precision
        numeric_cols = [
            "anchor_exposure",
            "component_clicks",
            "short_video_count",
            "short_video_leads",
        ]

        for col in numeric_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col).round(2).alias(col))

        # Format percentage columns
        if "click_through_rate" in df.columns:
            df = df.with_columns(
                (pl.col("click_through_rate") * 100)
                .round(2)
                .alias("click_through_rate_pct")
            )

        return df


def create_video_transform() -> VideoTransform:
    """Factory function to create video transformer."""
    return VideoTransform()
