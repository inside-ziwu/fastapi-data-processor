"""Message/private chat data transformation."""

import polars as pl
from typing import Any, Dict, List, Optional
from .base import BaseTransform
from ..config import MSG_MAP


class MessageTransform(BaseTransform):
    """Transform message/private chat data to standardized format."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.mapping = MSG_MAP
        self.sum_columns = [
            "enter_private_count",
            "private_open_count",
            "private_leads_count",
        ]

    def get_required_columns(self) -> List[str]:
        """Required columns for message transformation."""
        return list(self.mapping.keys())

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform message data to standardized format."""
        # Step 1: Rename columns using mapping
        df = self._rename_columns(df, self.mapping)

        # Step 2: Normalize NSC_CODE column
        df = self._normalize_nsc_code(df)

        # Step 3: Ensure date column if present; message数据有时无日期
        df = self._ensure_optional_date_column(df, ["日期", "date", "time", "私信日期", "消息日期"]) 

        # Step 4: Cast numeric columns
        df = self._cast_numeric_columns(df, self.sum_columns)

        # Step 5: Group by present key columns and aggregate
        group_keys = [c for c in ["NSC_CODE", "date"] if c in df.columns]
        df = self._aggregate_data(df, group_keys, self.sum_columns)

        # Step 6: Add computed columns
        df = self.add_computed_columns(df)

        return df

    def add_computed_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add message-specific computed columns."""
        # Add conversion rates if we have the required columns
        if (
            "private_leads_count" in df.columns
            and "private_open_count" in df.columns
            and df["private_open_count"].sum() > 0
        ):

            df = df.with_columns(
                (
                    pl.col("private_leads_count")
                    / pl.col("private_open_count")
                ).alias("private_conversion_rate")
            )

        if (
            "private_open_count" in df.columns
            and "enter_private_count" in df.columns
            and df["enter_private_count"].sum() > 0
        ):

            df = df.with_columns(
                (
                    pl.col("private_open_count")
                    / pl.col("enter_private_count")
                ).alias("open_rate")
            )

        return df


def create_message_transform() -> MessageTransform:
    """Factory function to create message transformer."""
    return MessageTransform()
