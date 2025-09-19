"""Message/private chat data transformation."""

import logging
import polars as pl
from typing import Any, Dict, List, Optional
from .base import BaseTransform
from ..config import MSG_MAP


logger = logging.getLogger(__name__)


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

        total_rows = df.height
        if "日期" in df.columns:
            null_raw = int(df["日期"].null_count())
            if null_raw:
                logger.warning(
                    f"[message-transform] before date normalize rows={total_rows}, null '日期' rows={null_raw}"
                )

        # Step 2: Normalize NSC_CODE column
        df = self._normalize_nsc_code(df)

        # Step 3: Ensure date column (strict) — fail-fast if missing/invalid
        df = self._ensure_date_column(df, ["日期", "date", "time", "私信日期", "消息日期"]) 

        if "date" in df.columns:
            null_after = int(df["date"].null_count())
            if null_after:
                sample = []
                try:
                    sample = (
                        df.filter(pl.col("date").is_null())
                        .select([c for c in ["NSC_CODE"] + self.sum_columns if c in df.columns])
                        .head(3)
                        .to_dicts()
                    )
                except Exception:
                    pass
                logger.warning(
                    f"[message-transform] after ensure_date rows={df.height}, null date rows={null_after}, sample={sample}"
                )
            else:
                logger.info(
                    f"[message-transform] after ensure_date rows={df.height}, null date rows=0"
                )

        # Step 4: Cast numeric columns
        df = self._cast_numeric_columns(df, self.sum_columns)

        # Step 5: Extraction-only — no aggregation
        wanted = ["NSC_CODE"] + (["date"] if "date" in df.columns else []) + self.sum_columns
        present = [c for c in wanted if c in df.columns]
        df = df.select(present)

        return df
        


def create_message_transform() -> MessageTransform:
    """Factory function to create message transformer."""
    return MessageTransform()
