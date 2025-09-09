"""Account BI data transformation."""

import polars as pl
from typing import Dict
from src.transforms.base import BaseTransformer
from src.transforms.utils import aggregate_by_keys
from src.config.source_mappings import ACCOUNT_BI_MAP

class AccountBITransform(BaseTransformer):
    """
    Transforms raw account BI data by aggregating daily metrics per NSC_CODE.
    """

    @property
    def get_input_rename_map(self) -> Dict[str, str]:
        """
        Defines the mapping from original source column names to standardized names.
        """
        return ACCOUNT_BI_MAP

    @property
    def get_output_schema(self) -> Dict[str, pl.DataType]:
        """
        Defines the final output schema for the account BI data.
        """
        return {
            "NSC_CODE": pl.Utf8,
            "date": pl.Date,
            "live_leads": pl.Float64,
            "short_video_plays": pl.Float64,
        }

    def _apply_transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Applies the core aggregation logic for account BI metrics.
        """
        metric_columns = list(self.get_output_schema.keys() - {"NSC_CODE", "date"})
        return aggregate_by_keys(
            df,
            group_keys=["NSC_CODE", "date"],
            metric_columns=metric_columns,
        )
