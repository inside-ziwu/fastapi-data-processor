"""Spending data transformation."""

import polars as pl
from typing import Dict
from src.transforms.base import BaseTransformer
from src.transforms.utils import aggregate_by_keys
from src.config.source_mappings import SPENDING_MAP

class SpendingTransform(BaseTransformer):
    """
    Transforms raw spending data by aggregating daily spending per NSC_CODE.
    
    Note: The processor is expected to have already pre-processed the Excel file
    by reading and concatenating only the required columns from multiple sheets.
    """

    @property
    def get_input_rename_map(self) -> Dict[str, str]:
        """
        Defines the mapping from the pre-selected source columns to standardized names.
        """
        return SPENDING_MAP

    @property
    def get_output_schema(self) -> Dict[str, pl.DataType]:
        """
        Defines the final output schema for the spending data.
        """
        return {
            "NSC_CODE": pl.Utf8,
            "date": pl.Date,
            "spending": pl.Float64,
        }

    def _apply_transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Applies the core aggregation logic. The input DataFrame is expected to be
        clean and contain only the three required columns.
        """
        return aggregate_by_keys(
            df,
            group_keys=["NSC_CODE", "date"],
            metric_columns=["spending"],
        )