"""Account base (dimension) data transformation."""

import polars as pl
from typing import Dict
from src.transforms.base import BaseTransformer
from src.config.source_mappings import ACCOUNT_BASE_MAP

class AccountBaseTransform(BaseTransformer):
    """
    Transforms the pre-merged dealer dimension data.
    
    This is a special transformer for a dimension table:
    - It does not have a 'date' column.
    - It aggregates by 'NSC_CODE' only.
    - It uses a 'first' aggregation strategy to get the first non-null value
      for dimension attributes, rather than summing.
    """

    @property
    def get_input_rename_map(self) -> Dict[str, str]:
        """
        Defines the mapping for the DataFrame pre-merged by the processor.
        The processor is expected to provide 'NSC_CODE', 'level', and 'store_name'.
        """
        return ACCOUNT_BASE_MAP

    @property
    def get_output_schema(self) -> Dict[str, pl.DataType]:
        """
        Defines the final output schema for the dealer dimension table.
        All columns are strings.
        """
        return {
            "NSC_CODE": pl.Utf8,
            "level": pl.Utf8,
            "store_name": pl.Utf8,
        }

    def _apply_transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Applies the core aggregation logic for the dimension table.
        It groups by NSC_CODE and takes the first non-null value for 'level'
        and 'store_name' to create a unique record per dealer.
        """
        # The base `transform` method already handles NSC_CODE normalization.
        # We just need to perform the specific aggregation.
        
        # Ensure all aggregation columns exist before grouping
        required_cols = ["NSC_CODE", "level", "store_name"]
        for col in required_cols:
            if col not in df.columns:
                # If a column is missing, add it with nulls to prevent errors
                df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

        return df.group_by("NSC_CODE").agg(
            pl.col("level").first().cast(pl.Utf8),
            pl.col("store_name").first().cast(pl.Utf8),
        )
