"""Live streaming data transformation."""

import polars as pl
from typing import Dict
from src.transforms.base import BaseTransformer
from src.transforms.utils import aggregate_by_keys
from src.config.source_mappings import LIVE_MAP

class LiveTransform(BaseTransformer):
    """
    Transforms raw live streaming data by aggregating daily metrics per NSC_CODE.
    """

    @property
    def get_input_rename_map(self) -> Dict[str, str]:
        """
        Defines the mapping from original source column names to standardized names.
        """
        return LIVE_MAP

    @property
    def get_output_schema(self) -> Dict[str, pl.DataType]:
        """
        Defines the final output schema for the live streaming data.
        """
        return {
            "NSC_CODE": pl.Utf8,
            "date": pl.Date,
            "over25_min_live_mins": pl.Float64,
            "live_effective_hours": pl.Float64,
            "effective_live_sessions": pl.Float64,
            "exposures": pl.Float64,
            "viewers": pl.Float64,
            "small_wheel_clicks": pl.Float64,
        }

    def _apply_transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Applies the core aggregation logic for live streaming metrics.
        """
        metric_columns = list(self.get_output_schema.keys() - {"NSC_CODE", "date"})
        return aggregate_by_keys(
            df,
            group_keys=["NSC_CODE", "date"],
            metric_columns=metric_columns,
        )
