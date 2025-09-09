"""Leads data transformation."""

import polars as pl
from typing import Dict
from src.transforms.base import BaseTransformer
from src.transforms.utils import aggregate_by_keys

class LeadsTransform(BaseTransformer):
    """
    Transforms raw leads data by aggregating daily metrics per NSC_CODE.
    """

    @property
    def get_input_rename_map(self) -> Dict[str, str]:
        """
        Defines the mapping from original source column names to standardized names.
        """
        return {
            "主机厂经销商id列表": "NSC_CODE",
            "留资日期": "date",
            "直播间表单提交商机量(去重)": "small_wheel_leads",
        }

    @property
    def get_output_schema(self) -> Dict[str, pl.DataType]:
        """
        Defines the final output schema for the leads data.
        """
        return {
            "NSC_CODE": pl.Utf8,
            "date": pl.Date,
            "small_wheel_leads": pl.Float64,
        }

    def _apply_transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Applies the core aggregation logic for leads metrics.
        """
        return aggregate_by_keys(
            df,
            group_keys=["NSC_CODE", "date"],
            metric_columns=["small_wheel_leads"],
        )