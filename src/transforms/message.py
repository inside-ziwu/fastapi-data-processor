"""Message/private chat data transformation."""

import polars as pl
from typing import Dict
from src.transforms.base import BaseTransformer
from src.transforms.utils import aggregate_by_keys

class MessageTransform(BaseTransformer):
    """
    Transforms raw private message data by aggregating daily metrics per NSC_CODE.

    Note: The processor is expected to have already extracted the date from
    sheet names and added it as a '日期' column to the DataFrame.
    """

    @property
    def get_input_rename_map(self) -> Dict[str, str]:
        """
        Defines the mapping from original source column names to standardized names.
        The '日期' column is added by the processor.
        """
        return {
            "主机厂经销商ID": "NSC_CODE",
            "日期": "date",
            "进入私信客户数": "enter_private_count",
            "主动咨询客户数": "private_open_count",
            "私信留资客户数": "private_leads_count",
        }

    @property
    def get_output_schema(self) -> Dict[str, pl.DataType]:
        """
        Defines the final output schema for the private message data.
        """
        return {
            "NSC_CODE": pl.Utf8,
            "date": pl.Date,
            "enter_private_count": pl.Float64,
            "private_open_count": pl.Float64,
            "private_leads_count": pl.Float64,
        }

    def _apply_transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Applies the core aggregation logic for private message metrics.
        """
        metric_columns = list(self.get_output_schema.keys() - {"NSC_CODE", "date"})
        return aggregate_by_keys(
            df,
            group_keys=["NSC_CODE", "date"],
            metric_columns=metric_columns,
        )