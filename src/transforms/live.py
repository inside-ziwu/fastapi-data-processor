"""Live streaming data transformation."""

import polars as pl
from typing import Dict
from src.transforms.base import BaseTransformer
from src.transforms.utils import aggregate_by_keys

class LiveTransform(BaseTransformer):
    """
    Transforms raw live streaming data by aggregating daily metrics per NSC_CODE.
    """

    @property
    def get_input_rename_map(self) -> Dict[str, str]:
        """
        Defines the mapping from original source column names to standardized names.
        """
        return {
            "主机厂经销商id列表": "NSC_CODE",
            "开播日期": "date",
            "超25分钟直播时长(分)": "over25_min_live_mins",
            "直播有效时长（小时）": "live_effective_hours",
            "超25min直播总场次": "effective_live_sessions",
            "曝光人数": "exposures",
            "场观": "viewers",
            "小风车点击次数（不含小雪花）": "small_wheel_clicks",
        }

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