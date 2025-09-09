"""Video data transformation."""

import polars as pl
from typing import Dict
from src.transforms.base import BaseTransformer
from src.transforms.utils import aggregate_by_keys

class VideoTransform(BaseTransformer):
    """
    Transforms raw video data by aggregating daily metrics per NSC_CODE.
    """

    @property
    def get_input_rename_map(self) -> Dict[str, str]:
        """
        Defines the mapping from original source column names to standardized names.
        """
        return {
            "主机厂经销商id": "NSC_CODE",
            "日期": "date",
            "锚点曝光次数": "anchor_exposure",
            "锚点点击次数": "component_clicks",
            "新发布视频数": "short_video_count",
            "短视频表单提交商机量": "short_video_leads",
        }

    @property
    def get_output_schema(self) -> Dict[str, pl.DataType]:
        """
        Defines the final output schema for the video data.
        """
        return {
            "NSC_CODE": pl.Utf8,
            "date": pl.Date,
            "anchor_exposure": pl.Float64,
            "component_clicks": pl.Float64,
            "short_video_count": pl.Float64,
            "short_video_leads": pl.Float64,
        }

    def _apply_transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Applies the core aggregation logic for video metrics.
        """
        metric_columns = list(self.get_output_schema.keys() - {"NSC_CODE", "date"})
        return aggregate_by_keys(
            df,
            group_keys=["NSC_CODE", "date"],
            metric_columns=metric_columns,
        )