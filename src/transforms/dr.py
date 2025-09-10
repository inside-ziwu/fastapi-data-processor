"""DR data transformation (register/leads records)."""

import polars as pl
from typing import Dict
from src.transforms.base import BaseTransformer
from src.transforms.utils import aggregate_by_keys
from src.config.source_mappings import DR_MAP

class DRTransform(BaseTransformer):
    """
    Transforms raw DR (leads) data by aggregating daily metrics per NSC_CODE.
    """

    @property
    def get_input_rename_map(self) -> Dict[str, str]:
        """
        Defines the mapping from original source column names to the required
        standardized names for processing.
        """
        return DR_MAP

    @property
    def get_output_schema(self) -> Dict[str, pl.DataType]:
        """
        Defines the final output schema, enforcing the data contract for this source.
        """
        return {
            "NSC_CODE": pl.Utf8,
            "date": pl.Date,
            "natural_leads": pl.Float64,
            "paid_leads": pl.Float64,
            "local_leads": pl.Float64,
            "cheyundian_paid_leads": pl.Float64,
            "regional_paid_leads": pl.Float64,
        }

    def _apply_transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Applies the core transformation logic:
        1.  Normalize 'leads_type' for consistent matching.
        2.  Create indicator columns for each metric based on specified conditions.
        3.  Aggregate these metrics by NSC_CODE and date.
        """
        # Normalize leads_type to handle variations like '广告' vs '付费'
        leads_type_norm = (
            pl.col("leads_type")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_lowercase()
        )
        
        # Define expressions for each metric calculation
        df_with_indicators = df.with_columns(
            pl.when(leads_type_norm == "自然").then(1).otherwise(0).alias("natural_leads"),
            pl.when(leads_type_norm.is_in(["广告", "付费"])).then(1).otherwise(0).alias("paid_leads"),
            pl.when(pl.col("NSC_CODE") == pl.col("send2dealer_id").cast(pl.Utf8)).then(1).otherwise(0).alias("local_leads"),
            pl.when(
                (leads_type_norm.is_in(["广告", "付费"])) &
                (pl.col("mkt_second_channel_name").is_in(['抖音车云店_BMW_本市_LS直发', '抖音车云店_LS直发']))
            ).then(1).otherwise(0).alias("cheyundian_paid_leads"),
            pl.when(
                (leads_type_norm.is_in(["广告", "付费"])) &
                (pl.col("mkt_second_channel_name") == '抖音车云店_BMW_总部BDT_LS直发')
            ).then(1).otherwise(0).alias("regional_paid_leads"),
        )

        # Aggregate the indicators by the primary keys
        return aggregate_by_keys(
            df_with_indicators,
            group_keys=["NSC_CODE", "date"],
            metric_columns=[
                "natural_leads",
                "paid_leads",
                "local_leads",
                "cheyundian_paid_leads",
                "regional_paid_leads",
            ],
        )