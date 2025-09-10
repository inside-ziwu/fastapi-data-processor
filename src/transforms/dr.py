import polars as pl
from .base import BaseTransformer
from ..config.source_mappings import DR_MAP
from ..config.enum_maps import LEADS_TYPE_MAP, CHANNEL_MAP_WHITELIST

class DRTransform(BaseTransformer):
    def __init__(self):
        super().__init__(DR_MAP)

    def transform(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        lf = self.rename_and_select(lf).with_columns([
            pl.col("leads_type").replace_strict(LEADS_TYPE_MAP, default="OTHER").alias("leads_type_std"),
            pl.col("mkt_second_channel_name").replace_strict(CHANNEL_MAP_WHITELIST, default="OTHER").alias("channel_std"),
        ])

        # Idempotency key for deduplication
        idemp_key = pl.concat_str([
           pl.col('nsc_code'), pl.lit('|'), pl.col('date').cast(pl.Utf8),
           pl.lit('|'), pl.col('leads_type_std'), pl.lit('|'), pl.col('channel_std'),
           pl.lit('|'), pl.col('send2dealer_id').cast(pl.Utf8).fill_null("")
        ]).alias('__dedup_key')
        lf = lf.with_columns(idemp_key).unique(subset='__dedup_key', keep='first').drop('__dedup_key')

        # Group by and aggregate using boolean sum
        lf_agg = lf.group_by(["nsc_code","date"]).agg(
            (pl.col("leads_type_std")=="NATURAL").sum().alias("natural_leads"),
            (pl.col("leads_type_std")=="PAID").sum().alias("paid_leads"),
            (pl.col("send2dealer_id").cast(pl.Utf8)==pl.col("nsc_code")).sum().alias("local_leads"),
            ((pl.col("leads_type_std")=="PAID") & (pl.col("channel_std")=="CLOUD_LOCAL")).sum().alias("cloud_store_paid_leads"),
            ((pl.col("leads_type_std")=="PAID") & (pl.col("channel_std")=="REGIONAL")).sum().alias("regional_paid_leads"),
        )
        
        metric_cols = ["natural_leads", "paid_leads", "local_leads", "cloud_store_paid_leads", "regional_paid_leads"]
        return self.cast_to_float(lf_agg, metric_cols)
