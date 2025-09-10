import polars as pl
from .base import BaseTransformer
from ..config.source_mappings import ACCOUNT_BI_MAP

class AccountBITransform(BaseTransformer):
    def __init__(self):
        super().__init__(ACCOUNT_BI_MAP)

    def transform(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        lf = self.rename_and_select(lf)
        metric_cols = ["account_bi_live_form_leads", "account_bi_video_views"]
        consolidated_lf = lf.group_by(["nsc_code", "date"]).agg([
            pl.col(c).sum() for c in metric_cols
        ])
        return self.cast_to_float(consolidated_lf, metric_cols)
