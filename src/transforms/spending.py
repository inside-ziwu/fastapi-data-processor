import polars as pl
from .base import BaseTransformer
from ..config.source_mappings import SPENDING_MAP

class SpendingTransform(BaseTransformer):
    def __init__(self):
        super().__init__(SPENDING_MAP)

    def transform(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        lf = self.rename_and_select(lf)
        metric_cols = ["spending_net"]
        consolidated_lf = lf.group_by(["nsc_code", "date"]).agg([
            pl.col(c).sum() for c in metric_cols
        ])
        return self.cast_to_float(consolidated_lf, metric_cols)
