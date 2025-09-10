import polars as pl
from .base import BaseTransformer
from ..config.source_mappings import LIVE_BI_MAP

class LiveTransform(BaseTransformer):
    def __init__(self):
        super().__init__(LIVE_BI_MAP)

    def transform(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        lf = self.rename_and_select(lf)
        metric_cols = [
            "live_gt_25min_duration_min", "live_effective_duration_hr", 
            "live_gt_25min_sessions", "live_exposures", "live_views", "live_widget_clicks"
        ]
        consolidated_lf = lf.group_by(["nsc_code", "date"]).agg([
            pl.col(c).sum() for c in metric_cols
        ])
        return self.cast_to_float(consolidated_lf, metric_cols)
