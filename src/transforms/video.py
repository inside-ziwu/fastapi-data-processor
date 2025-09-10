import polars as pl
from .base import BaseTransformer
from ..config.source_mappings import VIDEO_MAP

class VideoTransform(BaseTransformer):
    def __init__(self):
        super().__init__(VIDEO_MAP)

    def transform(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        lf = self.rename_and_select(lf)
        metric_cols = ["video_anchor_exposures", "video_anchor_clicks", "video_new_posts", "video_form_leads"]
        consolidated_lf = lf.group_by(["nsc_code", "date"]).agg([
            pl.col(c).sum() for c in metric_cols
        ])
        return self.cast_to_float(consolidated_lf, metric_cols)
