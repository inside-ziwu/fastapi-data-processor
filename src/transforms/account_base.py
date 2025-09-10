import polars as pl
from .base import BaseTransformer
from ..config.source_mappings import ACCOUNT_BASE_MAP

class AccountBaseTransform(BaseTransformer):
    def __init__(self):
        super().__init__(ACCOUNT_BASE_MAP)

    def transform(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        # Account base is a dimension table, no date consolidation needed.
        # Just rename, select, and ensure uniqueness on nsc_code.
        lf = self.rename_and_select(lf)
        return lf.unique(subset=["nsc_code"], keep="first")
