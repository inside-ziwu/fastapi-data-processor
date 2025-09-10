"""Account base (dimension) data transformation."""

import polars as pl
from typing import Any, Dict, List, Optional
from .base import BaseTransform
from ..config import ACCOUNT_BASE_MAP


class AccountBaseTransform(BaseTransform):
    """Transform account base dimension (no date)."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.mapping = ACCOUNT_BASE_MAP
        self.sum_columns: List[str] = []

    def get_required_columns(self) -> List[str]:
        return list(self.mapping.keys())

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        df = self._rename_columns(df, self.mapping)
        df = self._normalize_nsc_code(df)

        # Hotfix: Cast store_name to string to avoid mixed type errors
        if "store_name" in df.columns:
            df = df.with_columns(pl.col("store_name").cast(pl.Utf8))

        # 仅保留明确需求字段
        wanted = [v for v in self.mapping.values()]
        wanted_unique = []
        for c in wanted:
            if c not in wanted_unique and c in df.columns:
                wanted_unique.append(c)
        df = df.select(wanted_unique)
        # No date column here; join will fallback to NSC_CODE-only
        df = df.unique(subset=["NSC_CODE"]) if "NSC_CODE" in df.columns else df
        return df
