"""Account base (dimension) data transformation."""

import polars as pl
from typing import Any, Dict, List, Optional

_TEXT_SENTINELS = {"", "null", "--"}


def _clean_text_expr(column: str) -> pl.Expr:
    trimmed = pl.col(column).cast(pl.Utf8, strict=False).str.strip_chars()
    lowered = trimmed.str.to_lowercase()
    return pl.when(lowered.is_in(list(_TEXT_SENTINELS))).then(None).otherwise(trimmed)
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

        updates: List[pl.Expr] = []
        if "level" in df.columns:
            updates.append(_clean_text_expr("level").alias("level"))
        if "store_name" in df.columns:
            updates.append(_clean_text_expr("store_name").alias("store_name"))
        if updates:
            df = df.with_columns(updates)

        # 仅保留明确需求字段
        wanted = [v for v in self.mapping.values()]
        wanted_unique = []
        for c in wanted:
            if c not in wanted_unique and c in df.columns:
                wanted_unique.append(c)
        df = df.select(wanted_unique)

        if "NSC_CODE" not in df.columns:
            return df

        # Prefer非空值：同一个 NSC_CODE 取层级/门店的首个非空文本
        aggregations = []
        if "level" in df.columns:
            aggregations.append(pl.col("level").drop_nulls().first().alias("level"))
        if "store_name" in df.columns:
            aggregations.append(pl.col("store_name").drop_nulls().first().alias("store_name"))

        if aggregations:
            df = df.group_by("NSC_CODE").agg(aggregations)
        else:
            df = df.unique(subset=["NSC_CODE"])

        return df
