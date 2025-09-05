"""Base transformation interface."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import polars as pl
from .utils import (
    rename_columns,
    normalize_nsc_code,
    ensure_date_column,
    cast_numeric_columns,
    aggregate_data,
)


class BaseTransform(ABC):
    """Base interface for data transformations."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}

    @abstractmethod
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform input DataFrame to standardized format."""
        pass

    def validate_input(self, df: pl.DataFrame) -> bool:
        """Validate if input DataFrame has required columns."""
        required_cols = self.get_required_columns()
        if not required_cols:
            return True

        return all(col in df.columns for col in required_cols)

    def get_required_columns(self) -> List[str]:
        """Get list of required columns for this transformation."""
        return []

    def add_computed_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add computed columns specific to this data source."""
        return df

    def normalize_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize column names and types."""
        return df
    
    def _rename_columns(self, df: pl.DataFrame, mapping: Dict[str, str]) -> pl.DataFrame:
        """Rename columns based on mapping."""
        return rename_columns(df, mapping)
    
    def _normalize_nsc_code(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize NSC_CODE column."""
        return normalize_nsc_code(df)
    
    def _ensure_date_column(self, df: pl.DataFrame, date_candidates: Optional[List[str]] = None) -> pl.DataFrame:
        """Ensure date column exists and is properly formatted."""
        return ensure_date_column(df, date_candidates)
    
    def _cast_numeric_columns(self, df: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
        """Cast specified columns to numeric types."""
        return cast_numeric_columns(df, columns)
    
    def _aggregate_data(self, df: pl.DataFrame, group_cols: List[str], sum_columns: List[str]) -> pl.DataFrame:
        """Group by specified columns and aggregate numeric columns."""
        return aggregate_data(df, group_cols, sum_columns)
