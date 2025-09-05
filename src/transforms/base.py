"""Base transformation interface."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import polars as pl


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
