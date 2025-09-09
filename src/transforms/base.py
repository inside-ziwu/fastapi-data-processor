"""Base transformation class that enforces the data contract."""

from abc import ABC, abstractmethod
from typing import Dict, List
import polars as pl
from src.transforms.utils import (
    apply_strict_rename,
    normalize_nsc_code,
    normalize_date_column,
    cast_numeric_columns,
)

class BaseTransformer(ABC):
    """
    An abstract base class for all data transformations.

    This class implements the Template Method pattern. The `transform` method defines
    a fixed pipeline that all subclasses must follow, ensuring that the data contract
    is enforced at every step.

    Subclasses must implement:
    - get_input_rename_map(): Defines the expected input columns and their target names.
    - get_output_schema(): Defines the exact output columns and their data types.
    - _apply_transform(): Implements the core, source-specific transformation logic.
    """

    @property
    @abstractmethod
    def get_input_rename_map(self) -> Dict[str, str]:
        """Return the mapping from source column names to standardized internal names."""
        pass

    @property
    @abstractmethod
    def get_output_schema(self) -> Dict[str, pl.DataType]:
        """Return the final schema for the output DataFrame."""
        pass

    @abstractmethod
    def _apply_transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Core transformation logic to be implemented by subclasses."""
        pass

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Execute the full transformation pipeline, enforcing the data contract."""
        # 1. Rename columns strictly according to the input contract.
        df = apply_strict_rename(df, self.get_input_rename_map)

        # 2. Normalize primary key columns (NSC_CODE and date).
        # The 'account_base' source is a special dimension table without a date.
        is_dimension_table = "date" not in self.get_output_schema

        df = normalize_nsc_code(df, nsc_column="NSC_CODE")
        if not is_dimension_table:
            df = normalize_date_column(df, date_column="date")

        # 3. Apply the source-specific core transformation logic.
        df = self._apply_transform(df)

        # 4. Cast all numeric columns as defined in the output schema.
        numeric_cols = [
            col for col, dtype in self.get_output_schema.items() 
            if dtype in [pl.Float64, pl.Int64]
        ]
        df = cast_numeric_columns(df, numeric_cols)

        # 5. Enforce the final output schema.
        # This is the final gatekeeper for the data contract.
        output_expressions = []
        for col_name, col_type in self.get_output_schema.items():
            if col_name in df.columns:
                output_expressions.append(pl.col(col_name).cast(col_type))
            else:
                # If a column is missing, create it with null values.
                output_expressions.append(pl.lit(None, dtype=col_type).alias(col_name))
        
        df = df.select(output_expressions)

        return df
