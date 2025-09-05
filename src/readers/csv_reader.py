"""CSV file reader."""

import polars as pl
from .base import BaseReader


class CSVReader(BaseReader):
    """Reader for CSV files."""

    def read(self, path: str, **kwargs) -> pl.DataFrame:
        """Read CSV file using polars."""
        # Default configuration for CSV reading
        read_config = {
            "ignore_errors": True,
            "truncate_ragged_lines": True,
            "try_parse_dates": True,
            **kwargs,
        }

        return pl.read_csv(path, **read_config)

    def validate_path(self, path: str) -> bool:
        """Validate CSV file path."""
        return path.lower().endswith((".csv", ".txt"))
