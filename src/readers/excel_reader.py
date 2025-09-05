"""Excel file reader."""

import polars as pl
from .base import BaseReader


class ExcelReader(BaseReader):
    """Reader for Excel files."""

    def read(self, path: str, sheet_name=None, **kwargs) -> pl.DataFrame:
        """Read Excel file using polars."""
        # Default to first sheet if not specified
        if sheet_name is None:
            sheet_name = kwargs.get("sheet", 0)

        read_config = {
            "xlsx2csv_options": {"ignore_formats": ["date"]},
            "read_csv_options": {
                "ignore_errors": True,
                "truncate_ragged_lines": True,
            },
            **kwargs,
        }

        return pl.read_excel(path, sheet_name=sheet_name, **read_config)

    def validate_path(self, path: str) -> bool:
        """Validate Excel file path."""
        return path.lower().endswith((".xlsx", ".xls"))
