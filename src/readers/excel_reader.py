"""Excel file reader: keep it simple, always use openpyxl via pandas."""

import polars as pl
from .base import BaseReader


class ExcelReader(BaseReader):
    """Reader for Excel files (openpyxl only)."""

    def read(self, path: str, sheet_name=None, **kwargs) -> pl.DataFrame:
        """Read Excel via pandas/openpyxl, then convert to polars."""
        import pandas as pd

        if sheet_name is None:
            sheet_name = kwargs.get("sheet", 0)

        # Strip any engine-specific kwargs not applicable here
        pandas_kwargs = {k: v for k, v in kwargs.items() if k not in ["xlsx2csv_options", "ignore_formats", "sheet_id"]}

        df_pd = pd.read_excel(
            path,
            sheet_name=sheet_name,
            engine="openpyxl",
            **pandas_kwargs,
        )

        return pl.from_pandas(df_pd)

    def validate_path(self, path: str) -> bool:
        return path.lower().endswith((".xlsx", ".xls"))
