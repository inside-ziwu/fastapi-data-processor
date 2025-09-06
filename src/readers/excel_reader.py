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
        # Ensure string unique headers
        try:
            cols = [str(c) for c in df_pd.columns]
            seen = {}
            uniq = []
            for c in cols:
                if c in seen:
                    seen[c] += 1
                    uniq.append(f"{c}_{seen[c]}")
                else:
                    seen[c] = 0
                    uniq.append(c)
            df_pd.columns = uniq
        except Exception:
            pass
        try:
            return pl.from_pandas(df_pd)
        except Exception:
            # Fallback: dict-of-lists
            data = {str(k): list(df_pd[k].values) for k in df_pd.columns}
            return pl.DataFrame(data)

    def validate_path(self, path: str) -> bool:
        return path.lower().endswith((".xlsx", ".xls"))
