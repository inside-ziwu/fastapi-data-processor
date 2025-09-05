"""Excel file reader."""

import logging
import polars as pl
from .base import BaseReader

logger = logging.getLogger(__name__)

# Check dependencies at module level for clean logic
FASTEXCEL_AVAILABLE = False
OPENPYXL_AVAILABLE = False

try:
    import fastexcel
    FASTEXCEL_AVAILABLE = True
except ImportError:
    pass

try:
    import openpyxl
    OPENPYXL_AVAILABLE = True
except ImportError:
    pass


class ExcelReader(BaseReader):
    """Reader for Excel files."""

    def read(self, path: str, sheet_name=None, **kwargs) -> pl.DataFrame:
        """Read Excel file using native type inference of the backend."""
        # Handle sheet selection - calamine uses sheet_id, openpyxl uses sheet_name
        if sheet_name is None:
            sheet_name = kwargs.get("sheet", 0)
        
        if FASTEXCEL_AVAILABLE:
            # Calamine engine prefers sheet_id over sheet_name for index access
            if isinstance(sheet_name, int):
                return self._read_with_fastexcel(path, sheet_id=sheet_name, **kwargs)
            else:
                return self._read_with_fastexcel(path, sheet_name=sheet_name, **kwargs)
        elif OPENPYXL_AVAILABLE:
            logger.warning("fastexcel not available, using openpyxl fallback")
            # Remove fastexcel-specific kwargs before passing to openpyxl
            openpyxl_kwargs = {k: v for k, v in kwargs.items() if k not in ['xlsx2csv_options', 'ignore_formats']}
            return self._read_with_openpyxl(path, sheet_name, **openpyxl_kwargs)
        else:
            raise ImportError("Neither fastexcel nor openpyxl is available for Excel reading")

    def _read_with_fastexcel(self, path: str, sheet_name=None, sheet_id=None, **kwargs) -> pl.DataFrame:
        """Read Excel via fastexcel (calamine) and preserve native types."""
        if sheet_id is not None:
            df = pl.read_excel(path, sheet_id=sheet_id, **kwargs)
        elif sheet_name is not None:
            df = pl.read_excel(path, sheet_name=sheet_name, **kwargs)
        else:
            df = pl.read_excel(path, **kwargs)

        # polars may return a mapping when reading multiple sheets without selection
        if isinstance(df, dict):
            df = next(iter(df.values()))
        return df

    def _read_with_openpyxl(self, path: str, sheet_name, **kwargs) -> pl.DataFrame:
        """Read Excel using openpyxl and preserve native types."""
        import pandas as pd
        
        # Delegate type inference to pandas/openpyxl without forcing string casts
        df_pandas = pd.read_excel(
            path,
            sheet_name=sheet_name,
            engine="openpyxl",
            **kwargs,
        )
        
        # Convert to polars
        return pl.from_pandas(df_pandas)

    def validate_path(self, path: str) -> bool:
        """Validate Excel file path."""
        return path.lower().endswith((".xlsx", ".xls"))
