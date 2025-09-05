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
        """Read Excel file with consistent data types across backends."""
        # Handle sheet selection - calamine uses sheet_id, openpyxl uses sheet_name
        if sheet_name is None:
            sheet_name = kwargs.get("sheet", 0)
        
        if FASTEXCEL_AVAILABLE:
            # Calamine engine prefers sheet_id over sheet_name for index access
            if isinstance(sheet_name, int):
                return self._read_with_calamine(path, sheet_id=sheet_name, **kwargs)
            else:
                return self._read_with_calamine(path, sheet_name=sheet_name, **kwargs)
        elif OPENPYXL_AVAILABLE:
            logger.warning("fastexcel not available, using openpyxl fallback")
            # Remove fastexcel-specific kwargs before passing to openpyxl
            openpyxl_kwargs = {k: v for k, v in kwargs.items() if k not in ['xlsx2csv_options', 'ignore_formats']}
            return self._read_with_openpyxl(path, sheet_name, **openpyxl_kwargs)
        else:
            raise ImportError("Neither fastexcel nor openpyxl is available for Excel reading")

    def _read_with_calamine(self, path: str, sheet_name=None, sheet_id=None, **kwargs) -> pl.DataFrame:
        """Read Excel using calamine engine with consistent date handling."""
        # Use polars 1.8.2 API with calamine engine
        # Remove calamine-unsupported options
        calamine_kwargs = {k: v for k, v in kwargs.items() if k not in ['ignore_errors', 'truncate_ragged_lines']}
        
        read_config = {
            "engine": "calamine",
            **calamine_kwargs,
        }
        
        # Handle sheet selection - prefer sheet_id over sheet_name for calamine
        if sheet_id is not None:
            result = pl.read_excel(path, sheet_id=sheet_id, **read_config)
        elif sheet_name is not None:
            result = pl.read_excel(path, sheet_name=sheet_name, **read_config)
        else:
            result = pl.read_excel(path, **read_config)
        
        # Calamine returns dict {sheet_name: DataFrame}, we need to return single DataFrame
        if isinstance(result, dict):
            # Return first sheet if multiple sheets returned
            df = list(result.values())[0]
        else:
            df = result
            
        # Force all columns to string to match openpyxl behavior
        return df.select([pl.col(col).cast(pl.Utf8) for col in df.columns])

    def _read_with_openpyxl(self, path: str, sheet_name, **kwargs) -> pl.DataFrame:
        """Read Excel using openpyxl with same date behavior as fastexcel."""
        import pandas as pd
        
        # Read with pandas + openpyxl, but disable date parsing to match fastexcel
        df_pandas = pd.read_excel(
            path, 
            sheet_name=sheet_name, 
            engine='openpyxl',
            dtype=str,  # Force all columns to string to match fastexcel behavior
            keep_default_na=False  # Don't convert empty strings to NaN
        )
        
        # Convert to polars
        return pl.from_pandas(df_pandas)

    def validate_path(self, path: str) -> bool:
        """Validate Excel file path."""
        return path.lower().endswith((".xlsx", ".xls"))
