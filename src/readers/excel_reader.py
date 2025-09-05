"""Excel file reader."""

import logging
import polars as pl
from .base import BaseReader

logger = logging.getLogger(__name__)

class ExcelReader(BaseReader):
    """Reader for Excel files."""

    def read(self, path: str, sheet_name=None, **kwargs) -> pl.DataFrame:
        """Read Excel file using polars with fallback to openpyxl."""
        # Default to first sheet if not specified
        if sheet_name is None:
            sheet_name = kwargs.get("sheet", 0)

        try:
            # Try polars + fastexcel first
            read_config = {
                "xlsx2csv_options": {"ignore_formats": ["date"]},
                "read_csv_options": {
                    "ignore_errors": True,
                    "truncate_ragged_lines": True,
                },
                **kwargs,
            }
            return pl.read_excel(path, sheet_name=sheet_name, **read_config)
        except ImportError as e:
            if "fastexcel" in str(e):
                logger.warning("fastexcel not available, falling back to openpyxl")
                return self._read_with_openpyxl(path, sheet_name, **kwargs)
            raise
        except Exception as e:
            logger.error(f"Polars Excel read failed: {e}")
            raise

    def _read_with_openpyxl(self, path: str, sheet_name, **kwargs) -> pl.DataFrame:
        """Fallback Excel reading using openpyxl."""
        try:
            import openpyxl
            import pandas as pd
            
            # Read with pandas + openpyxl
            df_pandas = pd.read_excel(path, sheet_name=sheet_name, engine='openpyxl')
            
            # Convert to polars
            return pl.from_pandas(df_pandas)
            
        except ImportError:
            raise ImportError("Neither fastexcel nor openpyxl is available for Excel reading")
        except Exception as e:
            logger.error(f"Openpyxl fallback failed: {e}")
            raise

    def validate_path(self, path: str) -> bool:
        """Validate Excel file path."""
        return path.lower().endswith((".xlsx", ".xls"))
