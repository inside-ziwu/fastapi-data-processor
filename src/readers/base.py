"""Base reader interface and registry."""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Type
import polars as pl


class BaseReader(ABC):
    """Base interface for all data readers."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}

    @abstractmethod
    def read(self, path: str, **kwargs) -> pl.DataFrame:
        """Read data from file and return DataFrame."""
        pass

    def validate_path(self, path: str) -> bool:
        """Validate if reader can handle this path."""
        return True


class ReaderRegistry:
    """Registry for file readers."""

    def __init__(self):
        self._readers: Dict[str, Type[BaseReader]] = {}

    def register(self, file_type: str, reader_class: Type[BaseReader]):
        """Register a reader for specific file type."""
        self._readers[file_type] = reader_class

    def get_reader(self, file_type: str) -> Optional[Type[BaseReader]]:
        """Get reader for file type."""
        return self._readers.get(file_type)

    def auto_detect_reader(self, path: str) -> Optional[Type[BaseReader]]:
        """Auto-detect by file extension only (simple, predictable)."""
        return self._fallback_extension_detection(path)
        
    def _fallback_extension_detection(self, path: str) -> Optional[Type[BaseReader]]:
        """Fallback extension-based detection for when magic is not available."""
        import os
        
        EXTENSION_MAP = {
            ".csv": "csv",
            ".xlsx": "xlsx", 
            ".xls": "xlsx",
            ".txt": "csv"  # TXT treated as CSV
        }
        
        # Remove pollution after ~ and check clean extension
        basename = os.path.basename(path).lower()
        clean_basename = basename.split('~')[0]  # Remove pollution
        
        for ext, reader_key in EXTENSION_MAP.items():
            if clean_basename.endswith(ext):
                return self._readers.get(reader_key)
                
        return None
