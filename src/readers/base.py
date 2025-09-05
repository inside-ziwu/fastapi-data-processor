"""Base reader interface and registry."""

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
        """Auto-detect reader based on file extension."""
        path_lower = path.lower()

        if path_lower.endswith(".csv"):
            return self._readers.get("csv")
        elif path_lower.endswith(".xlsx") or path_lower.endswith(".xls"):
            return self._readers.get("xlsx")
        elif path_lower.endswith(".txt"):
            return self._readers.get("csv")  # TXT treated as CSV

        return None
