"""Data readers for different file formats and sources."""

from .base import BaseReader, ReaderRegistry
from .csv_reader import CSVReader
from .excel_reader import ExcelReader

__all__ = [
    "BaseReader",
    "ReaderRegistry",
    "CSVReader",
    "ExcelReader",
]

# 自动注册读取器
registry = ReaderRegistry()
registry.register("csv", CSVReader)
registry.register("xlsx", ExcelReader)
registry.register("xls", ExcelReader)
