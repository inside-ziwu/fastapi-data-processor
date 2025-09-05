"""Main data processing pipeline."""

import logging
from typing import Dict, List, Optional, Any
import polars as pl
from pathlib import Path

from .readers import ReaderRegistry, registry as reader_registry
from .transforms import BaseTransform
from .analysis import create_default_analysis_engine
from .config import FIELD_MAPPINGS

logger = logging.getLogger(__name__)


class DataProcessor:
    """Main data processing pipeline."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.reader_registry = reader_registry
        self.analysis_engine = create_default_analysis_engine()
        # Build transform mapping once with aliases for clarity and maintainability
        from .transforms import (
            VideoTransform,
            LiveTransform,
            MessageTransform,
            DRTransform,
            SpendingTransform,
            LeadsTransform,
            AccountBITransform,
            AccountBaseTransform,
        )

        self.TRANSFORM_MAP: Dict[str, type[BaseTransform]] = {
            "video": VideoTransform,
            "live": LiveTransform,
            "message": MessageTransform,
            "msg": MessageTransform,  # alias
            "dr": DRTransform,
            "spending": SpendingTransform,
            "ad": SpendingTransform,  # alias
            "lead": LeadsTransform,
            "account_bi": AccountBITransform,
            "account_base": AccountBaseTransform,
        }

    def process_pipeline(self, file_paths: Dict[str, str]) -> pl.DataFrame:
        """
        Process multiple data sources and return unified DataFrame.
        
        Memory-efficient implementation: processes files one by one to avoid OOM.

        Args:
            file_paths: Dict mapping source names to file paths

        Returns:
            Unified DataFrame with all data joined by NSC_CODE and date
        """
        # Step 1: Process files one by one to avoid memory accumulation
        processed_files = []
        
        for source_name, file_path in file_paths.items():
            try:
                df = self._process_single_source(source_name, file_path)
                if df is not None:
                    # Process immediately and add to list, don't keep in memory
                    processed_files.append((source_name, df))
                    logger.info(f"Processed {source_name}: {df.shape[0]} rows, {df.shape[1]} columns")
            except Exception as e:
                logger.warning(f"Failed to process {source_name}: {e}")
                continue

        # Step 2: Stream merge to avoid memory accumulation
        if not processed_files:
            return pl.DataFrame()

        return self._stream_merge_data_sources(processed_files)

    def _process_single_source(
        self, source_name: str, file_path: str
    ) -> Optional[pl.DataFrame]:
        """Process a single data source."""
        # Auto-detect reader
        reader_class = self.reader_registry.auto_detect_reader(file_path)
        if not reader_class:
            raise ValueError(f"No reader found for {file_path}")

        # Read data
        reader = reader_class()
        df = reader.read(file_path)

        # Get appropriate transform
        transform = self._get_transform_for_source(source_name)
        if transform:
            df = transform.transform(df)

        return df

    def _get_transform_for_source(
        self, source_name: str
    ) -> Optional[BaseTransform]:
        """通过查找表获取数据源对应的转换器。"""
        name = (source_name or "").lower()

        # Normalize special compound keys first
        key = name
        if "account" in name and "bi" in name:
            key = "account_bi"
        elif "account" in name and "base" in name:
            key = "account_base"

        # Exact match first (O(1))
        transform_class = self.TRANSFORM_MAP.get(key)
        if transform_class:
            return transform_class()

        # Optional fallback: conservative fuzzy match against known aliases
        for map_key, cls in self.TRANSFORM_MAP.items():
            if map_key in name:
                return cls()

        return None

    def _stream_merge_data_sources(
        self, processed_files: List[tuple[str, pl.DataFrame]]
    ) -> pl.DataFrame:
        """Stream merge data sources to avoid memory accumulation."""
        if not processed_files:
            return pl.DataFrame()
            
        if len(processed_files) == 1:
            # Only one file, return it directly
            return processed_files[0][1]
        
        # Stream merge: join files one by one to minimize memory usage
        result_df = processed_files[0][1]
        
        for source_name, df in processed_files[1:]:
            try:
                # Use how='left' to preserve all rows from first file
                result_df = self._safe_join(result_df, df, source_name)
                logger.info(f"Merged {source_name}: result shape {result_df.shape}")
            except Exception as e:
                logger.warning(f"Failed to merge {source_name}: {e}")
                # Continue with partial result rather than failing completely
                continue
                
        return result_df

    def _merge_data_sources(
        self, data_sources: Dict[str, pl.DataFrame]
    ) -> pl.DataFrame:
        """Merge all data sources into unified DataFrame."""
        if not data_sources:
            return pl.DataFrame()

        # Start with first data source
        result = list(data_sources.values())[0]

        # Join with remaining sources
        for source_name, df in list(data_sources.items())[1:]:
            result = self._safe_join(result, df, source_name)

        # Apply final analysis computations
        return self.analysis_engine.apply_computations(result)

    def _safe_join(
        self, left: pl.DataFrame, right: pl.DataFrame, source_name: str
    ) -> pl.DataFrame:
        """Safely join two DataFrames on NSC_CODE and/or date.

        - Never cross-join: if no common keys, skip with warning.
        - Prefer lazy streaming join for scalability; fallback to eager join if it fails.
        - Rely on Transform outputs to provide correct dtypes for keys.
        """
        join_keys = ["NSC_CODE", "date"]

        # Determine common join keys
        common_keys = [k for k in join_keys if k in left.columns and k in right.columns]

        if not common_keys:
            # Absolutely do not cross-join. It explodes memory and is almost always wrong.
            logger = logging.getLogger(__name__)
            logger.warning(
                f"Skip merging '{source_name}': no common join keys among {join_keys}. "
                f"left has {left.columns}, right has {right.columns}"
            )
            return left

        # Prefer lazy streaming join for performance/scalability
        try:
            return (
                left.lazy()
                .join(
                    right.lazy(),
                    on=common_keys,
                    how="outer",
                    suffix=f"_{source_name}",
                )
                .collect(streaming=True)
            )
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"Streaming join failed for '{source_name}': {e}. Falling back to eager join.")
            return left.rechunk().join(
                right.rechunk(),
                on=common_keys,
                how="outer",
                suffix=f"_{source_name}",
            )
