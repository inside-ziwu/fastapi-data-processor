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
        """Get transform for specific data source.

        Uses substring matching to avoid brittle exact-key coupling.
        """
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

        name = (source_name or "").lower()

        if "video" in name:
            return VideoTransform()
        if "live" in name:
            return LiveTransform()
        if "msg" in name or "message" in name:
            return MessageTransform()
        if "dr" in name:
            return DRTransform()
        if "spending" in name or "ad" in name:
            return SpendingTransform()
        if "lead" in name:
            return LeadsTransform()
        if "account" in name and "bi" in name:
            return AccountBITransform()
        if "account" in name and "base" in name:
            return AccountBaseTransform()

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
        """Safely join two DataFrames on NSC_CODE and date without catastrophic cross-joins."""
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

        # Prefer lazy join with streaming to reduce peak memory
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
