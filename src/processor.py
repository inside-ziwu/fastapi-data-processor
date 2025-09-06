"""Main data processing pipeline."""

import logging
from typing import Dict, List, Optional, Any
import polars as pl
from pathlib import Path
import os

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
        # Step 1: Extraction-only processing for each source to avoid early aggregation
        processed_files: list[tuple[str, pl.DataFrame]] = []

        for source_name, file_path in file_paths.items():
            try:
                df = self._process_single_source(source_name, file_path)
                if df is not None:
                    # Keep extracted dataframe for later aggregation phase
                    processed_files.append((source_name, df))
            except Exception as e:
                logger.warning(f"Failed to process {source_name}: {e}")
                continue

        if not processed_files:
            return pl.DataFrame()

        # Step 2: Aggregate each non-dimension source by NSC_CODE(+date) as specified
        aggregated = self._aggregate_sources(processed_files)

        # Diagnostics: NSC_CODE coverage per source (env-controlled)
        if self._diag_enabled():
            try:
                self._log_nsc_coverage(aggregated, None)
            except Exception as e:
                logger.debug(f"NSC coverage pre-merge diag failed: {e}")

        # Step 3: Stream merge aggregated sources; ensure account_base last
        merged = self._stream_merge_data_sources(aggregated)

        # Diagnostics: NSC_CODE coverage after merge vs account_base (env-controlled)
        if self._diag_enabled():
            try:
                self._log_nsc_coverage(aggregated, merged)
            except Exception as e:
                logger.debug(f"NSC coverage post-merge diag failed: {e}")

        # Step 4: Finalize wide table (month/day, period tags, effective days, fill nulls)
        finalized = self._finalize_wide_table(merged)
        return finalized

    def _process_single_source(
        self, source_name: str, file_path: str
    ) -> Optional[pl.DataFrame]:
        """Process a single data source."""
        # 参数健壮性：路径必须是字符串；若不是则尽力转换为字符串并告警
        if not isinstance(file_path, str):
            logger.warning(
                f"Path for {source_name} is {type(file_path).__name__}; coercing to string."
            )
            file_path = str(file_path)
        # Get appropriate transform first, so we can optimize reading (e.g., CSV column pruning)
        transform = self._get_transform_for_source(source_name)

        # Special handling: message Excel wants merge-all-sheets with a '日期' column = sheet name
        name_norm = (source_name or "").lower()
        is_excel = file_path.lower().endswith((".xlsx", ".xls"))
        sheets_used: list[str] = []
        # Heuristic: some providers append suffixes after .xlsx (e.g., .xlsx~tplv...)
        # Try opening as Excel if spending and extension check failed
        if (not is_excel) and ("spending" in name_norm or "ad" in name_norm):
            try:
                import pandas as pd
                pd.ExcelFile(file_path, engine="openpyxl")
                is_excel = True
            except Exception:
                is_excel = False
        if transform and transform.__class__.__name__.lower().startswith("message") and is_excel:
            import pandas as pd
            # read all sheets
            sheets = pd.read_excel(file_path, sheet_name=None, engine="openpyxl")
            frames = []
            for sheet_name, pdf in sheets.items():
                sheets_used.append(str(sheet_name))
                # 按需求：若无'日期'，新增一列 '日期' = sheet 名称
                pdf = pdf.copy()
                if "日期" not in pdf.columns:
                    pdf["日期"] = str(sheet_name)
                frames.append(pdf)
            if not frames:
                return pl.DataFrame()
            df = pl.from_pandas(pd.concat(frames, ignore_index=True))
        # Special handling: spending Excel may have multiple specific sheets to merge
        elif transform and transform.__class__.__name__.lower().startswith("spending") and is_excel:
            sheet_names_raw = None
            if isinstance(self.config, dict):
                sheet_names_raw = self.config.get("spending_sheet_names")
            if sheet_names_raw:
                import pandas as pd

                # Normalize input into a list of tokens (strings or ints)
                if isinstance(sheet_names_raw, list):
                    tokens = sheet_names_raw
                else:
                    tokens = [s.strip() for s in str(sheet_names_raw).replace("，", ",").split(",") if s.strip()]

                def _norm_name(s: str) -> str:
                    return (
                        (s or "").strip().lower()
                        .replace(" ", "")
                        .replace("（", "(")
                        .replace("）", ")")
                    )

                frames = []
                xls = pd.ExcelFile(file_path, engine="openpyxl")
                norm_map = {_norm_name(name): name for name in xls.sheet_names}

                for tok in tokens:
                    try:
                        # numeric index support
                        if isinstance(tok, int) or (isinstance(tok, str) and tok.isdigit()):
                            idx = int(tok)
                            if 0 <= idx < len(xls.sheet_names):
                                frames.append(pd.read_excel(xls, sheet_name=idx, engine="openpyxl"))
                                sheets_used.append(xls.sheet_names[idx])
                            continue

                        tnorm = _norm_name(str(tok))
                        # direct normalized equality
                        if tnorm in norm_map:
                            original = norm_map[tnorm]
                            frames.append(pd.read_excel(xls, sheet_name=original, engine="openpyxl"))
                            sheets_used.append(original)
                            continue

                        # substring fuzzy match as last resort
                        matched = None
                        for sn_norm, original in norm_map.items():
                            if tnorm in sn_norm:
                                matched = original
                                break
                        if matched:
                            frames.append(pd.read_excel(xls, sheet_name=matched, engine="openpyxl"))
                            sheets_used.append(matched)
                    except Exception as e:
                        # Swallow and continue to next token
                        continue

                if not frames:
                    # fallback: defer to generic reader path
                    df = None
                else:
                    import pandas as pd  # ensure in scope
                    pdf = pd.concat(frames, ignore_index=True)
                    # Reduce to required columns to avoid mixed-object issues on convert
                    required_cols = ["NSC CODE", "Date", "Spending(Net)"]
                    present = [c for c in required_cols if c in pdf.columns]
                    if present:
                        pdf = pdf[present]
                    df = pl.from_pandas(pdf)
            else:
                # Fallback to generic reader below
                df = None
        else:
            df = None

        if df is None:
            # Generic reading path (optimize CSV for large files by selecting only needed columns)
            reader_class = self.reader_registry.auto_detect_reader(file_path)
            if not reader_class:
                raise ValueError(f"No reader found for {file_path}")
            reader = reader_class()

            read_kwargs: dict[str, Any] = {}
            try:
                if file_path.lower().endswith(".csv") and getattr(transform, "mapping", None):
                    desired_source_fields = list(transform.mapping.keys())
                    subset_cols = self._infer_csv_subset_columns(file_path, desired_source_fields)
                    if subset_cols:
                        read_kwargs["columns"] = subset_cols
            except Exception as e:
                logger.warning(f"CSV subset inference failed for {source_name}: {e}. Reading full file.")

            df = reader.read(file_path, **read_kwargs)
            # default first sheet marker for excel when not explicitly handled
            if is_excel and not sheets_used:
                sheets_used = ["0"]

            # 对 message 的 CSV 与 Excel 保持一致：若无 '日期'，用文件名填充
            if transform and transform.__class__.__name__.lower().startswith("message"):
                if ("date" not in df.columns) and ("日期" not in df.columns):
                    from pathlib import Path
                    fname = Path(file_path).stem
                    df = df.with_columns(pl.lit(fname).alias("日期"))
        if transform:
            df = transform.transform(df)

        # Unified concise per-file info
        try:
            sheets_info = f"[{', '.join(sheets_used)}]" if sheets_used else "-"
            logger.info(
                f"Processed {source_name}: rows={df.shape[0]}, cols={df.shape[1]}, sheets={sheets_info}, columns={df.columns}"
            )
        except Exception:
            logger.info(
                f"Processed {source_name}: rows={df.shape[0]}, cols={df.shape[1]}, sheets=-"
            )

        return df

    def _infer_csv_subset_columns(self, path: str, desired_fields: list[str]) -> list[str]:
        """Read CSV header only and infer a minimal subset of columns to load.

        Uses the same fuzzy rule as transforms.utils._field_match to map desired source
        fields to actual header names, avoiding full-file load for wide CSVs.
        """
        import csv
        from .transforms.utils import _field_match

        # Read a small sample to sniff dialect
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            sample = f.read(8192)
            f.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample)
            except Exception:
                dialect = csv.excel
            reader = csv.reader(f, dialect)
            header = next(reader)

        actual_cols = list(header)
        subset: list[str] = []
        for want in desired_fields:
            for col in actual_cols:
                if _field_match(want, col):
                    subset.append(col)
                    break

        # Deduplicate while preserving order
        seen = set()
        subset_unique = [c for c in subset if not (c in seen or seen.add(c))]
        return subset_unique

    def _diag_enabled(self) -> bool:
        val = os.getenv("PROCESSOR_DIAG", "1").strip().lower()
        return val in {"1", "true", "yes", "on"}

    def _log_nsc_coverage(
        self,
        aggregated: List[tuple[str, pl.DataFrame]],
        merged: Optional[pl.DataFrame],
    ) -> None:
        """Log NSC_CODE coverage counts per source and after merge.

        Prints only counts, never values.
        """
        def uniq_nsc(df: pl.DataFrame) -> int:
            return (
                df.select("NSC_CODE").unique().height
                if (df is not None and "NSC_CODE" in df.columns and df.height > 0)
                else 0
            )

        parts = []
        ab_df: Optional[pl.DataFrame] = None
        for name, df in aggregated:
            cnt = uniq_nsc(df)
            parts.append(f"{name}:{cnt}")
            if "account_base" in (name or "").lower():
                ab_df = df

        logger.info(f"NSC_CODE coverage per source (aggregated): {', '.join(parts)}")
        if merged is not None:
            merged_cnt = uniq_nsc(merged)
            msg = f"NSC_CODE merged coverage: {merged_cnt}"
            if ab_df is not None and "NSC_CODE" in ab_df.columns:
                try:
                    ab_set = set(ab_df.select("NSC_CODE").unique().to_series().to_list())
                    merged_set = set(merged.select("NSC_CODE").unique().to_series().to_list())
                    inter = len(merged_set & ab_set)
                    missing_in_ab = len(merged_set - ab_set)
                    msg += f", account_base: {len(ab_set)}, intersect: {inter}, missing_in_account_base: {missing_in_ab}"
                except Exception:
                    pass
            logger.info(msg)

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

        # High-priority disambiguation to avoid false positives (e.g., 'ad' in 'leads')
        if "leads" in name:
            return self.TRANSFORM_MAP.get("lead")()
        # For spending/ad, avoid matching 'ad' as a substring of 'lead(s)'
        if ("spending" in name) or (" ad" in name) or name.endswith("_ad") or name == "ad":
            return self.TRANSFORM_MAP.get("ad")()

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
        # Ensure account_base is merged at the very end (by NSC_CODE only)
        def is_account_base(name: str) -> bool:
            n = (name or "").lower()
            return "account_base" in n or n == "accountbase" or n.endswith("_base")

        ordered = [item for item in processed_files if not is_account_base(item[0])] + [
            item for item in processed_files if is_account_base(item[0])
        ]

        result_df = ordered[0][1]

        for source_name, df in ordered[1:]:
            try:
                # Use how='left' to preserve all rows from first file
                result_df = self._safe_join(result_df, df, source_name)
                logger.info(f"Merged {source_name}: result shape {result_df.shape}")
            except Exception as e:
                logger.warning(f"Failed to merge {source_name}: {e}")
                # Continue with partial result rather than failing completely
                continue
                
        return result_df

    def _finalize_wide_table(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add month/day fields, T/T-1 tagging, effective day counts, and fill nulls.

        - period tagging: largest month -> 'T', second largest -> 'T-1'. Others left empty.
        - effective days computed per NSC_CODE within T and T-1 months as (max(day)-min(day)+1).
        - numeric nulls filled with 0 to satisfy "merge missing default 0" requirement.
        """
        if df.is_empty() or ("date" not in df.columns):
            # Still fill numeric nulls for consistency
            return self._fill_numeric_nulls(df)

        # month/day columns
        df = df.with_columns(
            pl.col("date").dt.month().alias("month"),
            pl.col("date").dt.day().alias("day"),
        )

        # Determine T and T-1 months globally
        months = (
            df.select(pl.col("month").drop_nans().drop_nulls().cast(pl.Int64))
            .unique()
            .sort("month")
            .to_series()
            .to_list()
        )
        t_month = max(months) if months else None
        t_1_month = sorted(months)[-2] if months and len(months) >= 2 else None

        # Tag period
        if t_month is not None:
            df = df.with_columns(
                pl.when(pl.col("month") == pl.lit(t_month))
                .then(pl.lit("T"))
                .when((t_1_month is not None) & (pl.col("month") == pl.lit(t_1_month)))
                .then(pl.lit("T-1"))
                .otherwise(pl.lit(None))
                .alias("period")
            )

        # Compute effective days per NSC_CODE for T and T-1
        eff_t = None
        eff_t1 = None
        if t_month is not None:
            eff_t = (
                df.filter(pl.col("month") == pl.lit(t_month))
                .group_by(["NSC_CODE"])
                .agg((pl.max("day") - pl.min("day") + 1).alias("T_effective_days"))
            )
        if t_1_month is not None:
            eff_t1 = (
                df.filter(pl.col("month") == pl.lit(t_1_month))
                .group_by(["NSC_CODE"])
                .agg((pl.max("day") - pl.min("day") + 1).alias("T_minus_1_effective_days"))
            )

        # Attach effective days (left joins by NSC_CODE)
        if eff_t is not None:
            df = (
                df.lazy()
                .join(eff_t.lazy(), on=["NSC_CODE"], how="left")
                .collect(streaming=True)
            )
        if eff_t1 is not None:
            df = (
                df.lazy()
                .join(eff_t1.lazy(), on=["NSC_CODE"], how="left")
                .collect(streaming=True)
            )

        # Fill numeric nulls with 0
        df = self._fill_numeric_nulls(df)
        return df

    def _fill_numeric_nulls(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df
        updates = []
        for name in df.columns:
            dtype = df.schema[name]
            if dtype in (pl.Float32, pl.Float64):
                updates.append(pl.col(name).fill_null(0.0).alias(name))
            elif dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
                updates.append(pl.col(name).fill_null(0).alias(name))
        if updates:
            df = df.with_columns(updates)
        return df

    def _aggregate_sources(self, extracted: List[tuple[str, pl.DataFrame]]) -> List[tuple[str, pl.DataFrame]]:
        """Aggregate each source as required:

        - For all sources except account_base: group by NSC_CODE+date (if date exists),
          sum numeric columns defined by Transform.sum_columns. If no sum columns,
          reduce to unique keys only.
        - For account_base: keep as-is (dimension table).
        """
        aggregated: list[tuple[str, pl.DataFrame]] = []
        for source_name, df in extracted:
            name = (source_name or "").lower()
            if "account_base" in name:
                aggregated.append((source_name, df))
                continue

            transform = self._get_transform_for_source(source_name)
            sum_cols = []
            if transform and hasattr(transform, "sum_columns"):
                sum_cols = [c for c in transform.sum_columns if c in df.columns]

            # determine keys
            if "date" not in df.columns:
                logger.warning(
                    f"Skip aggregating '{source_name}': missing 'date' column; "
                    "to avoid multi-date duplication in later joins."
                )
                # Strictly skip non-dimension sources without date to prevent row explosion
                continue

            keys = ["NSC_CODE", "date"]

            if sum_cols:
                agg_exprs = [pl.col(c).sum().alias(c) for c in sum_cols]
                grouped = df.group_by(keys).agg(agg_exprs)
                aggregated.append((source_name, grouped))
            else:
                # no numeric to sum -> return only the key columns (deduplicated)
                if keys:
                    key_df = df.select([c for c in keys if c in df.columns]).unique()
                    aggregated.append((source_name, key_df))
                else:
                    aggregated.append((source_name, pl.DataFrame()))

        return aggregated

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
