"""Main data processing pipeline."""

import logging
from typing import Dict, List, Optional, Any
import polars as pl
from pathlib import Path
import os
import re
import unicodedata

from .readers import ReaderRegistry, registry as reader_registry
from .transforms import BaseTransformer
from .analysis import create_default_analysis_engine
from .config import FIELD_MAPPINGS
from .cleaning.key_sanitizer import sanitize_key
from .transforms import MessageTransform

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

        # Structural fix: merge DR sources before aggregation (explicit config keys)
        dr_keys: List[str] = self.config.get("dr_sources", ["DR1_file", "DR2_file"]) if isinstance(self.config, dict) else ["DR1_file", "DR2_file"]
        dr_items = [(name, df) for name, df in processed_files if name in dr_keys]
        other_items = [(name, df) for name, df in processed_files if name not in dr_keys]
        if dr_items:
            try:
                dr_df = pl.concat([df for _, df in dr_items], how="vertical")
                logger.info(
                    f"Combined DR sources: {', '.join(name for name, _ in dr_items)} -> rows={dr_df.shape[0]}, cols={dr_df.shape[1]}"
                )
                processed_files = other_items + [("dr", dr_df)]
            except Exception as e:
                logger.warning(f"Failed to combine DR sources: {e}. Proceeding without combination.")

        # Step 2: Aggregate each non-dimension source by NSC_CODE(+date) as specified
        aggregated = self._aggregate_sources(processed_files)

        # Step 3: Stream merge aggregated sources; ensure account_base last
        merged = self._stream_merge_data_sources(aggregated)

        # Step 4: Finalize wide table (month/day, period tags, effective days, fill nulls)
        finalized = self._finalize_wide_table(merged)
        if "level" in finalized.columns:
            finalized = finalized.rename({"level": "层级"})
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
        is_excel = file_path.lower().endswith((".xlsx", ".xls", ".xlsm"))
        sheets_used: list[str] = []
        # Heuristic: providers may append suffixes after .xlsx or hide real type.
        # Try opening as Excel if extension check failed for message/spending/ad/account*.
        if (not is_excel) and (
            ("spending" in name_norm) or (" ad" in name_norm) or name_norm.endswith("_ad") or name_norm == "ad"
            or ("message" in name_norm) or ("msg" in name_norm)
            or ("account" in name_norm)
        ):
            try:
                import pandas as pd
                pd.ExcelFile(file_path, engine="openpyxl")
                is_excel = True
            except Exception:
                is_excel = False
        # Message-specific handling
        if transform and transform.__class__.__name__.lower().startswith("message") and not is_excel:
            try:
                logger.info(f"[message] Excel 识别: False (fallback to generic reader), file={file_path}")
            except Exception:
                pass

        if transform and transform.__class__.__name__.lower().startswith("message") and is_excel:
            try:
                logger.info(f"[message] Excel 识别: True, file={file_path}")
            except Exception:
                pass
            import pandas as pd
            # read all sheets
            sheets = pd.read_excel(file_path, sheet_name=None, engine="openpyxl")
            frames = []
            # 对照日志：记录 sheet 名与解析到的日期
            sheet_date_pairs: list[tuple[str, str]] = []
            for sheet_name, pdf in sheets.items():
                sheets_used.append(str(sheet_name))
                # 从 sheet 名推断日期：始终写入（覆盖） 'date'（ISO），失败时写入'日期'文本并告警
                pdf = pdf.copy()
                sheet_str = str(sheet_name or "").strip()
                s = unicodedata.normalize("NFKC", sheet_str)
                s = s.replace("年", "-").replace("月", "-").replace("日", "")
                s = s.replace("/", "-").replace(".", "-")
                s = re.sub(r"\s+", "", s)
                iso_val: Optional[str] = None
                # 优先在 sheet 名中检索完整日期（到日）
                m = re.search(r"(20\d{2})-(\d{1,2})-(\d{1,2})", s)
                if m:
                    y, mo, da = m.group(1), int(m.group(2)), int(m.group(3))
                    iso_val = f"{y}-{mo:02d}-{da:02d}"
                elif re.search(r"^\d{8}$", s):
                    iso_val = f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
                else:
                    m = re.search(r"(20\d{2})(\d{2})(\d{2})", s)
                    if m:
                        y, mo, da = m.group(1), int(m.group(2)), int(m.group(3))
                        iso_val = f"{y}-{mo:02d}-{da:02d}"
                    else:
                        # 仅有“年-月”，补01
                        m = re.search(r"(20\d{2})-(\d{1,2})", s)
                        if m:
                            y, mo = m.group(1), int(m.group(2))
                            iso_val = f"{y}-{mo:02d}-01"
                        else:
                            # 仅“年”，补01-01
                            m = re.search(r"(20\d{2})", s)
                            if m:
                                y = m.group(1)
                                iso_val = f"{y}-01-01"
                if iso_val:
                    # 写入为标准 ISO 字符串，后续在 transform 中统一解析为 pl.Date
                    pdf["日期"] = iso_val
                    sheet_date_pairs.append((sheet_str, iso_val))
                    try:
                        logger.info(f"[message] Sheet '{sheet_str}' → 日期 {iso_val}")
                    except Exception:
                        pass
                else:
                    # 严格模式：sheet 必须包含可解析日期
                    raise ValueError(f"[message] 无法从 sheet 名解析日期: '{sheet_str}' in file: {file_path}")
                frames.append(pdf)
            if not frames:
                return pl.DataFrame()
            # 合并并清洗 pandas 对象列，避免 bytes/int 混杂触发 Arrow 转换异常
            pdf = pd.concat(frames, ignore_index=True)
            try:
                obj_cols = [c for c in pdf.columns if str(pdf[c].dtype) == "object"]
                if obj_cols:
                    for c in obj_cols:
                        pdf[c] = pdf[c].apply(
                            lambda v: (
                                v.decode("utf-8", "ignore") if isinstance(v, (bytes, bytearray)) else (None if v is None else str(v))
                            )
                        )
            except Exception:
                pass
            df = pl.from_pandas(pdf)
            # Explicitly cast columns to their expected types
            df = df.with_columns([
                pl.col("日期").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("日期"),
                pl.col("进入私信客户数").cast(pl.Float64, strict=False).fill_null(0.0).alias("进入私信客户数"),
                pl.col("主动咨询客户数").cast(pl.Float64, strict=False).fill_null(0.0).alias("主动咨询客户数"),
                pl.col("私信留资客户数").cast(pl.Float64, strict=False).fill_null(0.0).alias("私信留资客户数"),
            ])
            logger.info(f"Message file raw DataFrame schema (before MessageTransform): {df.schema}")
            logger.info(f"Message file raw DataFrame head (5 rows, before MessageTransform):\n{df.head(5)}")
            # 打印对照日志
            try:
                if sheet_date_pairs:
                    pairs_str = ", ".join([f"'{n}'→{d}" for n, d in sheet_date_pairs])
                    logger.info(f"[message] Sheet 日期映射: {pairs_str}")
            except Exception:
                pass
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
                    if "Spending(Net)" in pdf.columns:
                        df = df.with_columns(pl.col("Spending(Net)").cast(pl.Float64).fill_nan(0.0).alias("Spending(Net)"))
            else:
                # Fallback to generic reader below
                df = None
        # Special handling: account_base Excel with two sheets (level + store)
        elif transform and transform.__class__.__name__.lower().startswith("accountbase") and is_excel:
            import pandas as pd
            # Read all sheets and pick columns via mapping per sheet
            xls = pd.ExcelFile(file_path, engine="openpyxl")
            # Strict header mapping definitions (no fuzzy)
            import unicodedata as _ud
            def _std(s: str) -> str:
                return _ud.normalize("NFKC", str(s or "")).strip().lower()
            NSC_ALIASES = {
                _std(x)
                for x in [
                    "NSC CODE", "NSC Code", "NSC_id", "NSC Code", "reg_dealer",
                    "主机厂经销商id列表", "主机厂经销商ID", "主机厂经销商id",
                ]
            }
            LEVEL_ALIASES = {_std("第二期层级")}
            STORE_ALIASES = {_std("抖音id"), _std("抖音ID")}
            level_frames: list[pl.DataFrame] = []
            store_frames: list[pl.DataFrame] = []
            for sheet_name in xls.sheet_names:
                sheets_used.append(str(sheet_name))
                pdf = pd.read_excel(xls, sheet_name=sheet_name, engine="openpyxl")
                # Ensure all column headers are strings to avoid Polars conversion errors
                try:
                    cols = [str(c) for c in pdf.columns]
                    # Ensure unique column names
                    seen: dict[str, int] = {}
                    uniq: list[str] = []
                    for c in cols:
                        if c in seen:
                            seen[c] += 1
                            uniq.append(f"{c}_{seen[c]}")
                        else:
                            seen[c] = 0
                            uniq.append(c)
                    pdf.columns = uniq
                except Exception:
                    pass
                # Pre-coerce ALL columns to strings to avoid mixed-type issues
                try:
                    def _to_str(v):
                        try:
                            import math
                            import numpy as _np
                            if v is None:
                                return None
                            # pandas NA/NaN
                            if isinstance(v, float) and math.isnan(v):
                                return None
                            if hasattr(v, "__array__") and _np.isnan(v).item() is True:  # type: ignore[attr-defined]
                                return None
                            if isinstance(v, (bytes, bytearray)):
                                return v.decode("utf-8", "ignore")
                            return str(v)
                        except Exception:
                            return str(v) if v is not None else None
                    for _c in list(pdf.columns):
                        pdf[_c] = pdf[_c].apply(_to_str)
                except Exception:
                    pass
                # Robust conversion to polars
                try:
                    pldf = pl.from_pandas(pdf)
                except Exception:
                    try:
                        import pandas as _pd
                        data = {}
                        for k in pdf.columns:
                            series = pdf[k]
                            vals = []
                            for v in list(series.values):
                                if v is None or (isinstance(v, float) and v != v):
                                    vals.append(None)
                                elif isinstance(v, (bytes, bytearray)):
                                    vals.append(v.decode("utf-8", "ignore"))
                                else:
                                    vals.append(str(v))
                            data[str(k)] = vals
                        pldf = pl.DataFrame(data)
                    except Exception as e:
                        raise e
                # Strict rename to NSC_CODE/level/store_name before selection
                try:
                    rename_map = {}
                    found = {"NSC_CODE": None, "level": None, "store_name": None}
                    for c in pldf.columns:
                        key = _std(c)
                        if key in NSC_ALIASES:
                            rename_map[c] = "NSC_CODE"
                            found["NSC_CODE"] = c
                        elif key in LEVEL_ALIASES:
                            rename_map[c] = "level"
                            found["level"] = c
                        elif key in STORE_ALIASES:
                            rename_map[c] = "store_name"
                            found["store_name"] = c
                    if rename_map:
                        pldf = pldf.rename(rename_map)
                    try:
                        logger.info(
                            f"[account_base] sheet '{sheet_name}' headers -> NSC_CODE:{found['NSC_CODE']}, level:{found['level']}, store_name:{found['store_name']}"
                        )
                    except Exception:
                        pass
                except Exception:
                    pass
                cols = set(pldf.columns)
                if {"NSC_CODE", "level"}.issubset(cols):
                    level_frames.append(pldf.select(["NSC_CODE", "level"]))
                if {"NSC_CODE", "store_name"}.issubset(cols):
                    store_frames.append(pldf.select(["NSC_CODE", "store_name"]))

            level_df = None
            if level_frames:
                level_df = pl.concat(level_frames, how="vertical").with_columns(
                    pl.col("NSC_CODE").cast(pl.Utf8),
                    pl.col("level").cast(pl.Utf8),
                )
                # Prefer first non-null level per NSC_CODE
                level_df = (
                    level_df.group_by("NSC_CODE").agg(pl.col("level").drop_nulls().first().alias("level"))
                )

            store_df = None
            if store_frames:
                store_df = pl.concat(store_frames, how="vertical").with_columns(
                    pl.col("NSC_CODE").cast(pl.Utf8),
                    pl.col("store_name").cast(pl.Utf8),
                )
                # Prefer first non-null store_name per NSC_CODE
                store_df = (
                    store_df.group_by("NSC_CODE").agg(pl.col("store_name").drop_nulls().first().alias("store_name"))
                )

            if level_df is not None and store_df is not None:
                # Ensure Utf8 types before join
                try:
                    level_df = level_df.with_columns(pl.col("NSC_CODE").cast(pl.Utf8, strict=False))
                    store_df = store_df.with_columns([
                        pl.col("NSC_CODE").cast(pl.Utf8, strict=False),
                        pl.col("store_name").cast(pl.Utf8, strict=False),
                    ])
                except Exception:
                    pass
                df = level_df.join(store_df, on="NSC_CODE", how="outer")
                try:
                    ln = int(level_df.height)
                    sn = int(store_df.height)
                    nn = int(df.select(pl.col("store_name").is_not_null().sum()).to_series(0)[0]) if "store_name" in df.columns else 0
                    logger.info(f"[account_base] merged level rows={ln}, store rows={sn}, store_name_non_null={nn}")
                except Exception:
                    pass
            elif level_df is not None:
                df = level_df
            elif store_df is not None:
                df = store_df
            else:
                df = pl.DataFrame()
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

            if is_excel and transform and transform.__class__.__name__.lower().startswith("spending"):
                # For spending Excel, explicitly read only mapped columns to avoid type inference issues with unneeded columns
                from src.config.source_mappings import SPENDING_MAP
                read_kwargs["usecols"] = list(SPENDING_MAP.keys())
                logger.info(f"Reading Spending Excel with explicit columns: {read_kwargs["usecols"]}")
            df = reader.read(file_path, **read_kwargs)
            # default first sheet marker for excel when not explicitly handled
            if is_excel and not sheets_used:
                sheets_used = ["0"]

            # 移除消息 CSV 的文件名兜底日期逻辑——严格依赖数据内提供的日期列
        if transform:
            df = transform.transform(df)

        # Sanitize the primary key after transformation
        if "NSC_CODE" in df.columns:
            # Enable unicode normalization to handle full-width characters, accepting the performance cost.
            df = sanitize_key(df, "NSC_CODE", normalize_unicode=True)

            # Collision detection
            collisions = (
                df.filter(pl.col("NSC_CODE").is_not_null())
                .group_by("NSC_CODE")
                .agg(
                    pl.n_unique("NSC_CODE__raw").alias("n_raw"),
                    pl.col("NSC_CODE__raw").alias("collided_raw_values")
                )
                .filter(pl.col("n_raw") > 1)
            )
            if not collisions.is_empty():
                logger.error(
                    f"[{source_name}] Sanitization resulted in key collisions. "
                    f"Aborting. Collisions: {collisions.to_dicts()}"
                )
                raise ValueError(f"Key collisions detected in source: {source_name}")

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

    def _log_level_distribution(self, merged: pl.DataFrame) -> None:
        """Log level distribution by unique NSC_CODE, if level present."""
        if merged is None or merged.is_empty() or ("NSC_CODE" not in merged.columns):
            return
        if "level" not in merged.columns:
            return
        try:
            pairs = (
                merged.select(["NSC_CODE", "level"]).drop_nulls("NSC_CODE").unique()
            )
            if pairs.is_empty():
                return
            dist = pairs.group_by("level").count()
            # Convert to dict for logging
            levels = dist["level"].to_list()
            counts = dist["count"].to_list()
            payload = {str(lv): int(ct) for lv, ct in zip(levels, counts)}
            logger.info(f"Level distribution (by NSC): {payload}")
        except Exception:
            pass

    def _get_transform_for_source(
        self, source_name: str
    ) -> Optional[BaseTransformer]:
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

        # Create the total effective_days column
        effective_days_expr = []
        if "T_effective_days" in df.columns:
            effective_days_expr.append(pl.col("T_effective_days").fill_null(0))
        if "T_minus_1_effective_days" in df.columns:
            effective_days_expr.append(pl.col("T_minus_1_effective_days").fill_null(0))
        
        if effective_days_expr:
            df = df.with_columns(
                pl.sum_horizontal(effective_days_expr).alias("effective_days")
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

        # suffix masking diagnostics removed from core

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
