"""Common utilities for data transformation modules."""

import polars as pl
from typing import Dict, List, Any, Optional
import re
import unicodedata
from datetime import date, datetime, timedelta

def standardize_name(name: str) -> str:
    """
    Standardize a column name for reliable matching.
    - Lowercase
    - Normalize full-width/half-width characters
    - Remove all whitespace and special characters
    """
    if not isinstance(name, str):
        return ""
    # Normalize to unify full-width and half-width forms
    s = unicodedata.normalize("NFKC", name)
    s = s.lower()
    # Remove all whitespace variants
    s = re.sub(r"\s+", "", s)
    # Unify common punctuation and remove non-alnum/non-CJK characters
    s = s.replace("（", "(").replace("）", ")").replace("：", ":")
    s = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", s)
    return s

def apply_strict_rename(df: pl.DataFrame, rename_map: Dict[str, str]) -> pl.DataFrame:
    """
    Rename columns based on a strict mapping.
    It finds source columns by a standardized name match and raises if a column is not found.
    """
    actual_rename_map = {}
    df_columns_standardized = {standardize_name(col): col for col in df.columns}
    
    # Track which target columns have been successfully mapped
    mapped_target_columns = set()

    for source_name, target_name in rename_map.items():
        standardized_source = standardize_name(source_name)
        if standardized_source in df_columns_standardized:
            actual_col_name = df_columns_standardized[standardized_source]
            # Only add to rename map if the target name isn't already mapped from another source
            # or if the actual_col_name is different from target_name (avoid self-rename)
            if actual_col_name != target_name and target_name not in mapped_target_columns:
                actual_rename_map[actual_col_name] = target_name
                mapped_target_columns.add(target_name)
        # else: do nothing, just skip this source_name if not found

    df_renamed = df.rename(actual_rename_map)

    # After renaming, check if all *required* target columns are present.
    # For ACCOUNT_BASE_MAP, all values are "NSC_CODE", "level", "store_name".
    # We need to ensure that at least one source for each target is found.
    
    # Collect all unique target names from the rename_map
    required_target_names = set(rename_map.values())

    for target_name in required_target_names:
        if target_name not in df_renamed.columns:
            # Find all source names that map to this target_name
            missing_sources = [
                source for source, target in rename_map.items()
                if target == target_name and standardize_name(source) not in df_columns_standardized
            ]
            raise ValueError(
                f"Required target column '{target_name}' not found in DataFrame "
                f"after attempting renames. Missing source candidates: {missing_sources}. "
                f"Available columns: {df_renamed.columns}"
            )

    return df_renamed


def normalize_nsc_code(df: pl.DataFrame, nsc_column: str) -> pl.DataFrame:
    """
    Normalize a given NSC_CODE column.
    - Handles multiple codes in a single cell, separated by various delimiters.
    - Explodes the DataFrame to have one row per NSC_CODE.
    - Cleans up the code format.
    """
    if nsc_column not in df.columns:
        raise ValueError(f"NSC Code column '{nsc_column}' not found in DataFrame.")

    # Cast to string and unify separators
    df = df.with_columns(
        pl.col(nsc_column).cast(pl.Utf8)
        .str.replace_all(r"[|，、/；]", ",")
        .alias(nsc_column)
    )

    # Split by comma and explode
    df = df.with_columns(pl.col(nsc_column).str.split(",").alias("_nsc_list")).explode("_nsc_list")

    # Clean up and filter empty
    df = df.with_columns(
        pl.col("_nsc_list").str.strip_chars().alias(nsc_column)
    ).drop("_nsc_list")

    df = df.filter(pl.col(nsc_column).is_not_null() & (pl.col(nsc_column) != ""))

    if df.height == 0:
        raise ValueError("No valid NSC_CODE values found after normalization.")

    return df

def normalize_date_column(df: pl.DataFrame, date_column: str, date_candidates: Optional[List[str]] = None) -> pl.DataFrame:
    """
    Ensure a date column exists and is parsed to pl.Date.
    It will search for candidates if the target `date_column` doesn't exist.
    """
    
    search_candidates = date_candidates or [
        "日期", "date", "time", "register_time", "开播日期", "留资日期"
    ]

    if date_column not in df.columns:
        # Find from candidates and rename
        df_columns_standardized = {standardize_name(col): col for col in df.columns}
        found = False
        for candidate in search_candidates:
            std_candidate = standardize_name(candidate)
            if std_candidate in df_columns_standardized:
                actual_col = df_columns_standardized[std_candidate]
                df = df.rename({actual_col: date_column})
                found = True
                break
        if not found:
            raise ValueError(f"Date column '{date_column}' or any of its candidates not found. Available: {df.columns}")

    # Robust parsing logic
    if df[date_column].dtype == pl.Date:
        return df
    if df[date_column].dtype == pl.Datetime:
        return df.with_columns(pl.col(date_column).cast(pl.Date))

    # Try vectorized parsing first
    parsed_date = (
        pl.col(date_column)
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.replace_all("年", "-")
        .str.replace_all("月", "-")
        .str.replace_all("日", "")
        .str.replace_all("/", "-")
        .str.replace_all("\\.", "-", literal=True)
        .str.split(" ")
        .list.first()
    )

    # YYYY-MM-DD, YYYY-M-D, etc.
    iso_date = parsed_date.str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    # YYYYMMDD
    compact_date = parsed_date.str.strptime(pl.Date, "%Y%m%d", strict=False)

    df = df.with_columns(pl.coalesce([iso_date, compact_date]).alias(date_column))
    
    # Fallback for Excel numeric dates or other complex cases
    if df[date_column].is_null().any():
        df = df.with_columns(
            pl.when(pl.col(date_column).is_null())
            .then(pl.col(date_column).map_elements(_to_date_py, return_dtype=pl.Date))
            .otherwise(pl.col(date_column))
            .alias(date_column)
        )

    if df[date_column].is_null().all():
        raise ValueError(f"All values in date column '{date_column}' are null after parsing.")

    return df.with_columns(pl.col(date_column).fill_null(strategy="forward").alias(date_column))


def _to_date_py(v: Any) -> Optional[date]:
    """Best-effort Python parser for dirty date values (single cell)."""
    if v is None:
        return None
    if isinstance(v, (date, datetime)):
        return v.date() if isinstance(v, datetime) else v
    
    # For Excel serial numbers
    if isinstance(v, (int, float)):
        if 20000 < v < 80000: # Heuristic for Excel dates
            try:
                return (datetime(1899, 12, 30) + timedelta(days=v)).date()
            except (ValueError, OverflowError):
                return None
        return None

    s = str(v).strip()
    s = unicodedata.normalize("NFKC", s)
    s = s.split("T")[0].split(" ")[0] # Get date part
    s = s.replace("年", "-").replace("月", "-").replace("日", "").replace("/", "-").replace(".", "-")

    if re.fullmatch(r"\d{8}", s):
        s = f"{s[:4]}-{s[4:6]}-{s[6:]}"
    
    try:
        return date.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def cast_numeric_columns(df: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
    """
    Cast specified columns to numeric types robustly (Float64).
    - Removes commas and percent signs.
    - Treats common placeholders as null.
    """
    for col_name in columns:
        if col_name in df.columns:
            # Ensure it's a string column before applying string operations
            if df[col_name].dtype != pl.Utf8:
                df = df.with_columns(pl.col(col_name).cast(pl.Utf8))

            clean_expr = (
                pl.col(col_name)
                .str.replace_all(",", "")
                .str.replace_all("%", "")
                .str.strip_chars()
            )
            
            # Replace common non-numeric placeholders with null
            null_if_expr = pl.when(
                clean_expr.str.to_lowercase().is_in(["", "-", "—", "n/a", "na", "null", "none"])
            ).then(None).otherwise(clean_expr)

            df = df.with_columns(
                null_if_expr.cast(pl.Float64, strict=False).alias(col_name)
            )
    return df

def aggregate_by_keys(df: pl.DataFrame, group_keys: List[str], metric_columns: List[str]) -> pl.DataFrame:
    """
    Group by specified keys and aggregate metric columns.
    - Sums up all specified metric columns.
    - Fills nulls with 0 in metric columns before summing.
    """
    agg_exprs = []
    for col in metric_columns:
        if col in df.columns:
            agg_exprs.append(pl.col(col).fill_null(0).sum().alias(col))

    if not agg_exprs:
        return df.select(group_keys).unique()

    return df.group_by(group_keys).agg(agg_exprs)
