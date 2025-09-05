"""Common utilities for data transformation modules."""

import polars as pl
from typing import Dict, List, Any, Optional


def rename_columns(df: pl.DataFrame, mapping: Dict[str, str]) -> pl.DataFrame:
    """Rename columns based on mapping with fuzzy matching.

    保持“笨”且可预测：仅按提供的映射做模糊重命名；不做任何业务耦合的校验。
    关键列的存在性应由调用该函数的 Transform 自行保证。
    """
    rename_map = {}
    for src_col, expected_col in mapping.items():
        # Find matching column in actual data
        for actual_col in df.columns:
            if _field_match(src_col, actual_col):
                rename_map[actual_col] = expected_col
                break

    if rename_map:
        df = df.rename(rename_map)

    return df


def _field_match(src: str, col: str) -> bool:
    """Check if column matches source field name."""
    # Normalize strings for comparison
    src_norm = src.replace(" ", "").lower()
    col_norm = col.replace(" ", "").lower()

    # Check for exact match or substring match
    return (
        src_norm == col_norm
        or src_norm in col_norm
        or col_norm in src_norm
    )


def normalize_nsc_code(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize NSC_CODE column - handle multiple codes, clean formatting."""
    if "NSC_CODE" not in df.columns:
        return df

    # Cast to string
    df = df.with_columns(pl.col("NSC_CODE").cast(pl.Utf8))

    # Handle multiple NSC codes in single cell (comma-separated)
    df = df.with_columns(
        pl.when(pl.col("NSC_CODE").str.contains(r"[,|]"))
        .then(pl.col("NSC_CODE").str.split(","))
        .otherwise(pl.col("NSC_CODE").str.split(""))
        .alias("_nsc_list")
    )

    # Explode multiple NSC codes into separate rows
    df = df.explode("_nsc_list")

    # Clean up NSC code
    df = df.with_columns(
        pl.col("_nsc_list").str.strip_chars().alias("NSC_CODE")
    ).drop("_nsc_list")

    # Filter out empty NSC codes
    df = df.filter(
        pl.col("NSC_CODE").is_not_null() & (pl.col("NSC_CODE") != "")
    )

    if df.height == 0:
        raise ValueError(
            "No valid NSC_CODE values found after normalization"
        )

    return df


def ensure_date_column(df: pl.DataFrame, date_candidates: Optional[List[str]] = None) -> pl.DataFrame:
    """Ensure date column exists and is properly formatted."""
    if date_candidates is None:
        date_candidates = ["日期", "date", "time", "开播日期", "直播日期", "日期时间"]

    if "date" not in df.columns:
        # Try to find date column with different names
        for candidate in date_candidates:
            if candidate in df.columns:
                df = df.rename({candidate: "date"})
                break
        # Fallback: normalized name match (handles zero-width/nbspace/fullwidth)
        if "date" not in df.columns:
            import re
            def _norm(s: str) -> str:
                s = re.sub(r"[\s\u200b\u200c\u200d\ufeff\u00a0]+", "", s or "")
                s = s.replace("（", "(").replace("）", ")").replace("：", ":")
                s = re.sub(r"[^\w\u4e00-\u9fa5:]+", "", s)
                return s.lower()
            norm_map = {_norm(c): c for c in df.columns}
            for candidate in date_candidates:
                key = _norm(candidate)
                if key in norm_map:
                    df = df.rename({norm_map[key]: "date"})
                    break

    if "date" not in df.columns:
        raise ValueError("No date column found")

    # Normalize dtype: always end up with pl.Date for joins to match
    dt = df["date"].dtype
    if dt == pl.Utf8:
        # common formats: YYYY-MM-DD / YYYY/MM/DD / YYYY.MM.DD
        df = df.with_columns(
            pl.col("date")
            .str.replace_all("/", "-")
            .str.replace_all(r"\.", "-")
            .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
            .alias("date")
        )
    elif dt == pl.Datetime:
        df = df.with_columns(pl.col("date").cast(pl.Date).alias("date"))

    return df


def ensure_optional_date_column(
    df: pl.DataFrame, date_candidates: Optional[List[str]] = None
) -> pl.DataFrame:
    """Ensure date column if present; do not raise when missing.

    - Renames first matching candidate to 'date'.
    - Parses to pl.Date if column is Utf8; otherwise leaves as-is.
    - If no candidate found, returns df unchanged.
    """
    if date_candidates is None:
        date_candidates = ["日期", "date", "time", "开播日期", "直播日期", "日期时间"]

    if "date" not in df.columns:
        for candidate in date_candidates:
            if candidate in df.columns:
                df = df.rename({candidate: "date"})
                break
        if "date" not in df.columns:
            import re
            def _norm(s: str) -> str:
                s = re.sub(r"[\s\u200b\u200c\u200d\ufeff\u00a0]+", "", s or "")
                s = s.replace("（", "(").replace("）", ")").replace("：", ":")
                s = re.sub(r"[^\w\u4e00-\u9fa5:]+", "", s)
                return s.lower()
            norm_map = {_norm(c): c for c in df.columns}
            for candidate in date_candidates:
                key = _norm(candidate)
                if key in norm_map:
                    df = df.rename({norm_map[key]: "date"})
                    break

    if "date" in df.columns:
        dt = df["date"].dtype
        if dt == pl.Utf8:
            df = df.with_columns(
                pl.col("date")
                .str.replace_all("/", "-")
                .str.replace_all(r"\.", "-")
                .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                .alias("date")
            )
        elif dt == pl.Datetime:
            df = df.with_columns(pl.col("date").cast(pl.Date).alias("date"))

    return df


def cast_numeric_columns(df: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
    """Cast specified columns to numeric types robustly.

    - Removes commas and percent signs.
    - Treats placeholders like "", "-", "—", "N/A", "NA", "null", "None" as nulls.
    - Uses non-strict casting to coerce remaining strings to Float64.
    """
    for col in columns:
        if col in df.columns:
            # PROBE: force element-wise to string to handle mixed object columns from pandas
            # This is slower but robust; after validation we can optimize back.
            expr = pl.col(col).map_elements(
                lambda v: (
                    "" if v is None else (
                        (v.decode("utf-8", "ignore") if isinstance(v, (bytes, bytearray)) else (v if isinstance(v, str) else str(v)))
                    )
                ),
                return_dtype=pl.Utf8,
            )

            clean = (
                expr
                .str.replace_all(",", "")
                .str.replace_all("%", "")
                .str.strip_chars()
            )

            df = df.with_columns(
                pl.when(
                    clean.str.to_lowercase().is_in([
                        "", "-", "—", "n/a", "na", "null", "none",
                    ])
                )
                .then(None)
                .otherwise(clean)
                .cast(pl.Float64, strict=False)
                .alias(col)
            )
    return df


def aggregate_data(df: pl.DataFrame, group_cols: List[str], sum_columns: List[str]) -> pl.DataFrame:
    """Group by specified columns and aggregate numeric columns."""
    # Build aggregation expressions
    agg_exprs = []
    for col in sum_columns:
        if col in df.columns:
            agg_exprs.append(pl.col(col).sum().alias(col))

    if agg_exprs:
        df = df.group_by(group_cols).agg(agg_exprs)
    else:
        df = df.unique(subset=group_cols)

    return df
