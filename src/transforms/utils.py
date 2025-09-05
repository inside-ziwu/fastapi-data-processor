"""Common utilities for data transformation modules."""

import polars as pl
from typing import Dict, List, Any, Optional


def rename_columns(df: pl.DataFrame, mapping: Dict[str, str]) -> pl.DataFrame:
    """Rename columns based on mapping with fuzzy matching.

    - Uses provided mapping as the single source of truth.
    - If NSC_CODE is still missing after the first pass, try a conservative
      fallback matching over common NSC synonyms to avoid brittle failures.
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

    # If NSC_CODE still missing, attempt a conservative fallback
    if "NSC_CODE" not in df.columns:
        nsc_candidates = []
        for c in df.columns:
            c_norm = c.replace(" ", "").lower()
            if any(
                token in c_norm
                for token in (
                    "nsc", "经销商id", "主机厂经销商id", "经销商id列表", "主机厂经销商id列表"
                )
            ):
                nsc_candidates.append(c)

        if nsc_candidates:
            # Pick the longest match (most specific) deterministically
            best = sorted(nsc_candidates, key=lambda x: len(x), reverse=True)[0]
            df = df.rename({best: "NSC_CODE"})

    # Validate NSC_CODE exists after renaming
    if "NSC_CODE" not in df.columns:
        raise ValueError(
            f"NSC_CODE column not found after renaming. Available: {df.columns}"
        )

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

    if "date" not in df.columns:
        raise ValueError("No date column found")

    # Convert to date format only if it's a string
    if df["date"].dtype == pl.Utf8:
        df = df.with_columns(
            pl.col("date")
            .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
            .alias("date")
        )

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

    if "date" in df.columns:
        if df["date"].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("date")
            )

    return df


def cast_numeric_columns(df: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
    """Cast specified columns to numeric types robustly.

    - Removes commas and percent signs.
    - Treats placeholders like "", "-", "—", "N/A", "NA", "null", "None" as nulls.
    - Uses non-strict casting to coerce remaining strings to Float64.
    """
    for col in columns:
        if col in df.columns:
            clean = (
                pl.col(col)
                .cast(pl.Utf8)
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
