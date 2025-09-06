"""Common utilities for data transformation modules."""

import polars as pl
from typing import Dict, List, Any, Optional
import re
import unicodedata
from datetime import date, datetime, timedelta


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
    """Check if column matches source field name with robust normalization.

    - Normalizes full-width/half-width characters via NFKC.
    - Removes all whitespace (including zero-width and NBSP).
    - Unifies common punctuation variants and drops non-alnum/non-CJK symbols (keeps content inside brackets).
    - Enables tolerant matches like '小风车点击次数（不含小雪花）' vs '小风车点击次数(不含小雪花)'.
    """
    import re
    import unicodedata

    def _norm(s: str) -> str:
        s = s or ""
        # Normalize to unify full-width and half-width forms
        s = unicodedata.normalize("NFKC", s)
        s = s.lower()
        # Remove all whitespace variants (space, zero-width, NBSP, etc.)
        s = re.sub(r"[\s\u200b\u200c\u200d\ufeff\u00a0]+", "", s)
        # Unify a few common punctuation variants explicitly
        s = s.replace("（", "(").replace("）", ")").replace("：", ":")
        # Drop non-alnum/non-CJK characters (parentheses removed but content preserved)
        s = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", s)
        return s

    src_norm = _norm(src)
    col_norm = _norm(col)

    return (
        src_norm == col_norm
        or (src_norm and src_norm in col_norm)
        or (col_norm and col_norm in src_norm)
    )


def normalize_nsc_code(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize NSC_CODE column - handle multiple codes, clean formatting."""
    if "NSC_CODE" not in df.columns:
        return df

    # Cast to string
    df = df.with_columns(pl.col("NSC_CODE").cast(pl.Utf8))

    # Unify common separators to comma: | ， 、 / ;
    unified = (
        pl.col("NSC_CODE")
        .str.replace_all(r"\|", ",")
        .str.replace_all("，", ",")
        .str.replace_all("、", ",")
        .str.replace_all("/", ",")
        .str.replace_all(";", ",")
    )
    df = df.with_columns(unified.alias("NSC_CODE"))

    # Split by comma into list (single element if no comma)
    df = df.with_columns(pl.col("NSC_CODE").str.split(",").alias("_nsc_list"))

    # Explode multiple NSC codes into separate rows
    df = df.explode("_nsc_list")

    # Clean up NSC code
    df = df.with_columns(
        pl.col("_nsc_list").cast(pl.Utf8).str.strip_chars().alias("NSC_CODE")
    ).drop("_nsc_list")

    # Filter out empty NSC codes
    df = df.filter(
        pl.col("NSC_CODE").is_not_null() & (pl.col("NSC_CODE").cast(pl.Utf8).str.strip_chars() != "")
    )

    if df.height == 0:
        raise ValueError(
            "No valid NSC_CODE values found after normalization"
        )

    return df


def _norm_header_key(s: str) -> str:
    s = re.sub(r"[\u200b\u200c\u200d\ufeff\u00a0]+", "", s or "")
    s = unicodedata.normalize("NFKC", s)
    return (
        s.replace(" ", "").replace("（", "(").replace("）", ")").replace("：", ":").lower()
    )


def _to_date_py(v: Any) -> Optional[date]:
    """Best-effort Python parser for dirty date values (single cell)."""
    if v is None:
        return None
    # Excel serial (rough bounds)
    if isinstance(v, (int, float)):
        iv = int(v)
        if 20000 <= iv <= 80000:
            base = date(1899, 12, 30)
            try:
                return base + timedelta(days=iv)
            except Exception:
                return None
        else:
            return None
    # String-like
    s = str(v)
    s = unicodedata.normalize("NFKC", s).strip()
    # Keep only date part if datetime-like
    if "T" in s:
        s = s.split("T", 1)[0]
    if " " in s:
        s = s.split(" ", 1)[0]
    # Chinese date
    s = s.replace("年", "-").replace("月", "-").replace("日", "")
    # Unify separators
    s = s.replace("/", "-").replace(".", "-")
    # 8-digit compact form
    if re.fullmatch(r"\d{8}", s):
        s = f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    # ISO date or datetime
    try:
        return date.fromisoformat(s)
    except Exception:
        try:
            return datetime.fromisoformat(s).date()
        except Exception:
            return None


def _normalize_and_parse_date_column(
    df: pl.DataFrame, date_candidates: Optional[List[str]], required: bool
) -> pl.DataFrame:
    """Shared implementation for ensuring/normalizing a date column.

    - Renames the first matching candidate to 'date' (header match normalized).
    - Parses to pl.Date via vector ops where possible; falls back to Python for edge cases.
    - If required=True and all values null after parsing, raises.
    """
    date_candidates = date_candidates or [
        "日期",
        "留资日期",
        "date",
        "time",
        "开播日期",
        "直播日期",
        "日期时间",
        "register_time",
        "register date",
    ]

    # Rename to 'date' if not present
    if "date" not in df.columns:
        for candidate in date_candidates:
            if candidate in df.columns:
                df = df.rename({candidate: "date"})
                break
        if "date" not in df.columns:
            norm_map = {_norm_header_key(c): c for c in df.columns}
            for candidate in date_candidates:
                key = _norm_header_key(candidate)
                if key in norm_map:
                    df = df.rename({norm_map[key]: "date"})
                    break

    if "date" not in df.columns:
        if required:
            raise ValueError(f"No date column found. Available: {df.columns}")
        return df

    # Already Date
    if df["date"].dtype == pl.Date:
        return df
    # Cast from Datetime
    if df["date"].dtype == pl.Datetime:
        return df.with_columns(pl.col("date").cast(pl.Date).alias("date"))

    # Vectorized normalization of strings (best effort)
    s = pl.col("date").cast(pl.Utf8)
    norm = (
        s.str.replace_all(r"[\u200b\u200c\u200d\ufeff\u00a0]", "")
        .str.replace_all("年", "-")
        .str.replace_all("月", "-")
        .str.replace_all("日", "")
        .str.replace_all("/", "-")
        # Replace literal dot with dash; r"\." matches a literal '.' in regex
        .str.replace_all(".", "-", literal=True)
        .str.strip_chars()
    )
    # Try multiple parse formats and coalesce
    # - Support zero/one-digit month/day via %-m/%-d (e.g., 2023-10-1)
    d1 = norm.str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    d1b = norm.str.strptime(pl.Date, "%Y-%-m-%-d", strict=False)
    d2 = norm.str.strptime(pl.Date, "%Y%m%d", strict=False)
    # As datetime and cast to date (handles time part)
    dt1 = norm.str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False).cast(pl.Date)
    dt1b = norm.str.strptime(pl.Datetime, "%Y-%-m-%-d %H:%M:%S", strict=False).cast(pl.Date)
    parsed = pl.coalesce([d1, d1b, d2, dt1, dt1b])

    df = df.with_columns(parsed.alias("date"))

    # Fallback to Python for leftover nulls (Excel serials and oddballs)
    if int(df.select(pl.col("date").is_null().sum()).to_series(0)[0]) > 0:
        df = df.with_columns(
            pl.when(pl.col("date").is_null())
            .then(pl.col("date").map_elements(_to_date_py, return_dtype=pl.Date))
            .otherwise(pl.col("date"))
            .alias("date")
        )

    if required and int(df.select(pl.col("date").is_null().sum()).to_series(0)[0]) == df.height:
        raise ValueError("Date parsing failed: all values are null after normalization")

    return df


def ensure_date_column(df: pl.DataFrame, date_candidates: Optional[List[str]] = None) -> pl.DataFrame:
    """Ensure date column exists and is properly formatted (fail-fast)."""
    return _normalize_and_parse_date_column(df, date_candidates, required=True)


def ensure_optional_date_column(
    df: pl.DataFrame, date_candidates: Optional[List[str]] = None
) -> pl.DataFrame:
    """Ensure date column if present; do not raise when missing."""
    return _normalize_and_parse_date_column(df, date_candidates, required=False)


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
