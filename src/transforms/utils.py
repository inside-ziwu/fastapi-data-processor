"""Common utilities for data transformation modules."""

import os
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


INVALID_NSC_TOKENS = {"", "null", "--"}


def _clean_nsc_expr(expr: pl.Expr) -> pl.Expr:
    """Normalize NSC code strings by casting to text, trimming whitespace, and nulling known sentinels."""
    cleaned = expr.cast(pl.Utf8, strict=False).str.strip_chars()
    lowered = cleaned.str.to_lowercase()
    return pl.when(lowered.is_in(list(INVALID_NSC_TOKENS))).then(None).otherwise(cleaned)


def normalize_nsc_code(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize NSC_CODE column - handle multiple codes, clean formatting."""
    if "NSC_CODE" not in df.columns:
        return df

    # Cast to string
    df = df.with_columns(_clean_nsc_expr(pl.col("NSC_CODE")).alias("NSC_CODE"))
    try:
        import logging

        diag = os.getenv("PROCESSOR_DIAG", "0").strip().lower() in {"1", "true", "yes", "on"}
        if diag:
            logger = logging.getLogger(__name__)
            blank_sample = (
                df.with_columns(pl.col("NSC_CODE").str.strip_chars().alias("_strip"))
                .filter(pl.col("_strip") == "")
                .select("NSC_CODE")
                .unique()
                .head(5)
                .to_series()
                .to_list()
            )
            if blank_sample:
                logger.warning(f"NSC_CODE contains blank entries after normalization: {blank_sample}")
    except Exception:
        pass

    # Unify common separators to comma: | ， 、 / ;
    unified = (
        _clean_nsc_expr(pl.col("NSC_CODE"))
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
    df = df.with_columns(_clean_nsc_expr(pl.col("_nsc_list")).alias("NSC_CODE")).drop("_nsc_list")

    # Filter out empty NSC codes
    df = df.filter(pl.col("NSC_CODE").is_not_null())

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
    # YYYY-MM or YYYY-M -> pad to first day
    m = re.fullmatch(r"(\d{4})-(\d{1,2})", s)
    if m:
        try:
            y = int(m.group(1)); mth = int(m.group(2))
            return date(y, mth, 1)
        except Exception:
            return None
    # YYYY -> Jan 1st
    if re.fullmatch(r"\d{4}", s):
        try:
            return date(int(s), 1, 1)
        except Exception:
            return None
    # ISO date or datetime
    try:
        return date.fromisoformat(s)
    except Exception:
        try:
            return datetime.fromisoformat(s).date()
        except Exception:
            return None


def _to_date_iso(v: Any) -> Optional[str]:
    """Wrapper that returns ISO 'YYYY-MM-DD' string or None via _to_date_py."""
    d = _to_date_py(v)
    return d.isoformat() if d else None

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

    # Preserve raw date values for robust Python fallback later
    RAW_COL = "__date_raw__"
    if RAW_COL not in df.columns:
        df = df.with_columns(pl.col("date").alias(RAW_COL))

    # Vectorized normalization of strings (best effort)
    s = pl.col("date").cast(pl.Utf8)
    norm = (
        s.str.replace_all(r"[\u200b\u200c\u200d\ufeff\u00a0]", "")
        .str.replace_all("年", "-")
        .str.replace_all("月", "-")
        .str.replace_all("日", "")
        .str.replace_all("/", "-")
        # Replace literal dot with dash
        .str.replace_all(".", "-", literal=True)
        # Normalize ISO 'T' separator and strip common timezone suffixes
        .str.replace_all("T", " ")
        .str.replace(r"(?:Z|[+-]\d{2}:?\d{2})$", "", literal=False)
        .str.strip_chars()
        # If only 'YYYY-MM' or 'YYYY-M', pad day to '-01'
        .str.replace(r"^(\d{4}-\d{1,2})$", r"$1-01", literal=False)
        # If only 'YYYY', expand to '-01-01'
        .str.replace(r"^(\d{4})$", r"$1-01-01", literal=False)
    )
    # Try multiple parse formats and coalesce
    # - Support zero/one-digit month/day via %-m/%-d (e.g., 2023-10-1)
    d1 = norm.str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    d1b = norm.str.strptime(pl.Date, "%Y-%-m-%-d", strict=False)
    d2 = norm.str.strptime(pl.Date, "%Y%m%d", strict=False)
    # As datetime and cast to date (handles time part)
    dt1 = norm.str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False).cast(pl.Date)
    dt1b = norm.str.strptime(pl.Datetime, "%Y-%-m-%-d %H:%M:%S", strict=False).cast(pl.Date)
    # Handle datetime strings that lack seconds (HH:MM)
    dt2 = norm.str.strptime(pl.Datetime, "%Y-%m-%d %H:%M", strict=False).cast(pl.Date)
    dt2b = norm.str.strptime(pl.Datetime, "%Y-%-m-%-d %H:%M", strict=False).cast(pl.Date)
    parsed = pl.coalesce([d1, d1b, d2, dt1, dt1b, dt2, dt2b])

    df = df.with_columns(parsed.alias("date"))

    # Fallback to Python for leftover nulls (Excel serials and oddballs)
    if int(df.select(pl.col("date").is_null().sum()).to_series(0)[0]) > 0:
        # Use ISO string fallback to avoid dtype mismatches; then parse once.
        df = df.with_columns(
            pl.when(pl.col("date").is_null())
            .then(pl.col(RAW_COL).map_elements(_to_date_iso, return_dtype=pl.Utf8))
            .otherwise(pl.col("date").cast(pl.Utf8))
            .alias("__date_str__")
        )
        df = df.with_columns(
            pl.col("__date_str__").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("date")
        )
        try:
            df = df.drop("__date_str__")
        except Exception:
            pass

    # Drop helper column if present
    if RAW_COL in df.columns:
        try:
            df = df.drop(RAW_COL)
        except Exception:
            pass

    if required and int(df.select(pl.col("date").is_null().sum()).to_series(0)[0]) == df.height:
        try:
            import logging
            logger = logging.getLogger(__name__)
            # Try to surface a few raw samples for diagnostics
            raw_col = RAW_COL if RAW_COL in df.columns else "date"
            samples = (
                df.select(pl.col(raw_col).cast(pl.Utf8).drop_nulls().head(5)).to_series(0).to_list()
                if raw_col in df.columns
                else []
            )
            logger.warning(f"Date parsing failed with all nulls; raw samples: {samples}")
        except Exception:
            pass
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
