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


def ensure_date_column(df: pl.DataFrame, date_candidates: Optional[List[str]] = None) -> pl.DataFrame:
    """Ensure date column exists and is properly formatted (fail-fast).

    Parsing strategy:
    - Accept common headers (中文/英文) and rename to 'date'.
    - NFKC normalize text, trim, unify separators '/', '.' -> '-', remove '年/月/日'.
    - Accept 'YYYYMMDD' 8-digit form.
    - Accept ISO datetime strings (keep date part).
    - Accept Excel serial numbers (rough range) as days from 1899-12-30.
    - Finally require non-null dates; raise if all null.
    """
    if date_candidates is None:
        date_candidates = ["日期", "留资日期", "date", "time", "开播日期", "直播日期", "日期时间"]

    if "date" not in df.columns:
        # Try to find date column with different names
        for candidate in date_candidates:
            if candidate in df.columns:
                df = df.rename({candidate: "date"})
                break
        # Fallback: normalized name match (handles zero-width/nbspace/fullwidth)
        if "date" not in df.columns:
            def _norm(s: str) -> str:
                s = re.sub(r"[\u200b\u200c\u200d\ufeff\u00a0]+", "", s or "")
                s = unicodedata.normalize("NFKC", s)
                return s.replace(" ", "").replace("（", "(").replace("）", ")").replace("：", ":").lower()
            norm_map = {_norm(c): c for c in df.columns}
            for candidate in date_candidates:
                key = _norm(candidate)
                if key in norm_map:
                    df = df.rename({norm_map[key]: "date"})
                    break

    if "date" not in df.columns:
        raise ValueError(f"No date column found. Available: {df.columns}")

    # Normalize dtype: always end up with pl.Date
    dt = df["date"].dtype
    if dt == pl.Date:
        return df
    elif dt == pl.Datetime:
        df = df.with_columns(pl.col("date").cast(pl.Date).alias("date"))
    else:
        def _to_date_py(v: Any) -> Optional[date]:
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
            # ISO date
            try:
                return date.fromisoformat(s)
            except Exception:
                # Try datetime
                try:
                    return datetime.fromisoformat(s).date()
                except Exception:
                    return None

        df = df.with_columns(pl.col("date").map_elements(_to_date_py, return_dtype=pl.Date).alias("date"))

    # Fail-fast if all nulls
    if int(df.select(pl.col("date").is_null().sum()).to_series(0)[0]) == df.height:
        raise ValueError("Date parsing failed: all values are null after normalization")

    return df


def ensure_optional_date_column(
    df: pl.DataFrame, date_candidates: Optional[List[str]] = None
) -> pl.DataFrame:
    """Ensure date column if present; do not raise when missing.

    Same parsing strategy as ensure_date_column, but never raises if missing
    or all-null; caller decides how to handle.
    """
    if date_candidates is None:
        date_candidates = ["日期", "留资日期", "date", "time", "开播日期", "直播日期", "日期时间"]

    if "date" not in df.columns:
        for candidate in date_candidates:
            if candidate in df.columns:
                df = df.rename({candidate: "date"})
                break
        if "date" not in df.columns:
            def _norm(s: str) -> str:
                s = re.sub(r"[\u200b\u200c\u200d\ufeff\u00a0]+", "", s or "")
                s = unicodedata.normalize("NFKC", s)
                return s.replace(" ", "").replace("（", "(").replace("）", ")").replace("：", ":").lower()
            norm_map = {_norm(c): c for c in df.columns}
            for candidate in date_candidates:
                key = _norm(candidate)
                if key in norm_map:
                    df = df.rename({norm_map[key]: "date"})
                    break

    if "date" in df.columns:
        dt = df["date"].dtype
        if dt == pl.Date:
            return df
        elif dt == pl.Datetime:
            df = df.with_columns(pl.col("date").cast(pl.Date).alias("date"))
        else:
            def _to_date_py(v: Any) -> Optional[date]:
                if v is None:
                    return None
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
                s = str(v)
                s = unicodedata.normalize("NFKC", s).strip()
                if "T" in s:
                    s = s.split("T", 1)[0]
                if " " in s:
                    s = s.split(" ", 1)[0]
                s = s.replace("年", "-").replace("月", "-").replace("日", "")
                s = s.replace("/", "-").replace(".", "-")
                if re.fullmatch(r"\d{8}", s):
                    s = f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
                try:
                    return date.fromisoformat(s)
                except Exception:
                    try:
                        return datetime.fromisoformat(s).date()
                    except Exception:
                        return None

            df = df.with_columns(pl.col("date").map_elements(_to_date_py, return_dtype=pl.Date).alias("date"))

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
