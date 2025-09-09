import polars as pl
import unicodedata as ud
import re

DEFAULT_BAD_TOKENS = {"", "NULL", "N/A", "-", "—", "无", "空", "未知"}

_ZW = re.compile(r"[\u200B-\u200D\uFEFF]")
_NBSP = re.compile(r"\u00A0")

def _canon(s: str | None) -> str | None:
    if s is None: return None
    s = ud.normalize("NFKC", str(s))
    s = _ZW.sub("", s)
    s = _NBSP.sub(" ", s)
    return s.strip()

def sanitize_key(
    df: pl.DataFrame | pl.LazyFrame, 
    col: str, 
    bad_tokens: set[str] = DEFAULT_BAD_TOKENS,
    keep_raw: bool = True,
    normalize_unicode: bool = False
) -> pl.DataFrame | pl.LazyFrame:
    if col not in df.columns:
        return df

    if keep_raw and f"{col}__raw" not in df.columns:
        df = df.with_columns(pl.col(col).alias(f"{col}__raw"))

    # Decide on the initial expression
    if normalize_unicode:
        # First, handle the non-vectorizable part
        expr = pl.col(col).cast(pl.Utf8).map_elements(_canon, return_dtype=pl.Utf8)
    else:
        # Purely vectorized path
        expr = pl.col(col).cast(pl.Utf8)

    # Then, apply unified vectorized cleaning
    final_expr = expr.str.strip_chars()
    final_expr = (
        pl.when(final_expr.is_in(bad_tokens))
        .then(None)
        .otherwise(final_expr)
    )

    return df.with_columns(final_expr.alias(col))