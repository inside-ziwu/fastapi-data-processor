import polars as pl
import unicodedata as ud
import re

# Define bad tokens that should be considered null
BAD_TOKENS = {"", "NULL", "N/A", "-", "—", "无", "空", "未知"}

# Pre-compile regex for performance
_ZW = re.compile(r"[\u200B-\u200D\uFEFF]")  # Zero-width characters + BOM
_NBSP = re.compile(r"\u00A0")                 # Non-breaking space

def _canon(s: str | None) -> str | None:
    """Canonicalization function for a single string value."""
    if s is None:
        return None
    
    # NFKC normalization handles full-width to half-width, among other things.
    s = ud.normalize("NFKC", str(s))
    
    # Remove invisible characters
    s = _ZW.sub("", s)
    s = _NBSP.sub(" ", s)
    
    # Strip whitespace
    s = s.strip()
    
    # Return None if the cleaned string is in the bad tokens list
    return None if s in BAD_TOKENS else s

def sanitize_key(
    df: pl.DataFrame | pl.LazyFrame, 
    col: str, 
    keep_raw: bool = True
) -> pl.DataFrame | pl.LazyFrame:
    """
    Applies a robust sanitization process to a key column in a DataFrame.

    Args:
        df: The input DataFrame or LazyFrame.
        col: The name of the key column to sanitize.
        keep_raw: If True, keeps the original column renamed with a `__raw` suffix.

    Returns:
        The DataFrame with the sanitized key column.
    """
    if col not in df.columns:
        # If the key column doesn't exist, do nothing.
        return df

    raw_col = f"{col}__raw"
    if keep_raw and raw_col not in df.columns:
        df = df.with_columns(pl.col(col).alias(raw_col))
    
    return df.with_columns(
        pl.col(col).cast(pl.Utf8).map_elements(_canon, return_dtype=pl.Utf8).alias(col)
    )
