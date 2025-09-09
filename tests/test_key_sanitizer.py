import pytest
import polars as pl
from polars.testing import assert_frame_equal

from src.cleaning.key_sanitizer import sanitize_key, DEFAULT_BAD_TOKENS

@pytest.mark.parametrize(
    "input_val, expected_val",
    [
        ("  ABC  ", "ABC"),
        (None, None),
        ("", None),
        ("NULL", None),
        ("N/A", None),
    ]
)
def test_sanitize_key_vectorized(input_val, expected_val):
    """Test the vectorized part of sanitize_key."""
    df = pl.DataFrame({"ID": [input_val]})
    # Test without unicode normalization to focus on vectorized logic
    sanitized_df = sanitize_key(df, "ID", keep_raw=False, normalize_unicode=False)
    assert sanitized_df["ID"][0] == expected_val

def test_sanitize_key_unicode_normalization():
    """Test the unicode normalization UDF part of sanitize_key."""
    data = {
        "ID": ["ＡＢＣ１２３", "A\u200bB", "A\u00a0B"],
        "Expected": ["ABC123", "AB", "A B"],
    }
    df = pl.DataFrame(data)
    # Enable unicode normalization
    sanitized_df = sanitize_key(df, "ID", keep_raw=False, normalize_unicode=True)
    assert_frame_equal(sanitized_df.select("ID"), df.select("Expected").rename({"Expected": "ID"}))

def test_sanitize_key_custom_bad_tokens():
    """Test overriding the default bad tokens."""
    df = pl.DataFrame({"ID": ["GOOD", "BAD"]})
    custom_bad_tokens = {"BAD"}
    sanitized_df = sanitize_key(df, "ID", bad_tokens=custom_bad_tokens, keep_raw=False)
    expected = pl.DataFrame({"ID": ["GOOD", None]})
    assert_frame_equal(sanitized_df, expected)

def test_sanitize_key_preserves_raw():
    """Test that keep_raw=True preserves the original column."""
    df = pl.DataFrame({"ID": ["  A  "]})
    sanitized_df = sanitize_key(df, "ID", keep_raw=True)
    assert "ID__raw" in sanitized_df.columns
    assert sanitized_df["ID__raw"][0] == "  A  "
    assert sanitized_df["ID"][0] == "A"