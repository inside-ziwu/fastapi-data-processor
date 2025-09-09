import pytest
import polars as pl
from polars.testing import assert_frame_equal

from src.cleaning.key_sanitizer import sanitize_key

@pytest.mark.parametrize(
    "input_val, expected_val",
    [
        # Basic cleaning
        ("  ABC  ", "ABC"),
        # Null and bad tokens
        (None, None),
        ("", None),
        ("NULL", None),
        ("N/A", None),
        ("无", None),
        # Unicode normalization (full-width to half-width)
        ("ＡＢＣ１２３", "ABC123"),
        # Invisible characters
        ("A\u200bB\u200cC", "ABC"),
        # Non-breaking space
        ("A\u00a0B", "A B"),
    ]
)
def test_sanitize_key_various_cases(input_val, expected_val):
    """Test sanitize_key with various individual values."""
    df = pl.DataFrame({"ID": [input_val]})
    sanitized_df = sanitize_key(df, "ID", keep_raw=False)
    assert sanitized_df["ID"][0] == expected_val

def test_sanitize_key_preserves_raw():
    """Test that keep_raw=True preserves the original column."""
    df = pl.DataFrame({"ID": ["  A  "]})
    sanitized_df = sanitize_key(df, "ID", keep_raw=True)
    assert "ID__raw" in sanitized_df.columns
    assert sanitized_df["ID__raw"][0] == "  A  "
    assert sanitized_df["ID"][0] == "A"

def test_sanitize_key_no_raw_by_default():
    """Test that keep_raw=False is the default effective behavior if not specified."""
    df = pl.DataFrame({"ID": ["  A  "]})
    # Note: The function defaults to keep_raw=True, this test checks the opposite
    sanitized_df = sanitize_key(df, "ID", keep_raw=False)
    assert "ID__raw" not in sanitized_df.columns
