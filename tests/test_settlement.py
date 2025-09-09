import os
import pytest
from unittest.mock import patch
import polars as pl
from polars.testing import assert_frame_equal

# Import the functions to be tested
from src.analysis.settlement import _is_level_normalization_enabled, _compute_settlement_level_normalized

# --- Tests for _is_level_normalization_enabled --- 

@pytest.mark.parametrize(
    "env_value, expected",
    [
        ("true", True),
        ("1", True),
        ("yes", True),
        ("on", True),
        ("TRUE", True),
        ("On", True),
        ("false", False),
        ("0", False),
        ("no", False),
        ("off", False),
        ("", False),
        (None, False), # Test case for when env var is not set
    ],
)
@patch("os.getenv")
def test_is_level_normalization_enabled(mock_getenv, env_value, expected):
    """Test the feature flag function with various environment variable values."""
    mock_getenv.return_value = env_value
    result = _is_level_normalization_enabled()
    assert result is expected
    # When the input is None, the default value is used.
    mock_getenv.assert_called_with("LEVEL_NORMALIZE_BY_NSC")

# --- Tests for _compute_settlement_level_normalized ---

@pytest.fixture
def sample_df() -> pl.DataFrame:
    """Create a sample DataFrame for testing."""
    data = {
        "层级": ["L1", "L1", "L2", "L2", "L2", "L3"],
        "经销商ID": ["A", "B", "C", "C", "D", "E"],
        "自然线索量": [10, 20, 30, 40, 50, 60],
        "付费线索量": [5, 10, 15, 20, 25, 30],
        "直播时长": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        "组件点击次数": [100, 200, 300, 400, 500, 600],
        "锚点曝光量": [1000, 2000, 3000, 4000, 5000, 6000],
    }
    return pl.DataFrame(data)

def test_normalization_logic(sample_df):
    """Test the core normalization logic: sum / n_unique."""
    result = _compute_settlement_level_normalized(sample_df).sort("层级")

    # Expected values, sorted by level (L1, L2, L3)
    expected_data = {
        "层级": ["L1", "L2", "L3"],
        "自然线索量": [15.0, 60.0, 60.0],
        "付费线索量": [7.5, 30.0, 30.0],
        "CTR_组件": [0.1, 0.1, 0.1], 
    }
    expected_df = pl.DataFrame(expected_data)

    # Select only columns that are expected to be in the result for this test
    result_subset = result.select(list(expected_data.keys()))
    assert_frame_equal(result_subset, expected_df, check_dtypes=False)

def test_zero_nsc_count_group():
    """Test a group that has rows but all its NSC_KEYs are null."""
    data = {
        "层级": ["L1", "L1"],
        "经销商ID": [None, None],
        "自然线索量": [100, 200],
        "付费线索量": [50, 100],
        "直播时长": [10.0, 20.0],
        "组件点击次数": [1, 1],
        "锚点曝光量": [10, 10],
    }
    df = pl.DataFrame(data)
    result = _compute_settlement_level_normalized(df)
    
    # n_unique of [None, None] is 1. The logic should handle this.
    # The count is 1, so the result is the sum.
    assert result["自然线索量"][0] == 300.0

def test_missing_derived_metric_columns(sample_df):
    """Test that derived metrics are not computed if source columns are missing."""
    # Drop columns needed for CTR calculation
    df_missing_cols = sample_df.drop(["组件点击次数", "锚点曝光量"])
    result = _compute_settlement_level_normalized(df_missing_cols)
    # Assert that the derived CTR column was not created
    assert "CTR_组件" not in result.columns

def test_debug_cols_exposure(sample_df):
    """Test that debug columns are exposed when requested."""
    result_no_debug = _compute_settlement_level_normalized(sample_df, expose_debug_cols=False)
    assert "level_nsc_count" not in result_no_debug.columns
    assert "自然线索量__sum" not in result_no_debug.columns

    result_with_debug = _compute_settlement_level_normalized(sample_df, expose_debug_cols=True)
    assert "level_nsc_count" in result_with_debug.columns
    assert "自然线索量__sum" in result_with_debug.columns
    # Sort before indexing to ensure stable test
    assert result_with_debug.sort("层级").filter(pl.col("层级") == "L1")["自然线索量__sum"][0] == 30