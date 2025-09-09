import os
import pytest
from unittest.mock import patch
import polars as pl
import numpy as np
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
        "组件点击次数": [100, 200, 300, 400, 500, 600],
        "锚点曝光量": [1000, 2000, 3000, 4000, 5000, 6000],
    }
    return pl.DataFrame(data)

def test_normalization_logic(sample_df):
    """Test the core normalization logic: sum / n_unique."""
    result = _compute_settlement_level_normalized(sample_df).sort("层级")

    expected_data = {
        "层级": ["L1", "L2", "L3"],
        "自然线索量": [15.0, 60.0, 60.0],
        "付费线索量": [7.5, 30.0, 30.0],
        "CTR_组件": [0.1, 0.1, 0.1],
    }
    expected_df = pl.DataFrame(expected_data)

    result_subset = result.select(list(expected_data.keys()))
    assert_frame_equal(result_subset, expected_df, check_dtypes=False)

def test_nsc_key_cleaning_and_null_exclusion():
    """Test that NSC keys are cleaned and nulls/blanks are excluded from n_unique count."""
    data = {
        "层级": ["L1", "L1", "L1", "L1", "L1"],
        "经销商ID": [" A ", "B", None, "", " B"],
        "自然线索量": [10, 20, 30, 40, 50],
    }
    df = pl.DataFrame(data)
    result = _compute_settlement_level_normalized(df, metrics_to_normalize=["自然线索量"], expose_debug_cols=True)
    
    # Unique NSCs are "A" and "B". count = 2.
    # Sum = 10+20+30+40+50 = 150.
    # Avg = 150 / 2 = 75.0
    assert result["level_nsc_count"][0] == 2
    assert result["自然线索量"][0] == 75.0

def test_nan_in_metrics():
    """Test that NaN values in metrics are correctly handled (treated as 0)."""
    data = {
        "层级": ["L1", "L1"],
        "经销商ID": ["A", "B"],
        "自然线索量": [100.0, np.nan], # Use float to allow NaN
    }
    df = pl.DataFrame(data)
    result = _compute_settlement_level_normalized(df, metrics_to_normalize=["自然线索量"], expose_debug_cols=True)

    # sum should be 100 (100 + 0), count is 2. avg = 50.0
    assert result["自然线索量__sum"][0] == 100.0
    assert result["自然线索量"][0] == 50.0

def test_missing_derived_metric_columns(sample_df):
    """Test that derived metrics are not computed if source columns are missing."""
    df_missing_cols = sample_df.drop(["组件点击次数", "锚点曝光量"])
    result = _compute_settlement_level_normalized(df_missing_cols)
    assert "CTR_组件" not in result.columns

def test_debug_cols_exposure(sample_df):
    """Test that debug columns are exposed when requested."""
    result_no_debug = _compute_settlement_level_normalized(sample_df, expose_debug_cols=False)
    assert "level_nsc_count" not in result_no_debug.columns
    assert "自然线索量__sum" not in result_no_debug.columns

    result_with_debug = _compute_settlement_level_normalized(sample_df, expose_debug_cols=True)
    assert "level_nsc_count" in result_with_debug.columns
    assert "自然线索量__sum" in result_with_debug.columns
    assert result_with_debug.sort("层级").filter(pl.col("层级") == "L1")["自然线索量__sum"][0] == 30
