import os
import pytest
from unittest.mock import patch
import polars as pl
import numpy as np
from polars.testing import assert_frame_equal

# Import the functions to be tested
from src.analysis.settlement import (
    _is_level_normalization_enabled,
    _compute_settlement_level_normalized,
)

# --- Tests for _is_level_normalization_enabled --- 

@pytest.mark.parametrize(
    "env_value, expected",
    [
        (None, True),      # Default to True when env var is not set
        ("true", True),
        ("1", True),
        ("yes", True),
        ("on", True),
        ("", True),         # Empty string is not an explicit disable
        ("false", False),
        ("0", False),
        ("no", False),
        ("off", False),
        ("FALSE", False),
    ],
)
@patch("os.getenv")
def test_is_level_normalization_enabled(mock_getenv, env_value, expected):
    """Test the feature flag function with the new default-on logic."""
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
        "自然线索量": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        "付费线索量": [5.0, 10.0, 15.0, 20.0, 25.0, 30.0],
        "组件点击次数": [100.0, 200.0, 300.0, 400.0, 500.0, 600.0],
        "锚点曝光量": [1000.0, 2000.0, 3000.0, 4000.0, 5000.0, 6000.0],
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
        "自然线索量": [10.0, 20.0, 30.0, 40.0, 50.0],
    }
    df = pl.DataFrame(data)
    result = _compute_settlement_level_normalized(df, expose_debug_cols=True)
    
    assert result["level_nsc_count"][0] == 2
    assert result["自然线索量"][0] == 75.0

def test_nan_in_metrics():
    """Test that NaN values in metrics are correctly handled (treated as 0)."""
    data = {
        "层级": ["L1", "L1"],
        "经销商ID": ["A", "B"],
        "自然线索量": [100.0, np.nan],
    }
    df = pl.DataFrame(data)
    result = _compute_settlement_level_normalized(df, expose_debug_cols=True)

    assert result["自然线索量__sum"][0] == 100.0
    assert result["自然线索量"][0] == 50.0

def test_identity_sum_equals_norm_times_count(sample_df):
    """Test the identity: normalized * count should be approx equal to sum."""
    result = _compute_settlement_level_normalized(sample_df, expose_debug_cols=True).sort("层级")
    
    l1_data = result.filter(pl.col("层级") == "L1")
    assert np.isclose(l1_data["自然线索量"][0] * l1_data["level_nsc_count"][0], l1_data["自然线索量__sum"][0])

    l2_data = result.filter(pl.col("层级") == "L2")
    assert np.isclose(l2_data["自然线索量"][0] * l2_data["level_nsc_count"][0], l2_data["自然线索量__sum"][0])
