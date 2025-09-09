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
    ORDERED_METRICS
)

# --- Tests for _is_level_normalization_enabled --- 

@pytest.mark.parametrize(
    "env_value, expected",
    [
        ("true", True),
        ("1", True),
        ("yes", True),
        ("on", True),
        (None, False),      # Default to False when env var is not set
        ("", False),         # Empty string is False
        ("false", False),
        ("0", False),
        ("no", False),
        ("off", False),
        ("FALSE", False),
    ],
)
@patch("os.getenv")
def test_is_level_normalization_enabled(mock_getenv, env_value, expected):
    """Test the feature flag function with opt-in logic."""
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
        "period": ["T", "T-1", "T", "T", "T-1", "T"],
        "natural_leads": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        "paid_leads": [5.0, 10.0, 15.0, 20.0, 25.0, 30.0],
        "spending_net": [100.0, 200.0, 300.0, 400.0, 500.0, 600.0],
        "component_clicks": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        "anchor_exposure": [100.0, 200.0, 300.0, 400.0, 500.0, 600.0],
        "effective_live_sessions": [1, 1, 1, 1, 1, 1],
        "exposures": [1,1,1,1,1,1], "viewers": [1,1,1,1,1,1],
        "T月有效天数": [30, 0, 30, 30, 0, 30],
        "T-1月有效天数": [0, 31, 0, 0, 31, 0],
    }
    return pl.DataFrame(data)

def test_normalization_logic(sample_df):
    """Test the core normalization logic: sum / n_unique."""
    result = _compute_settlement_level_normalized(sample_df).sort("层级")

    expected_data = {
        "层级": ["L1", "L2", "L3"],
        "自然线索量": [15.0, 60.0, 60.0],
        "付费线索量": [7.5, 30.0, 30.0],
        "组件点击率": [0.1, 0.1, 0.1],
    }
    expected_df = pl.DataFrame(expected_data)

    result_subset = result.select(list(expected_data.keys()))
    assert_frame_equal(result_subset, expected_df, check_dtypes=False)

def test_nsc_key_cleaning_and_null_exclusion():
    """Test that NSC keys are cleaned and nulls/blanks are excluded from n_unique count."""
    data = {
        "层级": ["L1", "L1", "L1", "L1", "L1"],
        "经销商ID": [" A ", "B", None, "", " B"],
        "period": ["T", "T", "T", "T", "T"],
        "natural_leads": [10.0, 20.0, 30.0, 40.0, 50.0],
        "spending_net": [1,1,1,1,1],
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
        "period": ["T", "T"],
        "natural_leads": [100.0, np.nan],
        "spending_net": [1,1],
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

def test_final_column_contract():
    """Ensures the function output contains all columns from ORDERED_METRICS."""
    # Create a dataframe with all possible source columns to ensure all metrics can be calculated
    source_cols = {
        "层级": "L1", "经销商ID": "A", "period": "T",
        "natural_leads": 1.0, "paid_leads": 1.0, "spending_net": 1.0,
        "live_effective_hours": 1.0, "live_leads": 1.0, "anchor_exposure": 1.0,
        "component_clicks": 1.0, "short_video_leads": 1.0, "short_video_count": 1.0,
        "short_video_plays": 1.0, "effective_live_sessions": 1.0, "exposures": 1.0,
        "viewers": 1.0, "small_wheel_clicks": 1.0, "small_wheel_leads": 1.0,
        "enter_private_count": 1.0, "private_open_count": 1.0, "private_leads_count": 1.0,
        "over25_min_live_mins": 1.0, "store_paid_leads": 1.0, "area_paid_leads": 1.0,
        "local_leads": 1.0, "T月有效天数": 1.0, "T-1月有效天数": 1.0
    }
    df = pl.DataFrame({k: [v] for k, v in source_cols.items()})

    out = _compute_settlement_level_normalized(df)

    # Check if all expected columns are present
    expected_cols = ["层级"] + ORDERED_METRICS
    # Using sets to ignore order and quickly find differences
    missing_cols = set(expected_cols) - set(out.columns)
    extra_cols = set(out.columns) - set(expected_cols)

    assert not missing_cols, f"Missing columns in output: {sorted(list(missing_cols))}"
    assert not extra_cols, f"Extra columns in output: {sorted(list(extra_cols))}"
    assert len(out.columns) == len(expected_cols), f"Expected {len(expected_cols)} columns, but got {len(out.columns)}"