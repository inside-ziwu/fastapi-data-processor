import os
import pytest
from unittest.mock import patch
import polars as pl
from polars.testing import assert_frame_equal

# Import the main function to be tested
from src.analysis.settlement import compute_settlement_cn

@pytest.fixture
def mock_env_on():
    """Fixture to mock the normalization environment variable to 'on'."""
    # Use a dictionary to hold the mock environment variables
    mock_env = {
        "LEVEL_NORMALIZE_BY_NSC": "1"
    }
    with patch("os.getenv", side_effect=lambda key, default=None: mock_env.get(key, default)):
        yield

def test_level_normalization_produces_all_contract_columns(mock_env_on):
    """
    Tests that when LEVEL_NORMALIZE_BY_NSC is on, the final output for the '层级'
    dimension includes all key metrics, especially the derived ratio metrics
    that were previously missing. This test replaces the obsolete tests.
    """
    # 1. Arrange: Create a dataframe with enough source data to calculate all metrics.
    # This makes the test robust against future changes.
    df = pl.DataFrame({
        "层级": ["L1", "L1"],
        "经销商ID": ["A", "B"],
        "门店名": ["Store A", "Store B"],
        # Base metrics for ratios
        "进私人数": [100.0, 50.0],
        "私信开口人数": [40.0, 30.0],
        "咨询留资人数": [20.0, 15.0],
        "有效天数": [30.0, 30.0],
        "车云店+区域投放总金额": [1000.0, 500.0],
        "总付费线索": [10.0, 5.0], # This is an internal, pre-calculated field
        # Add other required columns to prevent errors, with simple values
        "自然线索量": [1.0, 1.0],
        "付费线索量": [1.0, 1.0],
        "直播线索量": [1.0, 1.0],
        "锚点曝光量": [1.0, 1.0],
        "组件点击次数": [1.0, 1.0],
        "组件留资人数（获取线索量）": [1.0, 1.0],
        "短视频条数": [1.0, 1.0],
        "短视频播放量": [1.0, 1.0],
        "直播时长": [1.0, 1.0],
    })

    # 2. Act: Run the main settlement computation function
    result_df = compute_settlement_cn(df, dimension="层级")

    # 3. Assert: Check that the columns that were previously missing now exist.
    # We check for the final, aliased names as they appear in the output contract.
    expected_cols = [
        "直播车云店+区域付费线索量",
        "私信咨询率=开口/进私",
        "咨询留资率=留资/咨询",
        "私信转化率=留资/进私",
    ]

    output_columns = result_df.columns
    for col in expected_cols:
        assert col in output_columns, f"Expected column '{col}' is missing from the final output."

    # A basic sanity check on the output shape and keys
    assert result_df.shape[0] == 1
    assert result_df["层级"][0] == "L1"
