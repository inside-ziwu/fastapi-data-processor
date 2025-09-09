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
    mock_env = {
        "LEVEL_NORMALIZE_BY_NSC": "1"
    }
    with patch("os.getenv", side_effect=lambda key, default=None: mock_env.get(key, default)):
        yield

def test_full_pipeline_produces_all_contract_columns(mock_env_on):
    """
    Tests the full pipeline from source columns to final output, ensuring
    that columns derived in `_prepare_source_data` (like `总付费线索`)
    and their T/T-1 variants are correctly processed and included in the final output.
    """
    # 1. Arrange: Create a dataframe with source data, NOT intermediate data.
    df = pl.DataFrame({
        "层级": ["L1", "L1"],
        "经销商ID": ["A", "B"],
        "门店名": ["Store A", "Store B"],
        
        # Source data for `总付费线索` and its T/T-1 variants
        "车云店付费线索量": [8.0, 2.0], # Sum = 10
        "区域加码付费线索量": [2.0, 3.0], # Sum = 5
        "T月车云店付费线索量": [8.0, 2.0],
        "T月区域加码付费线索量": [2.0, 3.0],
        "T-1月车云店付费线索量": [8.0, 2.0],
        "T-1月区域加码付费线索量": [2.0, 3.0],

        # Base metrics for other ratios
        "进私人数": [100.0, 50.0],
        "私信开口人数": [40.0, 30.0],
        "咨询留资人数": [20.0, 15.0],
        "有效天数": [30.0, 30.0],
        "车云店+区域投放总金额": [1000.0, 500.0],
        
        # Add other required columns to prevent errors
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

    # 3. Assert: Check that all previously missing columns now exist.
    expected_cols = [
        "直播车云店+区域付费线索量",
        "T月直播车云店+区域付费线索量",
        "T-1月直播车云店+区域付费线索量",
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