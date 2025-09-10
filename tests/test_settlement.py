import pytest
import polars as pl
from datetime import date
from src.analysis.settlement import compute_settlement_cn


def test_settlement_logic_with_english_names():
    """Tests the settlement logic using internal English names."""
    # 1. Arrange: Create a dataframe with internal English names
    df = pl.DataFrame({
        "level": ["L1", "L1"],
        "nsc_code": ["A", "B"],
        "store_name": ["Store A", "Store B"],
        "date": [date(2023, 1, 1), date(2023, 1, 2)],
        "cloud_store_paid_leads": [8.0, 2.0],
        "regional_paid_leads": [2.0, 3.0],
        "msg_private_entrants": [100.0, 50.0],
        "msg_active_consultations": [40.0, 30.0],
        "msg_leads_from_private": [20.0, 15.0],
        "effective_days": [30.0, 30.0],
        "spending_net": [1000.0, 500.0],
        "natural_leads": [1.0, 1.0],
        "paid_leads": [1.0, 1.0],
        "account_bi_live_form_leads": [1.0, 1.0],
        "video_anchor_exposures": [1.0, 1.0],
        "video_anchor_clicks": [1.0, 1.0],
        "video_form_leads": [1.0, 1.0],
        "video_new_posts": [1.0, 1.0],
        "account_bi_video_views": [1.0, 1.0],
        "live_effective_duration_hr": [1.0, 1.0],
    })

    # 2. Act: Run the main settlement computation function
    result_df = compute_settlement_cn(df, dimension="层级")

    # 3. Assert: Check for calculated columns using internal English names
    expected_cols = [
        "total_paid_leads", #直播车云店+区域付费线索量
        "cpl_paid", #付费CPL（车云店+区域）
        "private_msg_consult_ratio", #私信咨询率=开口/进私
    ]

    output_columns = result_df.columns
    for col in expected_cols:
        assert col in output_columns, f"Expected column '{col}' is missing from the final output."

    assert result_df.shape[0] == 1
    assert result_df["level"][0] == "L1"
