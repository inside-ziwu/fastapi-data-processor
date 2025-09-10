import polars as pl
import pytest
from src.transforms.dr import DRTransform
from src.transforms.message import MessageTransform
# Import other transformers as needed

# --- Contract Test for DRTransform ---
@pytest.mark.skip(reason="Requires a local DR sample file")
def test_dr_transform_contract():
    # This test assumes you have a sample DR file.
    # PLEASE REPLACE 'path/to/your/dr_sample.csv' with the actual path.
    dr_file_path = 'path/to/your/dr_sample.csv'

    try:
        lf = pl.scan_csv(dr_file_path)
    except Exception as e:
        pytest.skip(f"Could not read DR sample file at {dr_file_path}: {e}")

    transformer = DRTransform()
    result_lf = transformer.transform(lf)
    result_df = result_lf.collect()

    expected_cols = {
        "nsc_code", "date", "natural_leads", "paid_leads", 
        "local_leads", "cloud_store_paid_leads", "regional_paid_leads"
    }
    assert expected_cols.issubset(set(result_df.columns))
    assert result_df["date"].dtype == pl.Date
    assert result_df["natural_leads"].dtype == pl.Float64

# --- Contract Test for MsgTransform ---
# Note: Testing MsgTransform is more complex due to multi-sheet logic.
# This test will focus on the consolidation part of the transform.
def test_msg_transform_consolidation():
    # Create a dummy LazyFrame that mimics the output of the multi-sheet read
    data = {
        'nsc_code': ['A001', 'A001', 'B002'],
        'date': ['2023-01-01', '2023-01-01', '2023-01-02'],
        'msg_private_entrants': [10, 5, 20],
        'msg_active_consultations': [8, 2, 15],
        'msg_leads_from_private': [4, 1, 10]
    }
    lf = pl.DataFrame(data).lazy()
    lf = lf.with_columns(pl.col('date').str.strptime(pl.Date))

    transformer = MessageTransform()
    result_lf = transformer.transform(lf)
    result_df = result_lf.collect()

    # After consolidation, there should be only 2 rows
    assert result_df.height == 2
    
    # Check the aggregated value for A001 on 2023-01-01
    row_a001 = result_df.filter(pl.col('nsc_code') == 'A001')
    assert row_a001[0, 'msg_private_entrants'] == 15
    assert row_a001[0, 'msg_active_consultations'] == 10
    assert row_a001[0, 'msg_leads_from_private'] == 5

# Add more contract tests for other transformers below
# e.g., test_video_transform_contract(), test_live_bi_transform_contract(), etc.
