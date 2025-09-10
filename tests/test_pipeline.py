import polars as pl
import pytest
import json
from src.processor import DataProcessor
from src.finalize import finalize_output

# --- End-to-End Golden File Test ---

# PLEASE REPLACE file paths with your actual local data paths
# You need a complete set of 9 files for this test.
SAMPLE_FILES = {
    "video_excel_file": 'tests/sample_data/video_data.xlsx',
    "live_bi_file": 'tests/sample_data/live_data.xlsx',
    "msg_excel_file": 'tests/sample_data/msg_data.xlsx',
    "DR1_file": 'tests/sample_data/DR1.csv',
    "DR2_file": 'tests/sample_data/DR2.csv',
    "account_base_file": 'tests/sample_data/account_base.xlsx',
    "leads_file": 'tests/sample_data/leads.xlsx',
    "account_bi_file": 'tests/sample_data/account_bi.xlsx',
    "Spending_file": 'tests/sample_data/spending.xlsx',
}

# PLEASE CREATE this golden file. 
# One way is to run the test once, capture the output, validate it manually,
# and then save it as the golden file.
GOLDEN_FILE_PATH = 'tests/golden_files/expected_output.json'

@pytest.fixture
def golden_data():
    try:
        with open(GOLDEN_FILE_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        pytest.skip(f"Golden file not found at {GOLDEN_FILE_PATH}. "
                    f"Run the pipeline once to generate an output and save it as the golden file.")

def test_end_to_end_pipeline(golden_data):
    # Check if all sample files exist before running
    for path in SAMPLE_FILES.values():
        if not os.path.exists(path):
            pytest.skip(f"Sample data file not found: {path}. Please provide all 9 sample files.")

    processor = DataProcessor()
    
    # Run the full pipeline with a specific dimension
    result_df_english = processor.run_full_analysis(SAMPLE_FILES, dimension='经销商ID')
    
    # Finalize the output (rename, reorder)
    final_df_chinese = finalize_output(result_df_english)
    
    # Convert result to a list of dicts for comparison
    result_data = final_df_chinese.to_dicts()
    
    # Compare with the golden file data
    # This is a strict comparison. Order of rows and key-value pairs matters.
    # For a more robust test, you might want to sort both lists of dicts by a unique key.
    assert result_data == golden_data, \
        f"Pipeline output does not match the golden file {GOLDEN_FILE_PATH}."
