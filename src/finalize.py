import polars as pl
from ..config.output_spec import FINAL_NAME_MAP, OUTPUT_ORDER
import logging

logger = logging.getLogger(__name__)

def finalize_output(df: pl.DataFrame) -> pl.DataFrame:
    """Prepares the final dataframe for API output, ensuring schema and order."""
    # 1) Check for duplicate Chinese names in the mapping
    cn_names = list(FINAL_NAME_MAP.values())
    if len(cn_names) != len(set(cn_names)):
        dups = {x for x in cn_names if cn_names.count(x) > 1}
        raise ValueError(f"FINAL_NAME_MAP has duplicate Chinese names: {sorted(dups)}")

    # 2) Check that all columns in OUTPUT_ORDER exist in the dataframe
    missing_in_df = [c for c in OUTPUT_ORDER if c not in df.columns]
    if missing_in_df:
        raise KeyError(f"OUTPUT_ORDER missing columns in df: {missing_in_df}")

    # 3) Check that all columns in OUTPUT_ORDER have a mapping
    mapping_missing = [c for c in OUTPUT_ORDER if c not in FINAL_NAME_MAP]
    if mapping_missing:
        raise KeyError(f"FINAL_NAME_MAP missing keys for: {mapping_missing}")

    # 4) Warn about extra columns in df that won't be in the output
    extras = [c for c in df.columns if c not in OUTPUT_ORDER]
    if extras:
        logger.warning(f"Extra columns not in OUTPUT_ORDER, will be dropped: {extras}")

    # 5) Rename to Chinese and select in the correct order
    df_renamed = df.rename(FINAL_NAME_MAP)
    
    # Select only the columns that are in the output spec, in the correct order
    final_cols_cn = [FINAL_NAME_MAP[c] for c in OUTPUT_ORDER]
    
    return df_renamed.select(final_cols_cn)
