import polars as pl
import logging
from .transforms.base import BaseTransformer
from .transforms.dr import DRTransform
from .transforms.message import MessageTransform
from .transforms.video import VideoTransform
from .transforms.live import LiveTransform
from .transforms.leads import LeadsTransform
from .transforms.spending import SpendingTransform
from .transforms.account_bi import AccountBITransform
from .transforms.account_base import AccountBaseTransform
from .analysis.settlement import compute_settlement_cn
from .config.metric_lists import NUMERIC_METRICS, METRIC_DTYPES

logger = logging.getLogger(__name__)

def _assert_no_collision(left_lf: pl.LazyFrame, right_lf: pl.LazyFrame, keys=('nsc_code','date')):
    left_cols, right_cols = set(left_lf.columns), set(right_lf.columns)
    coll = (left_cols & right_cols) - set(keys)
    if coll:
        raise ValueError(f"Join collision on non-key columns: {sorted(coll)}")

def _assert_account_base_unique(account_base: pl.LazyFrame):
    dup = (account_base.group_by("nsc_code").agg(pl.len().alias("cnt"))
                      .filter(pl.col("cnt") > 1))
    if dup.collect().height > 0:
        # To do: Log the duplicate nsc_codes for debugging
        raise ValueError("account_base has duplicate nsc_code; enforce uniqueness before join")

class DataProcessor:
    def __init__(self):
        self.transformer_map: dict[str, BaseTransformer] = {
            "video_excel_file": VideoTransform(),
            "live_bi_file": LiveBITransform(),
            "msg_excel_file": MsgTransform(),
            "DR1_file": DRTransform(),
            "DR2_file": DRTransform(),
            "account_base_file": AccountBaseTransform(),
            "leads_file": LeadsTransform(),
            "account_bi_file": AccountBITransform(),
            "Spending_file": SpendingTransform(),
        }

    def _process_and_consolidate(self, source_files: dict[str, str]) -> pl.DataFrame:
        processed_lfs: dict[str, pl.LazyFrame] = {}
        account_base_lf: pl.LazyFrame | None = None

        for name, path in source_files.items():
            if name not in self.transformer_map:
                logger.warning(f"No transformer found for {name}, skipping.")
                continue
            try:
                transformer = self.transformer_map[name]
                # All transformers should have a method that returns a LazyFrame
                lf = transformer.process_file_lazy(path)
                processed_lfs[name] = lf
            except Exception as e:
                logger.error(f"Failed to process file {name} at {path}: {e}", exc_info=True)
        
        # Separate account_base
        if "account_base_file" in processed_lfs:
            account_base_lf = processed_lfs.pop("account_base_file")
            _assert_account_base_unique(account_base_lf)

        # Merge DR sources
        dr_lfs = [v for k, v in processed_lfs.items() if k.startswith("DR")]
        other_lfs = [v for k, v in processed_lfs.items() if not k.startswith("DR")]
        if dr_lfs:
            # Union DR files, deduplicate, then aggregate
            dr_union_lf = pl.concat(dr_lfs, how="vertical").unique()
            # The DR transform already aggregates, so we just add it to the list
            other_lfs.append(dr_union_lf)

        if not other_lfs:
            return pl.DataFrame()

        # Build master key table
        all_keys = pl.concat(
            [lf.select(['nsc_code','date']) for lf in other_lfs]
        ).unique()

        # Iterative join with collision assertion
        result_lf = all_keys
        for lf in other_lfs:
            _assert_no_collision(result_lf, lf)
            result_lf = result_lf.join(lf, on=['nsc_code','date'], how='left')

        # Join dimension table
        if account_base_lf is not None:
            _assert_no_collision(result_lf, account_base_lf, keys=('nsc_code',))
            result_lf = result_lf.join(account_base_lf, on='nsc_code', how='left')

        # Finalize schema: create missing columns, fill nulls, cast dtypes
        missing_metrics = [c for c in NUMERIC_METRICS if c not in result_lf.columns]
        result_lf = result_lf.with_columns([pl.lit(0.0).alias(c) for c in missing_metrics])
        result_lf = result_lf.with_columns([
            pl.when(pl.col(c).is_null()).then(0.0).otherwise(pl.col(c)).alias(c)
            for c in NUMERIC_METRICS
        ])
        result_lf = result_lf.with_columns([
            pl.col(k).cast(v) for k,v in METRIC_DTYPES.items() if k in result_lf.columns
        ])

        return result_lf.collect(streaming=True).rechunk()

    def run_full_analysis(self, source_files: dict[str, str], dimension: str) -> pl.DataFrame:
        wide_df = self._process_and_consolidate(source_files)
        if wide_df.is_empty():
            return pl.DataFrame()
        
        final_metrics_df = compute_settlement_cn(wide_df, dimension)
        return final_metrics_df