from abc import ABC, abstractmethod
import polars as pl
import pandas as pd
from pathlib import Path

class BaseTransformer(ABC):
    def __init__(self, mapping: dict[str, str]):
        self.mapping = mapping

    def read_source_to_lazy(self, path: str) -> pl.LazyFrame:
        p = Path(path)
        suf = p.suffix.lower()
        if suf in {".parquet", ".pq"}:
            return pl.scan_parquet(path)
        if suf in {".csv", ".txt"}:
            return pl.scan_csv(path, ignore_errors=False)
        if suf in {".xlsx", ".xls"}:
            # Read all as string to avoid pandas type inference issues
            df = pd.read_excel(path, sheet_name=0, dtype=str)
            return pl.from_pandas(df).lazy()
        raise ValueError(f"Unsupported file type: {suf}")

    def rename_and_select(self, df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
        lf = df.lazy() if isinstance(df, pl.DataFrame) else df
        names = set(lf.columns)
        # Use mapping keys from self.mapping for checking
        missing = [k for k in self.mapping if k not in names]
        if missing:
            # Allow some mappings to be optional, check for a special marker
            if not all("__optional" in k for k in missing):
                 raise KeyError(f"Source missing required columns: {missing}")

        # Filter mapping to only include columns present in the dataframe
        valid_mapping = {k: v for k, v in self.mapping.items() if k in names}
        lf = lf.rename(valid_mapping).select(list(valid_mapping.values()))
        
        # Standardize nsc_code if it exists
        if 'nsc_code' in lf.columns:
            lf = lf.with_columns([
                pl.col('nsc_code').cast(pl.Utf8).str.strip_chars().alias('nsc_code'),
            ])

        # Standardize date if it exists
        if 'date' in lf.columns:
             lf = lf.with_columns([
                pl.when(pl.col('date').is_not_null())
                  .then(pl.col('date').str.strptime(pl.Date, strict=False))
                  .otherwise(pl.lit(None).cast(pl.Date))
                  .alias('date')
            ])
        return lf

    @staticmethod
    def cast_to_float(lf: pl.LazyFrame, metric_cols: list[str]) -> pl.LazyFrame:
        return lf.with_columns([pl.col(c).cast(pl.Float64) for c in metric_cols if c in lf.columns])

    @abstractmethod
    def transform(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        pass

    def process_file_lazy(self, path: str) -> pl.LazyFrame:
        lf = self.read_source_to_lazy(path)
        return self.transform(lf)