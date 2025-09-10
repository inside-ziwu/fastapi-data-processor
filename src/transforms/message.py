import polars as pl
import pandas as pd
from .base import BaseTransformer
from ..config.source_mappings import MSG_MAP
import re

class MessageTransform(BaseTransformer):
    def __init__(self):
        super().__init__(MSG_MAP)

    def _parse_date_from_sheet_name(self, sheet_name: str) -> str | None:
        s = str(sheet_name or "").strip()
        s = s.replace("年", "-").replace("月", "-").replace("日", "")
        s = s.replace("/", "-").replace(".", "-")
        s = re.sub(r"\s+", "", s)
        m = re.search(r"(20\d{2})-(\d{1,2})-(\d{1,2})", s)
        if m:
            y, mo, da = m.group(1), int(m.group(2)), int(m.group(3))
            return f"{y}-{mo:02d}-{da:02d}"
        return None

    def process_file_lazy(self, path: str) -> pl.LazyFrame:
        """Override to handle multi-sheet Excel file."""
        try:
            xls = pd.ExcelFile(path)
        except Exception as e:
            raise ValueError(f"Could not read Excel file at {path}: {e}")

        processed_sheets = []
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str)
            lf = pl.from_pandas(df).lazy()
            lf = self.rename_and_select(lf)
            
            date_str = self._parse_date_from_sheet_name(sheet_name)
            if date_str is None:
                # Fallback or error
                raise ValueError(f"Could not parse date from sheet name: '{sheet_name}' in {path}")
            
            lf = lf.with_columns(pl.lit(date_str).str.strptime(pl.Date).alias('date'))
            processed_sheets.append(lf)

        if not processed_sheets:
            return pl.LazyFrame()

        return pl.concat(processed_sheets, how="vertical")

    def transform(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        # The file processing is already done in the overridden process_file_lazy
        # This transform now just does the consolidation.
        metric_cols = ["msg_private_entrants", "msg_active_consultations", "msg_leads_from_private"]
        consolidated_lf = lf.group_by(["nsc_code", "date"]).agg([
            pl.col(c).sum() for c in metric_cols
        ])
        return self.cast_to_float(consolidated_lf, metric_cols)
