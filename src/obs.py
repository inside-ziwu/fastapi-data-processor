from datetime import datetime
import polars as pl
from pathlib import Path

def dump_plan(lf: pl.LazyFrame, tag: str = "pipeline"):
    """Dumps the logical and optimized query plans to the logs directory."""
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    try:
        logical = lf.describe_plan()
    except Exception:
        logical = lf.explain() # Fallback for older polars versions
    try:
        optimized = lf.describe_optimized_plan()
    except Exception:
        optimized = logical
    
    (log_dir / f"{tag}-logical-{ts}.txt").write_text(logical)
    (log_dir / f"{tag}-optimized-{ts}.txt").write_text(optimized)

def audit(step: str, lf_before: pl.LazyFrame, lf_after: pl.LazyFrame, key_cols=("nsc_code","date")):
    """Prints an audit trail of key counts before and after a step."""
    b = lf_before.select([*key_cols]).unique().collect().height
    a = lf_after.select([*key_cols]).unique().collect().height
    coverage_delta = a - b
    print(f"[audit] {step}: uniq_keys_before={b}, uniq_keys_after={a}, coverage_delta={coverage_delta}")
