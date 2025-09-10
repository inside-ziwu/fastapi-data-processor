import polars as pl
from ..config.constants import DEFAULT_TZ
from ..config.metric_lists import NUMERIC_METRICS, V10_NORMALIZATION_LIST

def SAFE_DIV(num: pl.Expr, den: pl.Expr) -> pl.Expr:
    return (num / den).nan_to_null().fill_null(0.0)

def compute_settlement_cn(df: pl.DataFrame, dimension: str) -> pl.DataFrame:
    # Timezone guardrail and month truncation
    lf = df.lazy().with_columns([
        pl.when(pl.col("date").is_dtype(pl.Datetime))
          .then(pl.col("date").dt.convert_time_zone(DEFAULT_TZ).dt.date())
          .otherwise(pl.col("date"))
          .alias("date")
    ]).with_columns(pl.col("date").dt.truncate("1mo").alias("month"))

    # Define aggregation keys based on dimension
    keys = ['level'] if dimension == '层级' else ['nsc_code','store_name','level']
    
    # Define aggregation expressions
    agg_exprs = [pl.col(m).sum().alias(m) for m in NUMERIC_METRICS if m in lf.columns]
    agg_exprs.append(pl.col('date').n_unique().alias('effective_days'))

    # Monthly aggregation
    monthly = (lf.group_by(keys+['month']).agg(agg_exprs)
                 .sort(keys+['month']))

    # T/T-1 alignment using shift
    # Note: This creates T-1 data, but the final output spec requires T and T-1 columns.
    # This logic needs to be adapted to pivot the data into T and T-1 columns.
    # For now, we will proceed with the aggregation and derivation logic on the monthly data.

    # Pre-aggregation composite metrics (calculated on monthly aggregated data)
    monthly = monthly.with_columns([
        (pl.col('natural_leads') + pl.col('paid_leads')).alias('total_leads'),
        (pl.col('cloud_store_paid_leads') + pl.col('regional_paid_leads')).alias('total_paid_leads'),
    ])

    # Post-aggregation derived metrics
    # This should be a separate spec-driven step, as discussed.
    derived = monthly.with_columns([
        SAFE_DIV(pl.col('spending_net'), pl.col('total_paid_leads')).alias('cpl_paid'),
        SAFE_DIV(pl.col('video_form_leads'), pl.col('video_anchor_exposures')).alias('component_lead_ratio'),
        # ... all other derived metrics from the blueprint
    ])

    # Level normalization
    if dimension == '层级':
        nsc_counts = (lf.group_by(['level','month'])
                        .agg(pl.col('nsc_code').n_unique().alias('level_nsc_count')))
        derived = (derived.join(nsc_counts, on=['level','month'], how='left')
                        .with_columns([
                            SAFE_DIV(pl.col(c), pl.col('level_nsc_count')).alias(c)
                            for c in V10_NORMALIZATION_LIST if c in derived.columns
                        ]))

    return derived.collect(streaming=True).rechunk()