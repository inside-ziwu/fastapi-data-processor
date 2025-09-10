import polars as pl
from ..config.constants import DEFAULT_TZ
from ..config.metric_lists import NUMERIC_METRICS, V10_NORMALIZATION_LIST

def SAFE_DIV(num: pl.Expr, den: pl.Expr) -> pl.Expr:
    return pl.when(den != 0).then(num / den).otherwise(0.0).fill_nan(0.0).fill_null(0.0)

def compute_settlement_cn(df: pl.DataFrame, dimension: str) -> pl.DataFrame:
    # This function now assumes the input DataFrame has internal English column names

    # Timezone guardrail and month truncation
    if 'date' in df.columns:
        lf = df.lazy()
        date_col_type = lf.schema['date']
        if date_col_type == pl.Datetime:
            lf = lf.with_columns(
                pl.col("date").dt.convert_time_zone(DEFAULT_TZ).dt.date().alias("date")
            )
        lf = lf.with_columns(pl.col("date").dt.truncate("1mo").alias("month"))
    else:
        lf = df.lazy()

    keys = ['level'] if dimension == '层级' else ['nsc_code', 'store_name', 'level']
    
    agg_exprs = [pl.col(m).sum().alias(m) for m in NUMERIC_METRICS if m in lf.columns]
    if 'date' in lf.columns:
        agg_exprs.append(pl.col('date').n_unique().alias('effective_days'))

    group_keys = [k for k in keys if k in lf.columns]
    if 'month' in lf.columns:
        group_keys.append('month')
    
    if not group_keys:
        return pl.DataFrame()
        
    monthly = (lf.group_by(group_keys).agg(agg_exprs).sort(group_keys))

    # Composite and derived metrics
    # This part should be driven by a spec from config for better maintenance
    if 'natural_leads' in monthly.columns and 'paid_leads' in monthly.columns:
        monthly = monthly.with_columns((pl.col('natural_leads') + pl.col('paid_leads')).alias('total_leads'))
    if 'cloud_store_paid_leads' in monthly.columns and 'regional_paid_leads' in monthly.columns:
        monthly = monthly.with_columns((pl.col('cloud_store_paid_leads') + pl.col('regional_paid_leads')).alias('total_paid_leads'))
    if 'spending_net' in monthly.columns and 'total_paid_leads' in monthly.columns:
        monthly = monthly.with_columns(SAFE_DIV(pl.col('spending_net'), pl.col('total_paid_leads')).alias('cpl_paid'))
    if 'msg_active_consultations' in monthly.columns and 'msg_private_entrants' in monthly.columns:
        monthly = monthly.with_columns(SAFE_DIV(pl.col('msg_active_consultations'), pl.col('msg_private_entrants')).alias('private_msg_consult_ratio'))

    # Level normalization
    if dimension == '层级' and 'level' in group_keys:
        nsc_counts = (lf.group_by(['level','month']).agg(pl.col('nsc_code').n_unique().alias('level_nsc_count')))
        monthly = (monthly.join(nsc_counts, on=['level','month'], how='left'))
        # Apply normalization
        cols_to_normalize = [c for c in V10_NORMALIZATION_LIST if c in monthly.columns]
        if cols_to_normalize:
            monthly = monthly.with_columns([
                SAFE_DIV(pl.col(c), pl.col('level_nsc_count')).alias(c) for c in cols_to_normalize
            ])

    return monthly.collect(streaming=True).rechunk()