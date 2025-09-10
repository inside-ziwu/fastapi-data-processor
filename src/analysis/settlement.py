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

    # --- Start: Robust Derived Metrics Calculation ---
    # Ensure all base metric columns exist, filling with 0 if not.
    all_metric_cols = set(NUMERIC_METRICS)
    for col_name in all_metric_cols:
        if col_name not in monthly.columns:
            monthly = monthly.with_columns(pl.lit(0.0, dtype=pl.Float64).alias(col_name))

    # Now, calculate all derived metrics without checking for column existence.
    monthly = monthly.with_columns([
        (pl.col('natural_leads') + pl.col('paid_leads')).alias('total_leads'),
        (pl.col('cloud_store_paid_leads') + pl.col('regional_paid_leads')).alias('total_paid_leads'),
        SAFE_DIV(pl.col('spending_net'), pl.col('total_paid_leads')).alias('cpl_paid'),
        SAFE_DIV(pl.col('spending_net'), pl.col('total_leads')).alias('cpl_total'),
        SAFE_DIV(pl.col('local_leads'), pl.col('total_leads')).alias('local_leads_ratio'),
        SAFE_DIV(pl.col('spending_net'), pl.col('account_bi_live_form_leads')).alias('live_cpl'),
        SAFE_DIV(pl.col('live_views'), pl.col('live_exposures')).alias('exposure_to_view_ratio'),
        SAFE_DIV(pl.col('live_widget_clicks'), pl.col('live_views')).alias('widget_click_ratio'),
        SAFE_DIV(pl.col('leads_from_live_form'), pl.col('live_widget_clicks')).alias('widget_lead_ratio'),
        SAFE_DIV(pl.col('video_anchor_clicks'), pl.col('video_anchor_exposures')).alias('component_click_ratio'),
        SAFE_DIV(pl.col('video_form_leads'), pl.col('video_anchor_exposures')).alias('component_lead_ratio'),
        SAFE_DIV(pl.col('msg_active_consultations'), pl.col('msg_private_entrants')).alias('private_msg_consult_ratio'),
        SAFE_DIV(pl.col('msg_leads_from_private'), pl.col('msg_active_consultations')).alias('consult_to_lead_ratio'),
        SAFE_DIV(pl.col('msg_leads_from_private'), pl.col('msg_private_entrants')).alias('private_msg_conversion_ratio'),
        SAFE_DIV(pl.col('live_exposures'), pl.col('live_gt_25min_sessions')).alias('avg_exposure_per_session'),
        SAFE_DIV(pl.col('live_views'), pl.col('live_gt_25min_sessions')).alias('avg_view_per_session'),
        SAFE_DIV(pl.col('leads_from_live_form'), pl.col('live_gt_25min_sessions')).alias('avg_widget_leads_per_session'),
        SAFE_DIV(pl.col('live_widget_clicks'), pl.col('live_gt_25min_sessions')).alias('avg_widget_clicks_per_session'),
        SAFE_DIV(pl.col('spending_net'), pl.col('effective_days')).alias('avg_daily_spending'),
        SAFE_DIV(pl.col('live_effective_duration_hr'), pl.col('effective_days')).alias('avg_daily_effective_duration_hr'),
        SAFE_DIV(pl.col('msg_private_entrants'), pl.col('effective_days')).alias('avg_daily_private_entrants'),
        SAFE_DIV(pl.col('msg_active_consultations'), pl.col('effective_days')).alias('avg_daily_active_consultations'),
        SAFE_DIV(pl.col('msg_leads_from_private'), pl.col('effective_days')).alias('avg_daily_msg_leads'),
    ])
    # --- End: Robust Derived Metrics Calculation ---

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