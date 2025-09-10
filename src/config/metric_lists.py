"""Lists of metrics for consistent processing."""

# All numeric metrics that should be filled with 0.0 if null after join
NUMERIC_METRICS = {
    "natural_leads", "paid_leads", "local_leads", "cloud_store_paid_leads", 
    "regional_paid_leads", "total_leads", "total_paid_leads", 
    "msg_private_entrants", "msg_active_consultations", "msg_leads_from_private",
    "video_anchor_exposures", "video_anchor_clicks", "video_new_posts", "video_form_leads",
    "live_gt_25min_duration_min", "live_effective_duration_hr", "live_gt_25min_sessions",
    "live_exposures", "live_views", "live_widget_clicks", "leads_from_live_form",
    "account_bi_live_form_leads", "account_bi_video_views", "spending_net"
}

# Data types for metrics to enforce schema consistency
import polars as pl
METRIC_DTYPES = {m: pl.Float64 for m in NUMERIC_METRICS}

# Whitelist for level normalization (average per store)
V10_NORMALIZATION_LIST = [
    "natural_leads", "paid_leads", "local_leads", "cloud_store_paid_leads", 
    "regional_paid_leads", "total_leads", "total_paid_leads", 
    "msg_private_entrants", "msg_active_consultations", "msg_leads_from_private",
    "video_anchor_exposures", "video_anchor_clicks", "video_new_posts", "video_form_leads",
    "live_gt_25min_duration_min", "live_effective_duration_hr", "live_gt_25min_sessions",
    "live_exposures", "live_views", "live_widget_clicks", "leads_from_live_form",
    "account_bi_live_form_leads", "account_bi_video_views", "spending_net",
    "avg_daily_spending", "avg_daily_private_entrants", "avg_daily_active_consultations",
    "avg_daily_msg_leads", "avg_daily_effective_duration_hr"
]
