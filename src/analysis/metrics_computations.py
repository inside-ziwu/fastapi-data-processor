"""Comprehensive metrics computations migrated from original data processor."""

from typing import Any, Dict, List
import polars as pl
from .computations import BaseComputation


def safe_div(num_expr, den_expr):
    """Safe division with null handling."""
    # Handle both Polars expressions and literal values
    num_expr = (
        pl.lit(num_expr)
        if not hasattr(num_expr, "is_not_null")
        else num_expr
    )
    den_expr = (
        pl.lit(den_expr)
        if not hasattr(den_expr, "is_not_null")
        else den_expr
    )

    return (
        pl.when(
            (den_expr != 0)
            & den_expr.is_not_null()
            & num_expr.is_not_null()
        )
        .then(num_expr / den_expr)
        .otherwise(None)
    )


class IntermediateMetricsComputation(BaseComputation):
    """Compute intermediate derived metrics."""

    def __init__(self):
        super().__init__("intermediate_metrics")

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add intermediate derived metrics."""
        return df.with_columns(
            (
                pl.col("paid_leads_total").fill_null(0)
                + pl.col("area_leads_total").fill_null(0)
            ).alias("paid_area_leads_total"),
            (
                pl.col("paid_leads_t").fill_null(0)
                + pl.col("area_leads_t").fill_null(0)
            ).alias("paid_area_leads_t"),
            (
                pl.col("paid_leads_t_minus_1").fill_null(0)
                + pl.col("area_leads_t_minus_1").fill_null(0)
            ).alias("paid_area_leads_t_minus_1"),
            (pl.col("over25_min_live_mins_total") / 60).alias(
                "effective_live_hours_25min"
            ),
            (pl.col("over25_min_live_mins_t") / 60).alias(
                "effective_live_hours_25min_t"
            ),
            (pl.col("over25_min_live_mins_t_minus_1") / 60).alias(
                "effective_live_hours_25min_t_minus_1"
            ),
        )

    def compute(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Compute summary statistics for intermediate metrics."""
        if df.height == 0:
            return {}

        return {
            "total_paid_area_leads": df["paid_area_leads_total"].sum(),
            "total_effective_live_hours_25min": df["effective_live_hours_25min"].sum(),
        }


class CPLMetricsComputation(BaseComputation):
    """Compute comprehensive CPL metrics."""

    def __init__(self):
        super().__init__("cpl_metrics")

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add CPL-related metrics."""
        return df.with_columns(
            safe_div(
                pl.col("spending_net_total"),
                (
                    pl.col("natural_leads_total").fill_null(0)
                    + pl.col("ad_leads_total").fill_null(0)
                ),
            ).alias("total_cpl"),
            safe_div(
                pl.col("spending_net_total"), pl.col("paid_area_leads_total")
            ).alias("paid_cpl"),
            safe_div(pl.col("spending_net_t"), pl.col("paid_area_leads_t")).alias(
                "paid_cpl_t"
            ),
            safe_div(
                pl.col("spending_net_t_minus_1"),
                pl.col("paid_area_leads_t_minus_1"),
            ).alias("paid_cpl_t_minus_1"),
        )

    def compute(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Compute overall CPL statistics."""
        if df.height == 0:
            return {}

        return {
            "avg_total_cpl": df["total_cpl"].mean(),
            "avg_paid_cpl": df["paid_cpl"].mean(),
            "avg_paid_cpl_t": df["paid_cpl_t"].mean(),
        }


class DailyAveragesComputation(BaseComputation):
    """Compute daily average metrics."""

    def __init__(self, t_days: int = 30, t_minus_1_days: int = 30):
        super().__init__("daily_averages")
        self.t_days = t_days
        self.t_minus_1_days = t_minus_1_days

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add daily average metrics."""
        total_days = self.t_days + self.t_minus_1_days
        
        return df.with_columns(
            safe_div(
                pl.col("spending_net_total"), total_days
            ).alias("avg_daily_spending"),
            safe_div(pl.col("spending_net_t"), self.t_days).alias(
                "avg_daily_spending_t"
            ),
            safe_div(pl.col("spending_net_t_minus_1"), self.t_minus_1_days).alias(
                "avg_daily_spending_t_minus_1"
            ),
            safe_div(
                pl.col("paid_area_leads_total"), total_days
            ).alias("avg_daily_paid_leads"),
            safe_div(pl.col("paid_area_leads_t"), self.t_days).alias(
                "avg_daily_paid_leads_t"
            ),
            safe_div(pl.col("paid_area_leads_t_minus_1"), self.t_minus_1_days).alias(
                "avg_daily_paid_leads_t_minus_1"
            ),
            safe_div(
                pl.col("effective_live_hours_25min"), total_days
            ).alias("avg_daily_effective_live_hours_25min"),
            safe_div(pl.col("effective_live_hours_25min_t"), self.t_days).alias(
                "avg_daily_effective_live_hours_25min_t"
            ),
            safe_div(
                pl.col("effective_live_hours_25min_t_minus_1"), self.t_minus_1_days
            ).alias("avg_daily_effective_live_hours_25min_t_minus_1"),
        )

    def compute(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Compute daily average summaries."""
        if df.height == 0:
            return {}

        return {
            "avg_daily_spending": df["avg_daily_spending"].mean(),
            "avg_daily_paid_leads": df["avg_daily_paid_leads"].mean(),
            "avg_daily_effective_live_hours": df["avg_daily_effective_live_hours_25min"].mean(),
        }


class LiveMetricsComputation(BaseComputation):
    """Compute live streaming specific metrics."""

    def __init__(self):
        super().__init__("live_metrics")

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add live streaming metrics."""
        return df.with_columns(
            safe_div(
                pl.col("exposures_total"), pl.col("effective_live_sessions_total")
            ).alias("avg_exposures_per_session"),
            safe_div(
                pl.col("exposures_t"), pl.col("effective_live_sessions_t")
            ).alias("avg_exposures_per_session_t"),
            safe_div(
                pl.col("exposures_t_minus_1"),
                pl.col("effective_live_sessions_t_minus_1"),
            ).alias("avg_exposures_per_session_t_minus_1"),
            safe_div(pl.col("viewers_total"), pl.col("exposures_total")).alias(
                "exposure_to_viewer_rate"
            ),
            safe_div(pl.col("viewers_t"), pl.col("exposures_t")).alias(
                "exposure_to_viewer_rate_t"
            ),
            safe_div(
                pl.col("viewers_t_minus_1"), pl.col("exposures_t_minus_1")
            ).alias("exposure_to_viewer_rate_t_minus_1"),
            safe_div(
                pl.col("viewers_total"), pl.col("effective_live_sessions_total")
            ).alias("avg_viewers_per_session"),
            safe_div(
                pl.col("viewers_t"), pl.col("effective_live_sessions_t")
            ).alias("avg_viewers_per_session_t"),
            safe_div(
                pl.col("viewers_t_minus_1"),
                pl.col("effective_live_sessions_t_minus_1"),
            ).alias("avg_viewers_per_session_t_minus_1"),
            safe_div(
                pl.col("small_wheel_clicks_total"), pl.col("viewers_total")
            ).alias("small_wheel_click_rate"),
            safe_div(pl.col("small_wheel_clicks_t"), pl.col("viewers_t")).alias(
                "small_wheel_click_rate_t"
            ),
            safe_div(
                pl.col("small_wheel_clicks_t_minus_1"), pl.col("viewers_t_minus_1")
            ).alias("small_wheel_click_rate_t_minus_1"),
        )

    def compute(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Compute live metrics summaries."""
        if df.height == 0:
            return {}

        return {
            "avg_exposures_per_session": df["avg_exposures_per_session"].mean(),
            "avg_exposure_to_viewer_rate": df["exposure_to_viewer_rate"].mean(),
            "avg_viewers_per_session": df["avg_viewers_per_session"].mean(),
            "avg_small_wheel_click_rate": df["small_wheel_click_rate"].mean(),
        }


class ConversionRatesComputation(BaseComputation):
    """Compute conversion rate metrics."""

    def __init__(self):
        super().__init__("conversion_rates")

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add conversion rate metrics."""
        return df.with_columns(
            safe_div(
                pl.col("small_wheel_leads_total"),
                pl.col("small_wheel_clicks_total"),
            ).alias("small_wheel_leads_rate"),
            safe_div(
                pl.col("small_wheel_leads_t"), pl.col("small_wheel_clicks_t")
            ).alias("small_wheel_leads_rate_t"),
            safe_div(
                pl.col("small_wheel_leads_t_minus_1"),
                pl.col("small_wheel_clicks_t_minus_1"),
            ).alias("small_wheel_leads_rate_t_minus_1"),
            safe_div(
                pl.col("small_wheel_leads_total"),
                pl.col("effective_live_sessions_total"),
            ).alias("avg_small_wheel_leads_per_session"),
            safe_div(
                pl.col("small_wheel_leads_t"), pl.col("effective_live_sessions_t")
            ).alias("avg_small_wheel_leads_per_session_t"),
            safe_div(
                pl.col("small_wheel_leads_t_minus_1"),
                pl.col("effective_live_sessions_t_minus_1"),
            ).alias("avg_small_wheel_leads_per_session_t_minus_1"),
            safe_div(
                pl.col("small_wheel_clicks_total"),
                pl.col("effective_live_sessions_total"),
            ).alias("avg_small_wheel_clicks_per_session"),
            safe_div(
                pl.col("small_wheel_clicks_t"), pl.col("effective_live_sessions_t")
            ).alias("avg_small_wheel_clicks_per_session_t"),
            safe_div(
                pl.col("small_wheel_clicks_t_minus_1"),
                pl.col("effective_live_sessions_t_minus_1"),
            ).alias("avg_small_wheel_clicks_per_session_t_minus_1"),
        )

    def compute(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Compute conversion rate summaries."""
        if df.height == 0:
            return {}

        return {
            "avg_small_wheel_leads_rate": df["small_wheel_leads_rate"].mean(),
            "avg_small_wheel_leads_per_session": df["avg_small_wheel_leads_per_session"].mean(),
            "avg_small_wheel_clicks_per_session": df["avg_small_wheel_clicks_per_session"].mean(),
        }


class VideoMetricsComputation(BaseComputation):
    """Compute video/anchor metrics."""

    def __init__(self):
        super().__init__("video_metrics")

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add video/anchor metrics."""
        return df.with_columns(
            safe_div(
                pl.col("component_clicks_total"), pl.col("anchor_exposure_total")
            ).alias("component_click_rate"),
            safe_div(
                pl.col("component_clicks_t"), pl.col("anchor_exposure_t")
            ).alias("component_click_rate_t"),
            safe_div(
                pl.col("component_clicks_t_minus_1"),
                pl.col("anchor_exposure_t_minus_1"),
            ).alias("component_click_rate_t_minus_1"),
            safe_div(
                pl.col("short_video_leads_total"), pl.col("anchor_exposure_total")
            ).alias("component_leads_rate"),
            safe_div(
                pl.col("short_video_leads_t"), pl.col("anchor_exposure_t")
            ).alias("component_leads_rate_t"),
            safe_div(
                pl.col("short_video_leads_t_minus_1"),
                pl.col("anchor_exposure_t_minus_1"),
            ).alias("component_leads_rate_t_minus_1"),
        )

    def compute(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Compute video metrics summaries."""
        if df.height == 0:
            return {}

        return {
            "avg_component_click_rate": df["component_click_rate"].mean(),
            "avg_component_leads_rate": df["component_leads_rate"].mean(),
        }


class LocalMetricsComputation(BaseComputation):
    """Compute local leads ratio metrics."""

    def __init__(self):
        super().__init__("local_metrics")

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add local leads ratio metrics."""
        return df.with_columns(
            safe_div(
                pl.col("local_leads_total"),
                (
                    pl.col("natural_leads_total").fill_null(0)
                    + pl.col("ad_leads_total").fill_null(0)
                ),
            ).alias("local_leads_ratio"),
        )

    def compute(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Compute local metrics summaries."""
        if df.height == 0:
            return {}

        return {
            "avg_local_leads_ratio": df["local_leads_ratio"].mean(),
        }


class PrivateMessageMetricsComputation(BaseComputation):
    """Compute private message/chat metrics."""

    def __init__(self):
        super().__init__("private_message_metrics")

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add private message metrics."""
        return df.with_columns(
            safe_div(
                pl.col("enter_private_count_total"), (30 + 30)  # T + T-1 days
            ).alias("avg_daily_private_entry_count"),
            safe_div(pl.col("enter_private_count_t"), 30).alias(
                "avg_daily_private_entry_count_t"
            ),
            safe_div(
                pl.col("enter_private_count_t_minus_1"), 30
            ).alias("avg_daily_private_entry_count_t_minus_1"),
            safe_div(
                pl.col("private_open_count_total"), (30 + 30)
            ).alias("avg_daily_private_open_count"),
            safe_div(pl.col("private_open_count_t"), 30).alias(
                "avg_daily_private_open_count_t"
            ),
            safe_div(pl.col("private_open_count_t_minus_1"), 30).alias(
                "avg_daily_private_open_count_t_minus_1"
            ),
            safe_div(
                pl.col("private_leads_count_total"), (30 + 30)
            ).alias("avg_daily_private_leads_count"),
            safe_div(pl.col("private_leads_count_t"), 30).alias(
                "avg_daily_private_leads_count_t"
            ),
            safe_div(
                pl.col("private_leads_count_t_minus_1"), 30
            ).alias("avg_daily_private_leads_count_t_minus_1"),
            safe_div(
                pl.col("private_open_count_total"),
                pl.col("enter_private_count_total"),
            ).alias("private_open_rate"),
            safe_div(
                pl.col("private_open_count_t"), pl.col("enter_private_count_t")
            ).alias("private_open_rate_t"),
            safe_div(
                pl.col("private_open_count_t_minus_1"),
                pl.col("enter_private_count_t_minus_1"),
            ).alias("private_open_rate_t_minus_1"),
            safe_div(
                pl.col("private_leads_count_total"),
                pl.col("private_open_count_total"),
            ).alias("private_leads_rate"),
            safe_div(
                pl.col("private_leads_count_t"), pl.col("private_open_count_t")
            ).alias("private_leads_rate_t"),
            safe_div(
                pl.col("private_leads_count_t_minus_1"),
                pl.col("private_open_count_t_minus_1"),
            ).alias("private_leads_rate_t_minus_1"),
            safe_div(
                pl.col("private_leads_count_total"),
                pl.col("enter_private_count_total"),
            ).alias("private_conversion_rate"),
            safe_div(
                pl.col("private_leads_count_t"), pl.col("enter_private_count_t")
            ).alias("private_conversion_rate_t"),
            safe_div(
                pl.col("private_leads_count_t_minus_1"),
                pl.col("enter_private_count_t_minus_1"),
            ).alias("private_conversion_rate_t_minus_1"),
        )

    def compute(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Compute private message summaries."""
        if df.height == 0:
            return {}

        return {
            "avg_private_open_rate": df["private_open_rate"].mean(),
            "avg_private_leads_rate": df["private_leads_rate"].mean(),
            "avg_private_conversion_rate": df["private_conversion_rate"].mean(),
        }