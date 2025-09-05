"""
Backward compatibility layer for data_processor.py

This module provides a compatibility interface for code that imports from
the old monolithic data_processor.py, redirecting to the new modular architecture.
"""

import warnings
import logging
from typing import Dict, List, Any, Optional
import polars as pl

# Configure logging for backward compatibility
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Import from new modular architecture
from src import DataProcessor
from src.config import FIELD_MAPPINGS

# Warn about deprecation
warnings.warn(
    "data_processor.py is deprecated. Use 'from src import DataProcessor' instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Create a singleton processor instance for backward compatibility
_processor = DataProcessor()

# Export FIELD_MAPPINGS for backward compatibility
FIELD_MAPPINGS = FIELD_MAPPINGS

# Legacy field mappings for backward compatibility
VIDEO_MAP = {
    "主机厂经销商id": "NSC_CODE",
    "日期": "date",
    "锚点曝光次数": "anchor_exposure",
    "锚点点击次数": "component_clicks",
    "新发布视频数": "short_video_count",
    "短视频表单提交商机量": "short_video_leads",
}
LIVE_MAP = {
    "主机厂经销商id列表": "NSC_CODE",
    "开播日期": "date",
    "超25分钟直播时长(分)": "over25_min_live_mins",
    "直播有效时长（小时）": "live_effective_hours",
    "超25min直播总场次": "effective_live_sessions",
    "曝光人数": "exposures",
    "场观": "viewers",
    "小风车点击次数（不含小雪花）": "small_wheel_clicks",
}
MSG_MAP = {
    "主机厂经销商ID": "NSC_CODE",
    "日期": "date",
    "进入私信客户数": "enter_private_count",
    "主动咨询客户数": "private_open_count",
    "私信留资客户数": "private_leads_count",
}
ACCOUNT_BI_MAP = {
    "主机厂经销商id列表": "NSC_CODE",
    "日期": "date",
    "直播间表单提交商机量": "live_leads",
    "短-播放量": "short_video_plays",
}
LEADS_MAP = {
    "主机厂经销商id列表": "NSC_CODE",
    "留资日期": "date",
    "直播间表单提交商机量(去重)": "small_wheel_leads",
}

SPENDING_MAP = {
    "NSC_CODE": "NSC_CODE",
    "date": "date",
    "总消耗": "total_spending",
    "表单提交量": "form_submissions",
    "表单提交成本": "form_cost",
}


def process_all_files(file_paths: Dict[str, str]) -> pl.DataFrame:
    """
    Process multiple data files and return unified DataFrame.

    This function maintains backward compatibility with the original interface.

    Args:
        file_paths: Dictionary mapping source names to file paths

    Returns:
        Unified DataFrame with all data joined by NSC_CODE and date
    """
    logger.info("Using backward compatibility interface for data_processor")
    return _processor.process_pipeline(file_paths)


# Legacy function names for maximum backward compatibility
def process_video_data(file_path: str) -> pl.DataFrame:
    """Process video data - legacy interface."""
    return _processor._process_single_source("video", file_path)


def process_live_data(file_path: str) -> pl.DataFrame:
    """Process live data - legacy interface."""
    return _processor._process_single_source("live", file_path)


def process_message_data(file_path: str) -> pl.DataFrame:
    """Process message data - legacy interface."""
    return _processor._process_single_source("msg", file_path)


__all__ = [
    "process_all_files",
    "process_video_data",
    "process_live_data",
    "process_message_data",
    "FIELD_MAPPINGS",
    "VIDEO_MAP",
    "LIVE_MAP",
    "MSG_MAP",
    "ACCOUNT_BI_MAP",
    "LEADS_MAP",
    "SPENDING_MAP",
]
