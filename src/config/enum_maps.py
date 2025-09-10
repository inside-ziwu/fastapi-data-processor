"""Enum mappings for standardizing categorical data."""

LEADS_TYPE_MAP = {
    "自然": "NATURAL",
    "广告": "PAID",
    "自然线索": "NATURAL",
    "付费": "PAID"
}

# This is a placeholder. The actual channel names should be confirmed from data.
CHANNEL_MAP_WHITELIST = {
    "抖音车云店_BMW_本市_LS直发": "CLOUD_LOCAL",
    "抖音车云店_LS直发": "CLOUD_LOCAL",
    "抖音车云店_BMW_总部BDT_LS直发": "REGIONAL"
}
