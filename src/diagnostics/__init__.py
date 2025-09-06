"""Diagnostics package: optional, opt-in via env switches."""

from .metrics import (
    log_settlement_inputs,
    log_level_distribution,
    log_suffix_masking,
    log_account_base_conflicts,
)

__all__ = [
    "log_settlement_inputs",
    "log_level_distribution",
    "log_suffix_masking",
    "log_account_base_conflicts",
]
