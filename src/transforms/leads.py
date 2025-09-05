"""Leads data transformation."""

import polars as pl
from typing import Any, Dict, List, Optional
from .base import BaseTransform
from ..config import LEADS_MAP


class LeadsTransform(BaseTransform):
    """Transform leads data to standardized format."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.mapping = LEADS_MAP
        # Focus on small_wheel_leads as a key metric
        self.sum_columns: List[str] = ["small_wheel_leads"]

    def get_required_columns(self) -> List[str]:
        return list(self.mapping.keys())

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        # 1) 先做显式、可预测的直改，避免模糊匹配带来的意外
        direct_keys = {"主机厂经销商id列表": "NSC_CODE", "留资日期": "date"}
        present_direct = {k: v for k, v in direct_keys.items() if k in df.columns}
        if present_direct:
            df = df.rename(present_direct)

        # 2) 如果仍然没有 NSC_CODE，在本 Transform 内做“智能”兜底
        #    —— 业务相关逻辑应该放在具体 Transform，而不是通用工具函数
        if "NSC_CODE" not in df.columns:
            nsc_col = self._find_nsc_column_in_messy_data(df.columns)
            if nsc_col:
                df = df.rename({nsc_col: "NSC_CODE"})

        df = self._rename_columns(df, self.mapping)
        df = self._normalize_nsc_code(df)
        df = self._ensure_date_column(df, ["date", "留资日期", "日期"]) 

        df = self._cast_numeric_columns(df, self.sum_columns)
        # Extraction-only — no aggregation
        wanted = ["NSC_CODE", "date"] + self.sum_columns
        present = [c for c in wanted if c in df.columns]
        df = df.select(present)
        return df

    def _find_nsc_column_in_messy_data(self, columns: list[str]) -> Optional[str]:
        """在列名混乱的情况下，尝试定位 NSC_CODE 对应列（仅限 leads 领域）。

        优先匹配更具体/更长的命名，避免误伤。返回命中列名或 None。
        """
        # 有序候选，从最具体到较宽泛
        candidates = (
            "主机厂经销商id列表",
            "经销商id列表",
            "主机厂经销商id",
            "经销商id",
            "NSC Code",
            "NSC_code",
            "NSC",
        )

        norm_map = {c.replace(" ", "").lower(): c for c in columns}
        for pat in candidates:
            key = pat.replace(" ", "").lower()
            # 精确或包含匹配（包含更谨慎，避免短 token 误命中）
            if key in norm_map:
                return norm_map[key]
            for k_norm, original in norm_map.items():
                if key in k_norm:
                    return original
        return None
