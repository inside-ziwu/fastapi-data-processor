"""Leads data transformation."""

import polars as pl
import logging
from typing import Any, Dict, List, Optional
from .base import BaseTransform
from .utils import _field_match
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
        # 严格映射：仅接受这三列，但允许列名存在空格/全角差异（规范化后精确匹配）
        import re
        logger = logging.getLogger(__name__)
        def _norm(s: str) -> str:
            s = (s or "")
            # 去除所有空白及零宽字符、NBSP
            s = re.sub(r"[\s\u200b\u200c\u200d\ufeff\u00a0]+", "", s)
            s = s.replace("（", "(").replace("）", ")").replace("：", ":")
            s = re.sub(r"[^\w\u4e00-\u9fa5:]+", "", s)
            return s.strip().lower()

        targets = {
            "主机厂经销商id列表": "NSC_CODE",
            "留资日期": "date",
            "直播间表单提交商机量(去重)": "small_wheel_leads",
        }

        norm_cols = {_norm(c): c for c in df.columns}
        # Probe: log original and normalized headers for diagnosis
        try:
            logger.warning(f"[leads probe] original columns: {df.columns}")
            logger.warning(f"[leads probe] normalized keys: {list(norm_cols.keys())}")
        except Exception:
            pass
        rename_map = {}
        missing = []
        for raw_name, out_name in targets.items():
            key = _norm(raw_name)
            if key in norm_cols:
                rename_map[norm_cols[key]] = out_name
            else:
                missing.append(raw_name)

        if missing:
            # extra probe: show nearest matches by substring
            nearest = {}
            for want in ["主机厂经销商id列表", "留资日期", "直播间表单提交商机量(去重)"]:
                wkey = _norm(want)
                hits = [orig for key, orig in norm_cols.items() if wkey in key or key in wkey]
                nearest[want] = hits
            logger.warning(f"[leads probe] nearest candidates: {nearest}")
            raise ValueError(f"leads 缺少必要列: {missing}")

        df = df.rename(rename_map)
        df = self._normalize_nsc_code(df)
        df = self._ensure_date_column(df, ["date", "留资日期", "日期"]) 

        df = self._cast_numeric_columns(df, self.sum_columns)
        # Extraction-only — 仅输出 NSC_CODE, date, 小风车留资量
        df = df.select(["NSC_CODE", "date", "small_wheel_leads"]) 
        return df

    # 严格口径：不做 NSC_CODE 的“智能兜底”
