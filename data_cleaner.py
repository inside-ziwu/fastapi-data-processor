"""
数据清洗器 - 单一职责原则
只做一件事：把脏数据变成干净数据
"""

import logging
from typing import Any, Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class DataCleaner:
    """数据清洗器 - 在数据进入任何系统之前完成清洗"""
    
    @staticmethod
    def clean_value(value: Any) -> Optional[str]:
        """统一清洗字符串值"""
        if value is None:
            return None
        if isinstance(value, str):
            value = value.strip()
            return value if value else None
        return str(value).strip() or None
    
    @staticmethod
    def clean_number(value: Any) -> Optional[float]:
        """清洗数字值 - 明确拒绝非数字"""
        if value is None:
            return None
        
        if isinstance(value, (int, float)):
            return float(value)
        
        if isinstance(value, str):
            value = value.strip().replace(',', '')
            try:
                return float(value)
            except ValueError:
                return None
        
        return None
    
    @staticmethod
    def clean_text(value: Any) -> Optional[str]:
        """清洗文本值 - 只处理字符串，其他类型返回None"""
        if value is None:
            return None
        if isinstance(value, str):
            value = value.strip()
            return value if value else None
        return None
    
    @staticmethod
    def clean_level_data(record: Dict[str, Any]) -> Dict[str, Any]:
        """清洗Level维度表数据"""
        cleaned = {}
        
        # 必填字段
        level = DataCleaner.clean_text(record.get('level', ''))
        if not level:
            logger.error("Level维度表缺少必填字段: level")
            return {}
        
        cleaned['level'] = level
        
        # 数值字段
        numeric_fields = [
            'natural_leads_total', 'ad_leads_total', 'spending_net_total',
            'avg_daily_spending', 'avg_daily_spending_t', 'avg_daily_spending_t_minus_1',
            # ... 其他数值字段
        ]
        
        for field in numeric_fields:
            value = record.get(field)
            cleaned[field] = DataCleaner.clean_number(value)
        
        return cleaned
    
    @staticmethod
    def is_valid_level_record(record: Dict[str, Any]) -> bool:
        """验证Level记录是否有效"""
        return bool(record.get('level'))