#!/usr/bin/env python3
"""
飞书写入器 v3 - 简化版本
直接使用传入的table_id，无需维度配置
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

try:
    import lark_oapi as lark
    from lark_oapi.api.bitable.v1 import *
except ImportError:
    # 提供兼容性提示
    raise ImportError("需要安装飞书SDK: pip install lark-oapi")

logger = logging.getLogger(__name__)


class FeishuWriterV3:
    """简化版飞书写入器 - 直接使用table_id"""
    
    def __init__(self, config: dict):
        """
        初始化飞书写入器
        
        Args:
            config: 配置字典，必须包含table_id
        """
        self.enabled = config.get("enabled", False)
        self.app_id = config.get("app_id", "")
        self.app_secret = config.get("app_secret", "")
        self.app_token = config.get("app_token", "")
        self.table_id = config.get("table_id", "")
        
        # 创建client
        self.client = lark.Client.builder() \
            .app_id(self.app_id) \
            .app_secret(self.app_secret) \
            .log_level(lark.LogLevel.INFO) \
            .build()
            
        # 缓存字段映射：field_name -> field_info
        self._field_cache: Dict[str, Dict[str, Any]] = {}
        
        # 反向映射缓存：英文键 -> 中文字段信息
        self._reverse_mapping_cache: Dict[str, Dict[str, Any]] = {}
        
        # 验证配置
        if not all([self.app_id, self.app_secret, self.app_token, self.table_id]):
            self.enabled = False
            logger.warning("[飞书] 配置不完整，写入功能已禁用。")
            
    async def get_table_schema(self) -> Dict[str, Dict[str, Any]]:
        """获取表格的字段schema"""
        if not self.enabled:
            logger.warning("[飞书] 功能未启用，无法获取schema。")
            return {}
            
        if self._field_cache:
            return self._field_cache
            
        try:
            request: ListAppTableFieldRequest = ListAppTableFieldRequest.builder() \
                .app_token(self.app_token) \
                .table_id(self.table_id) \
                .page_size(200) \
                .build()
                
            response: ListAppTableFieldResponse = self.client.bitable.v1.app_table_field.list(request)
            
            if not response.success():
                logger.error(f"[飞书] 获取字段列表失败，code: {response.code}, msg: {response.msg}")
                return {}
                
            # 解析字段信息
            schema = {}
            if response.data and response.data.items:
                for field in response.data.items:
                    schema[field.field_name] = {
                        "id": field.field_id,
                        "type": field.type,
                        "ui_type": field.ui_type,
                        "property": field.property or {}
                    }
                    
            self._field_cache = schema
            logger.info(f"[飞书] 获取到 {len(schema)} 个字段")
            return schema
            
        except Exception as e:
            logger.error(f"[飞书] 获取schema时发生错误: {e}")
            return {}
            
    def _build_reverse_mapping(self, schema: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """构建英文到中文的反向映射"""
        if not schema:
            return {}
            
        # 从data_processor.py导入映射表
        try:
            from data_processor import FIELD_EN_MAP
            reverse_map = {}
            
            # 创建反向映射：英文键 -> 中文字段信息
            for cn_name, en_name in FIELD_EN_MAP.items():
                if cn_name in schema:
                    reverse_map[en_name] = schema[cn_name]
                    
            logger.info(f"[飞书] 构建反向映射：{len(reverse_map)} 个字段")
            return reverse_map
            
        except ImportError:
            logger.warning("[飞书] 无法导入FIELD_EN_MAP，使用空映射")
            return {}

    def _process_value_by_type(self, value: Any, field_info: Dict[str, Any]) -> Any:
        """根据字段类型处理值"""
        if value is None or value == "":
            return None
            
        ui_type = field_info.get("ui_type", "")
        
        try:
            # 文本类型
            if ui_type == "Text":
                return str(value)
                
            # 数值类型
            elif ui_type == "Number":
                return float(value)
                
            # 日期类型
            elif ui_type == "DateTime":
                if isinstance(value, int):
                    return value
                elif isinstance(value, str):
                    dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    return int(dt.timestamp() * 1000)
                else:
                    return value
                    
            # 布尔类型
            elif ui_type == "Checkbox":
                return bool(value)
                
            # 单选类型
            elif ui_type == "SingleSelect":
                options = field_info.get("property", {}).get("options", [])
                for option in options:
                    if str(option.get("name")) == str(value):
                        return {"id": option.get("id")}
                return {"id": str(value)}
                
            # 多选类型
            elif ui_type == "MultiSelect":
                values = value if isinstance(value, list) else [value]
                result = []
                options = field_info.get("property", {}).get("options", [])
                option_map = {opt.get("name"): opt.get("id") for opt in options}
                
                for val in values:
                    val_str = str(val)
                    option_id = option_map.get(val_str, val_str)
                    result.append({"id": option_id})
                return result
                
            # 其他类型
            else:
                return str(value)
                
        except Exception as e:
            logger.warning(f"[飞书] 值处理失败: {value} -> {e}")
            return str(value)
            
    def _has_meaningful_value(self, v: Any) -> bool:
        """True if value is not None and not empty string after strip; 0 is meaningful."""
        if v is None:
            return False
        if isinstance(v, str) and v.strip() == "":
            return False
        return True
            
    async def write_records(self, records: List[Dict[str, Any]]) -> bool:
        """写入记录到飞书多维表格"""
        if not self.enabled:
            logger.info("[飞书] 写入未启用，跳过写入。")
            return True
            
        if not records:
            logger.info("[飞书] 没有记录可写入。")
            return True
            
        try:
            logger.info(f"[飞书] 开始写入，共 {len(records)} 条记录")
            
            # 获取表格schema
            schema = await self.get_table_schema()
            if not schema:
                logger.error("[飞书] 无法获取表格schema")
                return False
                
            # 构建反向映射
            reverse_mapping = self._build_reverse_mapping(schema)
            logger.info(f"[飞书] 反向映射：{list(reverse_mapping.keys())[:10]}...")
            
            # 构建写入数据 - 使用反向映射
            table_records = []
            
            for record_idx, record in enumerate(records):
                fields_data = {}
                
                for field_name, value in record.items():
                    if not self._has_meaningful_value(value):
                        continue
                        
                    # 使用反向映射查找中文字段
                    field_info = reverse_mapping.get(field_name)
                    if field_info:
                        processed_value = self._process_value_by_type(value, field_info)
                        if processed_value is not None:
                            fields_data[field_info["id"]] = processed_value
                    else:
                        # 尝试直接匹配中文字段名
                        direct_match = schema.get(field_name)
                        if direct_match:
                            processed_value = self._process_value_by_type(value, direct_match)
                            if processed_value is not None:
                                fields_data[direct_match["id"]] = processed_value
                        else:
                            logger.debug(f"[飞书] 字段 '{field_name}' 不存在，跳过")
                        
                if fields_data:
                    table_records.append(
                        AppTableRecord.builder()
                        .fields(fields_data)
                        .build()
                    )
                    
            if not table_records:
                logger.warning("[飞书] 没有有效记录可以写入")
                return False
                
            # 分批处理
            batch_size = 500
            total_success = 0
            total_total = len(table_records)
            
            for i in range(0, len(table_records), batch_size):
                batch = table_records[i:i+batch_size]
                
                request: BatchCreateAppTableRecordRequest = BatchCreateAppTableRecordRequest.builder() \
                    .app_token(self.app_token) \
                    .table_id(self.table_id) \
                    .request_body(BatchCreateAppTableRecordRequestBody.builder()
                        .records(batch)
                        .build()) \
                    .build()
                    
                response: BatchCreateAppTableRecordResponse = self.client.bitable.v1.app_table_record.batch_create(request)
                
                if response.success():
                    batch_success = len(response.data.records) if response.data else 0
                    total_success += batch_success
                    logger.info(f"[飞书] 批次 {i//batch_size + 1} 成功写入 {batch_success}/{len(batch)} 条记录")
                else:
                    logger.error(f"[飞书] 批次 {i//batch_size + 1} 写入失败: code={response.code}, msg={response.msg}")
                    
            logger.info(f"[飞书] 写入完成: 成功 {total_success}/{total_total} 条记录")
            return total_success == total_total
            
        except Exception as e:
            logger.error(f"[飞书] 写入时发生错误: {e}")
            return False
            
    async def validate_config(self) -> Dict[str, Any]:
        """验证配置和表格结构"""
        result = {
            "enabled": self.enabled,
            "table_id": self.table_id,
            "valid": True,
            "errors": []
        }
        
        if not self.enabled:
            result["valid"] = False
            result["errors"].append("飞书功能未启用")
            return result
            
        if not all([self.app_id, self.app_secret, self.app_token, self.table_id]):
            result["valid"] = False
            result["errors"].append("飞书配置不完整")
            return result
            
        try:
            schema = await self.get_table_schema()
            if not schema:
                result["valid"] = False
                result["errors"].append("无法获取表格schema")
            else:
                result["field_count"] = len(schema)
                result["fields"] = list(schema.keys())
                
                # 测试反向映射
                reverse_mapping = self._build_reverse_mapping(schema)
                result["mapped_fields"] = len(reverse_mapping)
                result["sample_mapping"] = dict(list(reverse_mapping.items())[:5])
                
        except Exception as e:
            result["valid"] = False
            result["errors"].append(str(e))
            
        return result