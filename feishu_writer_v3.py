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
            
            # 调试信息：显示schema中的字段名
            logger.info(f"[飞书] Schema字段: {list(schema.keys())[:10]}...")
            logger.info(f"[飞书] FIELD_EN_MAP: {list(FIELD_EN_MAP.items())[:5]}...")
            
            # 构建标准化映射：处理空格和特殊字符
            def normalize_field_name(name: str) -> str:
                """更智能的标准化：处理所有可能的变体"""
                import re
                # 处理空格
                name = re.sub(r'\s+', '', name)
                # 处理括号
                name = name.replace('（', '(').replace('）', ')')
                # 处理斜杠差异：| -> /
                name = name.replace('|', '/')
                # 处理加号差异：➕ -> +
                name = name.replace('➕', '+')
                # 处理特殊字符
                name = re.sub(r'[^\w\(\)\+/\-]', '', name)
                return name
            
            # 创建schema的标准化索引
            normalized_schema = {normalize_field_name(k): (k, v) for k, v in schema.items()}
            
            # 创建反向映射：英文键 -> 中文字段信息
            matched_count = 0
            failed_mappings = []
            
            for cn_name, en_name in FIELD_EN_MAP.items():
                normalized_cn = normalize_field_name(cn_name)
                
                if normalized_cn in normalized_schema:
                    original_cn, field_info = normalized_schema[normalized_cn]
                    reverse_map[en_name] = field_info
                    matched_count += 1
                    logger.debug(f"[飞书] 映射成功: {en_name} -> {original_cn} -> {field_info['id']}")
                elif cn_name in schema:
                    # 直接匹配（兼容旧格式）
                    reverse_map[en_name] = schema[cn_name]
                    matched_count += 1
                    logger.debug(f"[飞书] 直接匹配: {en_name} -> {cn_name} -> {schema[cn_name]['id']}")
                else:
                    failed_mappings.append((cn_name, en_name))
                    logger.debug(f"[飞书] 未找到中文字段: {cn_name} (标准化: {normalized_cn})")
            
            # 报告映射结果
            logger.info(f"[飞书] 构建反向映射：共 {len(FIELD_EN_MAP)} 个映射，成功匹配 {matched_count} 个字段")
            
            if failed_mappings:
                logger.warning(f"[飞书] 映射失败的字段: {failed_mappings[:10]}")
                
                # 显示schema中可用的中文字段
                chinese_fields = [k for k in schema.keys() if any('\u4e00' <= c <= '\u9fff' for c in k)]
                logger.info(f"[飞书] 可用的中文字段: {chinese_fields}")
            
            # 显示前10个成功映射
            if reverse_map:
                sample = dict(list(reverse_map.items())[:10])
                logger.info(f"[飞书] 反向映射示例: {sample}")
            
            return reverse_map
            
        except ImportError:
            logger.warning("[飞书] 无法导入FIELD_EN_MAP，使用空映射")
            return {}

    def _process_value_by_type(self, value: Any, field_info: Dict[str, Any]) -> Any:
        """根据字段类型处理值 - 只做类型转换，不做清洗"""
        ui_type = field_info.get("ui_type", "")
        
        # 此时value已经是干净数据，只需类型转换
        if ui_type == "Text":
            return str(value)
        elif ui_type == "Number":
            return float(value)
        elif ui_type == "DateTime":
            if isinstance(value, int):
                return value
            dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
            return int(dt.timestamp() * 1000)
        elif ui_type == "Checkbox":
            return bool(value)
        elif ui_type == "SingleSelect":
            return {"id": str(value)}
        elif ui_type == "MultiSelect":
            return [{"id": str(v)} for v in value] if isinstance(value, list) else [{"id": str(value)}]
        else:
            return str(value)
            
    def _has_meaningful_value(self, v: Any) -> bool:
        """True if value is not None and not empty string after strip; 0 is meaningful."""
        if v is None:
            return False
        if isinstance(v, str) and v.strip() == "":
            return False
        return True
            
    async def write_records(self, records: List[Dict[str, Any]]) -> bool:
        """写入记录到飞书多维表格 - 只做写入，不做清洗"""
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
            
            # 🔍 映射验证探针：显示未映射的字段
            if records:
                first_record_keys = set(records[0].keys())
                mapped_keys = set(reverse_mapping.keys())
                unmapped_keys = first_record_keys - mapped_keys
                if unmapped_keys:
                    logger.warning(f"🔍 未映射字段: {list(unmapped_keys)[:5]}{'...' if len(unmapped_keys) > 5 else ''}")
                    logger.info(f"🔍 已映射字段示例: {dict(list(reverse_mapping.items())[:3])}")
            
            # 构建写入数据
            table_records = []
            
            for record in records:
                fields_data = {}
                
                for field_name, value in record.items():
                    if value is None:  # 干净数据，直接跳过None
                        continue
                        
                    field_info = reverse_mapping.get(field_name)
                    if field_info:
                        processed_value = self._process_value_by_type(value, field_info)
                        if processed_value is not None:
                            fields_data[field_info["id"]] = processed_value
                
                if fields_data:  # 只有有效数据才写入
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
                
                # 🔍 精简探针：只打印关键信息
                if len(batch) > 0:
                    first_record = batch[0]
                    if hasattr(first_record, '_fields') and first_record._fields:
                        field_summary = []
                        for field_id, value in first_record._fields.items():
                            # 反向查找字段名
                            field_name = None
                            for cn_name, info in schema.items():
                                if info.get('id') == field_id:
                                    field_name = cn_name
                                    break
                            field_summary.append(f"{field_name or field_id}({field_id})={value}[{type(value).__name__}]")
                        
                        logger.info(f"🔍 批次{i//batch_size + 1}样本: {', '.join(field_summary[:3])}{'...' if len(field_summary) > 3 else ''}")
                
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
                
                # 🔍 增强验证：显示详细的字段映射报告
                try:
                    from data_processor import FIELD_EN_MAP
                    
                    # 创建详细的映射报告
                    mapping_report = []
                    for cn_name, en_name in FIELD_EN_MAP.items():
                        if cn_name in schema:
                            field_info = schema[cn_name]
                            mapping_report.append({
                                "chinese": cn_name,
                                "english": en_name,
                                "field_id": field_info.get('id'),
                                "type": field_info.get('ui_type'),
                                "status": "✅ 匹配成功"
                            })
                        else:
                            mapping_report.append({
                                "chinese": cn_name,
                                "english": en_name,
                                "field_id": None,
                                "type": None,
                                "status": "❌ 未找到"
                            })
                    
                    result["mapping_report"] = mapping_report
                    result["mapping_success_rate"] = f"{len([m for m in mapping_report if m['status'] == '✅ 匹配成功'])}/{len(mapping_report)}"
                    
                    # 显示字段类型分析
                    field_types = {}
                    for cn_name, info in schema.items():
                        field_type = info.get('ui_type', 'unknown')
                        if field_type not in field_types:
                            field_types[field_type] = []
                        field_types[field_type].append(cn_name)
                    
                    result["field_types"] = {k: len(v) for k, v in field_types.items()}
                    
                    # 显示表格基本信息
                    logger.info(f"🔍 表格验证结果:")
                    logger.info(f"   表格ID: {self.table_id}")
                    logger.info(f"   总字段数: {len(schema)}")
                    logger.info(f"   映射成功率: {result['mapping_success_rate']}")
                    logger.info(f"   字段类型分布: {result['field_types']}")
                    
                    # 显示前10个未映射字段
                    missing_fields = [m for m in mapping_report if m['status'] == '❌ 未找到']
                    if missing_fields:
                        logger.warning(f"🔍 未映射字段(前10个):")
                        for field in missing_fields[:10]:
                            logger.warning(f"   {field['chinese']} -> {field['english']}")
                    
                except ImportError:
                    logger.warning("[飞书验证] 无法导入FIELD_EN_MAP用于验证")
                
        except Exception as e:
            result["valid"] = False
            result["errors"].append(str(e))
            
        return result