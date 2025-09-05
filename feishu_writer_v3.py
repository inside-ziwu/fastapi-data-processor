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
                
                request: BatchCreateAppTableRecordRequest = BatchCreateAppTableRecordRequest.builder() \
                    .app_token(self.app_token) \
                    .table_id(self.table_id) \
                    .request_body(BatchCreateAppTableRecordRequestBody.builder()
                        .records(batch)
                        .build()) \
                    .build()
                
                try:
                    response: BatchCreateAppTableRecordResponse = self.client.bitable.v1.app_table_record.batch_create(request)
                    
                    if response.success():
                        batch_success = len(response.data.records) if response.data else 0
                        total_success += batch_success
                        logger.info(f"[飞书] 批次 {i//batch_size + 1} 成功写入 {batch_success}/{len(batch)} 条记录")
                    else:
                        # 捕获飞书返回的详细错误信息
                        error_detail = {
                            "code": response.code,
                            "message": response.msg,
                            "request_id": getattr(response, 'request_id', 'unknown'),
                            "table_id": self.table_id,
                            "batch_size": len(batch)
                        }
                        
                        # 尝试获取更详细的错误信息
                        if hasattr(response, 'data') and response.data:
                            error_detail['data'] = str(response.data)
                        
                        logger.error(f"[飞书] 写入失败详情: {json.dumps(error_detail, ensure_ascii=False)}")
                        return False
                        
                except Exception as e:
                    # 捕获所有异常，包括网络错误、格式错误等
                    logger.error(f"[飞书] API调用异常: {type(e).__name__}: {str(e)}")
                    if hasattr(e, 'response'):
                        logger.error(f"[飞书] 响应内容: {e.response}")
                    return False
                    
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
                
                try:
                    from data_processor import FIELD_EN_MAP
                    
                    # 创建映射报告
                    mapping_report = []
                    for cn_name, en_name in FIELD_EN_MAP.items():
                        if cn_name in schema:
                            field_info = schema[cn_name]
                            mapping_report.append({
                                "chinese": cn_name,
                                "english": en_name,
                                "field_id": field_info.get('id'),
                                "type": field_info.get('ui_type'),
                                "status": "found"
                            })
                        else:
                            mapping_report.append({
                                "chinese": cn_name,
                                "english": en_name,
                                "field_id": None,
                                "type": None,
                                "status": "missing"
                            })
                    
                    result["mapping_report"] = mapping_report
                    found_count = len([m for m in mapping_report if m['status'] == "found"])
                    result["mapping_success_rate"] = f"{found_count}/{len(mapping_report)}"
                    
                    # 只记录关键信息
                    missing_count = len(mapping_report) - found_count
                    if missing_count > 0:
                        missing_examples = [m["english"] for m in mapping_report if m["status"] == "missing"][:3]
                        logger.warning(f"飞书字段映射: {missing_count}/{len(mapping_report)} 字段缺失, 示例: {missing_examples}")
                    
                except ImportError:
                    logger.warning("无法导入FIELD_EN_MAP用于验证")
                
        except Exception as e:
            result["valid"] = False
            result["errors"].append(str(e))
            
        return result