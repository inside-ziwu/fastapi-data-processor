"""
Synchronous Feishu writer - replaces async version with proper sync implementation.

This eliminates the fake async pattern where sync SDK calls were wrapped in async methods.
"""

import logging
import json
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# Optional import with fallback
try:
    import lark
    from lark_oapi.api.bitable.v1 import *
    SDK_AVAILABLE = True
except ImportError as e:
    SDK_AVAILABLE = False
    logger.warning(
        f"飞书SDK导入失败，Feishu功能将不可用。错误: {e}。安装: pip install lark-oapi"
    )


class FeishuWriterSync:
    """Synchronous Feishu writer - no fake async."""

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

        # 检查SDK是否可用
        if not SDK_AVAILABLE:
            logger.warning("飞书SDK未安装，FeishuWriterSync将不可用")
            self.client = None
        else:
            # 创建client
            self.client = (
                lark.Client.builder()
                .app_id(self.app_id)
                .app_secret(self.app_secret)
                .log_level(lark.LogLevel.INFO)
                .build()
            )

        # 缓存字段映射：field_name -> field_info
        self._field_cache: Dict[str, Dict[str, Any]] = {}

        # 反向映射缓存：英文键 -> 中文字段信息
        self._reverse_mapping_cache: Dict[str, Dict[str, Any]] = {}

        # 验证配置
        if not all(
            [self.app_id, self.app_secret, self.app_token, self.table_id]
        ):
            self.enabled = False
            logger.warning("[飞书] 配置不完整，写入功能已禁用。")

    def get_table_schema(self) -> Dict[str, Dict[str, Any]]:
        """获取表格的字段schema - 同步版本"""
        if not self.enabled:
            logger.warning("[飞书] 功能未启用，无法获取schema。")
            return {}

        if not SDK_AVAILABLE:
            logger.error("[飞书] SDK未安装，无法获取schema。")
            return {}

        if self._field_cache:
            return self._field_cache

        try:
            # 同步调用，不是async
            request: ListAppTableFieldRequest = (
                ListAppTableFieldRequest.builder()
                .app_token(self.app_token)
                .table_id(self.table_id)
                .page_size(200)
                .build()
            )

            response: ListAppTableFieldResponse = (
                self.client.bitable.v1.app_table_field.list(request)
            )

            if not response.success():
                logger.error(
                    f"[飞书] 获取字段列表失败，code: {response.code}, msg: {response.msg}"
                )
                return {}

            # 解析字段信息
            schema = {}
            if response.data and response.data.items:
                for field in response.data.items:
                    schema[field.field_name] = {
                        "id": field.field_id,
                        "type": field.type,
                        "ui_type": field.ui_type,
                        "property": field.property or {},
                    }

            self._field_cache = schema
            logger.info(f"[飞书] 获取到 {len(schema)} 个字段")
            return schema

        except Exception as e:
            logger.error(f"[飞书] 获取schema时发生错误: {e}")
            return {}

    def _build_reverse_mapping(
        self, schema: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """构建英文到中文的反向映射 - 统一处理一对一和一对多"""
        if not schema:
            return {}

        # 统一映射处理：一对一和一对多都是同一种逻辑
        try:
            from src.config import FIELD_MAPPINGS
        except ImportError:
            # Fallback for backward compatibility
            FIELD_MAPPINGS = {}

        reverse_map = {}

        # 字符标准化函数
        def normalize_field_name(name: str) -> str:
            """标准化字段名，处理字符差异"""
            import re

            # 处理空格
            name = re.sub(r"\s+", "", name)

            # 字符标准化映射
            char_mappings = {
                "（": "(",
                "）": ")",  # 括号
                "|": "/",  # 分隔符统一
                "➕": "+",  # 特殊加号
                "：": ":",  # 冒号
            }

            for old_char, new_char in char_mappings.items():
                name = name.replace(old_char, new_char)

            # 删除无用字符，保留有用的
            name = re.sub(r"[^\w\(\)/+:]", "", name)
            return name

        # 创建schema标准化索引
        normalized_schema = {
            normalize_field_name(k): (k, v) for k, v in schema.items()
        }

        # 统一映射处理：一对一和一对多都是同一种逻辑
        matched_count = 0
        failed_mappings = []

        for english_field, chinese_list in FIELD_MAPPINGS.items():
            for chinese_field in chinese_list:
                normalized_cn = normalize_field_name(chinese_field)

                if normalized_cn in normalized_schema:
                    original_cn, field_info = normalized_schema[normalized_cn]
                    reverse_map[english_field] = field_info
                    matched_count += 1
                    logger.debug(
                        f"[飞书] 映射: {english_field} -> {original_cn} -> {field_info['id']}"
                    )
                elif chinese_field in schema:
                    reverse_map[english_field] = schema[chinese_field]
                    matched_count += 1
                    logger.debug(
                        f"[飞书] 直接映射: {english_field} -> {chinese_field} -> {schema[chinese_field]['id']}"
                    )
                else:
                    failed_mappings.append((chinese_field, english_field))
                    logger.debug(f"[飞书] 未找到中文字段: {chinese_field}")

        # 诊断信息
        schema_fields = set(schema.keys())
        config_fields = set()
        for chinese_list in FIELD_MAPPINGS.values():
            config_fields.update(chinese_list)

        unmapped = schema_fields - config_fields
        if unmapped:
            logger.warning(f"[飞书] schema中未映射字段: {sorted(unmapped)}")

        logger.info(
            f"[飞书] 映射完成: 成功{matched_count}个, 失败{len(failed_mappings)}个"
        )
        return reverse_map

    def _convert_value_by_type(
        self, value: Any, field_info: Dict[str, Any]
    ) -> Any:
        """根据字段类型转换值"""
        ui_type = field_info.get("ui_type", "Text")

        try:
            if ui_type == "Text":
                return str(value)

            elif ui_type == "Number":
                # 确保是数字类型
                if isinstance(value, (int, float)):
                    return float(value)
                elif isinstance(value, str):
                    # 字符串转数字，失败则设为0
                    try:
                        return float(value.strip().replace(",", ""))
                    except (ValueError, AttributeError):
                        return 0.0
                else:
                    return 0.0

            elif ui_type == "DateTime":
                # 确保是Unix时间戳（毫秒）
                if isinstance(value, int):
                    return value
                elif isinstance(value, str):
                    # 解析ISO格式日期字符串
                    try:
                        dt_str = value.replace("Z", "+00:00")
                        dt = datetime.fromisoformat(dt_str)
                        return int(dt.timestamp() * 1000)
                    except (ValueError, AttributeError):
                        # 如果解析失败，使用当前时间
                        return int(datetime.now().timestamp() * 1000)
                else:
                    # 其他类型使用当前时间
                    return int(datetime.now().timestamp() * 1000)

            elif ui_type == "Checkbox":
                return bool(value)

            elif ui_type == "SingleSelect":
                # 单选需要{"id": "value"}格式
                return {"id": str(value)}

            elif ui_type == "MultiSelect":
                # 多选需要[{"id": "value"}]格式
                if isinstance(value, list):
                    return [{"id": str(v)} for v in value if v is not None]
                else:
                    return [{"id": str(value)}]

            else:
                # 默认按文本处理
                return str(value)

        except Exception as e:
            # 转换失败时记录错误并使用默认值
            logger.warning(
                f"字段转换失败: {value}[{type(value)}] -> {ui_type}: {e}"
            )
            return str(value) if ui_type != "Number" else 0.0

    def _convert_record(
        self,
        record: Dict[str, Any],
        schema: Dict[str, Dict[str, Any]],
        reverse_mapping: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """将单条记录转换为飞书API需要的格式 - 统一映射逻辑"""
        converted = {}

        for field_name, value in record.items():
            if value is None:  # None值直接跳过
                continue

            # 统一映射：reverse_mapping已经包含了一对一和一对多的处理结果
            field_info = reverse_mapping.get(field_name)
            if field_info:  # 找到映射的字段
                field_id = field_info["id"]
                converted[field_id] = self._convert_value_by_type(
                    value, field_info
                )
            else:
                logger.debug(f"[飞书] 未找到映射: {field_name}")

        return converted

    def _has_meaningful_value(self, v: Any) -> bool:
        """True if value is not None and not empty string after strip; 0 is meaningful."""
        if v is None:
            return False
        if isinstance(v, str) and v.strip() == "":
            return False
        return True

    def write_records(self, records: List[Dict[str, Any]]) -> bool:
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
            schema = self.get_table_schema()
            if not schema:
                logger.error("[飞书] 无法获取表格schema")
                return False

            # 构建反向映射
            reverse_mapping = self._build_reverse_mapping(schema)

            # 构建写入数据
            table_records = []

            for record in records:
                # 使用新的转换函数处理整条记录
                converted_fields = self._convert_record(
                    record, schema, reverse_mapping
                )

                if converted_fields:  # 只有有效数据才写入
                    table_records.append(
                        AppTableRecord.builder()
                        .fields(converted_fields)
                        .build()
                    )

            if not table_records:
                logger.warning("[飞书] 没有有效记录可以写入")
                return False

            # 写入前验证：检查第一条记录的数据类型
            if table_records:
                first_record = table_records[0]
                if hasattr(first_record, "_fields") and first_record._fields:
                    logger.info(
                        f"[飞书] 准备写入 {len(table_records)} 条记录，第一条包含 {len(first_record._fields)} 个字段"
                    )
                    # 只显示前3个字段的预览
                    preview_fields = list(first_record._fields.items())[:3]
                    for field_id, value in preview_fields:
                        logger.debug(
                            f"[飞书] 字段预览: {field_id}={value}[{type(value).__name__}]"
                        )

            # 分批处理
            batch_size = 500
            total_success = 0
            total_total = len(table_records)

            for i in range(0, len(table_records), batch_size):
                batch = table_records[i : i + batch_size]

                request: BatchCreateAppTableRecordRequest = (
                    BatchCreateAppTableRecordRequest.builder()
                    .app_token(self.app_token)
                    .table_id(self.table_id)
                    .request_body(
                        BatchCreateAppTableRecordRequestBody.builder()
                        .records(batch)
                        .build()
                    )
                    .build()
                )

                try:
                    response: BatchCreateAppTableRecordResponse = (
                        self.client.bitable.v1.app_table_record.batch_create(
                            request
                        )
                    )

                    if response.success():
                        batch_success = (
                            len(response.data.records) if response.data else 0
                        )
                        total_success += batch_success
                        logger.info(
                            f"[飞书] 批次 {i//batch_size + 1} 成功写入 {batch_success}/{len(batch)} 条记录"
                        )
                    else:
                        # 捕获飞书返回的详细错误信息
                        error_detail = {
                            "code": response.code,
                            "message": response.msg,
                            "request_id": getattr(
                                response, "request_id", "unknown"
                            ),
                            "table_id": self.table_id,
                            "batch_size": len(batch),
                        }

                        # 尝试获取更详细的错误信息
                        if hasattr(response, "data") and response.data:
                            error_detail["data"] = str(response.data)

                        logger.error(
                            f"[飞书] 写入失败详情: {json.dumps(error_detail, ensure_ascii=False)}"
                        )
                        return False

                except Exception as e:
                    # 捕获所有异常，包括网络错误、格式错误等
                    logger.error(
                        f"[飞书] API调用异常: {type(e).__name__}: {str(e)}"
                    )
                    if hasattr(e, "response"):
                        logger.error(f"[飞书] 响应内容: {e.response}")
                    return False

            logger.info(
                f"[飞书] 写入完成: 成功 {total_success}/{total_total} 条记录"
            )
            return total_success == total_total

        except Exception as e:
            logger.error(f"[飞书] 写入时发生错误: {e}")
            return False

    def validate_config(self) -> Dict[str, Any]:
        """验证配置和表格结构 - 同步版本"""
        result = {
            "enabled": self.enabled,
            "table_id": self.table_id,
            "valid": True,
            "errors": [],
        }

        if not self.enabled:
            result["valid"] = False
            result["errors"].append("飞书功能未启用")
            return result

        if not SDK_AVAILABLE:
            result["valid"] = False
            result["errors"].append("飞书SDK未安装")
            return result

        if not all(
            [self.app_id, self.app_secret, self.app_token, self.table_id]
        ):
            result["valid"] = False
            result["errors"].append("飞书配置不完整")
            return result

        try:
            schema = self.get_table_schema()
            if not schema:
                result["valid"] = False
                result["errors"].append("无法获取表格schema")
            else:
                result["field_count"] = len(schema)
                result["fields"] = list(schema.keys())

                # 测试反向映射
                reverse_mapping = self._build_reverse_mapping(schema)
                result["mapped_fields"] = len(reverse_mapping)
                result["sample_mapping"] = dict(
                    list(reverse_mapping.items())[:5]
                )

                try:
                    from src.config import FIELD_MAPPINGS

                    # 创建反向映射用于验证 (Chinese -> English)
                    chinese_to_english = {}
                    for english_name, chinese_list in FIELD_MAPPINGS.items():
                        for chinese_name in chinese_list:
                            chinese_to_english[chinese_name] = english_name

                    # 创建映射报告
                    mapping_report = []
                    for cn_name, en_name in chinese_to_english.items():
                        if cn_name in schema:
                            field_info = schema[cn_name]
                            mapping_report.append(
                                {
                                    "chinese": cn_name,
                                    "english": en_name,
                                    "field_id": field_info.get("id"),
                                    "type": field_info.get("ui_type"),
                                    "status": "found",
                                }
                            )
                        else:
                            mapping_report.append(
                                {
                                    "chinese": cn_name,
                                    "english": en_name,
                                    "field_id": None,
                                    "type": None,
                                    "status": "missing",
                                }
                            )

                    result["mapping_report"] = mapping_report
                    found_count = len(
                        [m for m in mapping_report if m["status"] == "found"]
                    )
                    result["mapping_success_rate"] = (
                        f"{found_count}/{len(mapping_report)}"
                    )

                    # 只记录关键信息
                    missing_count = len(mapping_report) - found_count
                    if missing_count > 0:
                        missing_examples = [
                            m["english"]
                            for m in mapping_report
                            if m["status"] == "missing"
                        ][:3]
                        logger.warning(
                            f"飞书字段映射: {missing_count}/{len(mapping_report)} 字段缺失, 示例: {missing_examples}"
                        )

                except ImportError:
                    logger.warning("无法导入FIELD_MAPPINGS用于验证")

        except Exception as e:
            result["valid"] = False
            result["errors"].append(str(e))

        return result


def create_feishu_writer(config: Dict[str, Any]) -> FeishuWriterSync:
    """Factory function to create Feishu writer."""
    return FeishuWriterSync(config)


# 向后兼容：提供异步包装器，但内部使用同步实现
class FeishuWriterV3:
    """Backward compatibility wrapper - keeps async interface but uses sync internally."""

    def __init__(self, config: dict):
        self._sync_writer = FeishuWriterSync(config)

    async def get_table_schema(self) -> Dict[str, Dict[str, Any]]:
        """Async wrapper for sync method."""
        return self._sync_writer.get_table_schema()

    async def write_records(self, records: List[Dict[str, Any]]) -> bool:
        """Async wrapper for sync method."""
        return self._sync_writer.write_records(records)

    async def validate_config(self) -> Dict[str, Any]:
        """Async wrapper for sync method."""
        return self._sync_writer.validate_config()
