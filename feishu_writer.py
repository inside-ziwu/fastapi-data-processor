import httpx
import asyncio
import json
from typing import List, Dict
from field_mapping import EN_TO_CN_MAP
import logging

# 使用与data_processor.py相同的日志配置
logger = logging.getLogger("feishu")

class FeishuWriter:
    def __init__(self, config: dict):
        self.enabled = config.get("enabled", False)
        self.app_id = config.get("app_id", "")
        self.app_secret = config.get("app_secret", "")
        self.app_token = config.get("app_token", "")
        self.table_id = config.get("table_id", "")
        self.base_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        
    async def get_tenant_token(self) -> str:
        """第1步：获取tenant_access_token"""
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        payload = {"app_id": self.app_id, "app_secret": self.app_secret}
        
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload)
            result = resp.json()
            return result["tenant_access_token"]
    
    async def write_records(self, records: List[str]) -> bool:
        """第2步：批量写入飞书，480条分批，带详细日志"""
        if not self.enabled or not records:
            logger.info("[飞书] 写入未启用或无数据")
            return True
            
        try:
            logger.info(f"[飞书] 开始写入，共{len(records)}条记录")
            
            # 第1步：获取token
            token = await self.get_tenant_token()
            logger.info("[飞书] 获取token成功")
            headers = {"Authorization": f"Bearer {token}"}
            
            # 解析并转换中文字段
            data_records = [eval(r) if isinstance(r, str) else r for r in records]
            cn_records = []
            for record in data_records:
                cn_record = {}
                for en_key, value in record.items():
                    cn_key = EN_TO_CN_MAP.get(en_key, en_key)
                    cn_record[cn_key] = value
                cn_records.append(cn_record)
            
            # 调试：第一批前5条记录（仅在DEBUG级别显示）
            if cn_records and logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"[调试] 第一批前5条记录: {cn_records[:5]}")
            
            # 调试：检查字段类型和空值（仅在DEBUG级别显示）
            if logger.isEnabledFor(logging.DEBUG):
                problematic_fields = []
                for i, record in enumerate(cn_records):
                    for key, value in record.items():
                        if value is None or value == "":
                            logger.debug(f"[调试] 记录{i+1}字段{key}为空值")
                            problematic_fields.append(key)
                        elif isinstance(value, float):
                            if str(value) == "nan" or str(value) == "NaN" or str(value) == "inf" or str(value) == "-inf":
                                logger.debug(f"[调试] 记录{i+1}字段{key}为无效浮点数: {value}")
                                problematic_fields.append(key)
                            elif abs(value) > 1e15:
                                logger.debug(f"[调试] 记录{i+1}字段{key}数值过大: {value}")
                                problematic_fields.append(key)
                            elif abs(value) < 1e-15 and value != 0:
                                logger.debug(f"[调试] 记录{i+1}字段{key}数值过小: {value}")
                                problematic_fields.append(key)
                        elif isinstance(value, str) and len(value) > 500:
                            logger.debug(f"[调试] 记录{i+1}字段{key}文本过长: {len(value)}字符")
                            problematic_fields.append(key)
                
                if problematic_fields:
                    logger.debug(f"[调试] 发现问题字段: {set(problematic_fields)}")
                else:
                    logger.debug("[调试] 所有字段检查通过")
            
            # 清理和格式化数据，处理飞书API限制
            cleaned_records = []
            empty_records_count = 0
            
            for record_index, record in enumerate(cn_records):
                cleaned = {}
                valid_field_count = 0
                
                for key, value in record.items():
                    # 跳过空值，但保留有效值
                    if value is None or value == "" or str(value).lower() == "nan" or str(value) == "NaN":
                        continue
                    
                    # 处理浮点数精度问题
                    if isinstance(value, float):
                        # 处理特殊浮点值
                        if str(value) == "nan" or str(value) == "NaN" or str(value) == "inf" or str(value) == "-inf":
                            continue
                        # 限制小数位数，避免科学计数法
                        if abs(value) < 1e-10:  # 处理极小的数值
                            value = 0.0
                        elif abs(value) >= 1e6:  # 处理大数值
                            value = round(value, 2)
                        else:
                            value = round(value, 6)  # 一般数值保留6位小数
                    
                    # 处理文本长度限制
                    if isinstance(value, str) and len(value) > 500:
                        value = value[:497] + "..."
                    
                    cleaned[key] = value
                    valid_field_count += 1
                
                # 确保记录不为空
                if valid_field_count > 0:
                    cleaned_records.append(cleaned)
                else:
                    empty_records_count += 1
                    logger.warning(f"[调试] 记录{record_index+1}清理后为空，已跳过")
            
            if empty_records_count > 0:
                logger.warning(f"[调试] 共跳过{empty_records_count}条空记录")
            
            if not cleaned_records:
                logger.warning("[飞书] 所有记录清理后为空，无数据可写入")
                return True
            
            chunks = [cleaned_records[i:i+480] for i in range(0, len(cleaned_records), 480)]
            logger.info(f"[飞书] 分{len(chunks)}批写入，每批{len(chunks[0]) if chunks else 0}条")
            
# 第2步：批量写入，带智能重试和错误处理
            success_count = 0
            total_processed = 0
            
            async with httpx.AsyncClient() as client:
                for i, chunk in enumerate(chunks):
                    payload = {"records": [{"fields": item} for item in chunk]}
                    
                    # 调试：检查payload结构
                    if i == 0:
                        logger.info(f"[调试] 第{i+1}批payload: {len(payload['records'])}条记录")
                        if payload['records']:
                            first_record = payload['records'][0]['fields']
                            logger.info(f"[调试] 样本记录字段: {list(first_record.keys())[:10]}...")
                    
                    try:
                        resp = await client.post(self.base_url, json=payload, headers=headers, timeout=30)
                        
                        if resp.status_code == 200:
                            result = resp.json()
                            added_records = result.get('data', {}).get('records', [])
                            success_count += len(added_records)
                            logger.info(f"[飞书] 第{i+1}批批量写入成功：{len(added_records)}/{len(chunk)}条")
                            total_processed += len(chunk)
                        else:
                            error_resp = resp.json()
                            error_msg = error_resp.get("msg", "未知错误")
                            error_code = error_resp.get("code", "未知错误码")
                            
                            logger.error(f"[飞书] 第{i+1}批批量写入失败：{resp.status_code} - {error_code} - {error_msg}")
                            logger.error(f"[飞书] 完整错误响应: {json.dumps(error_resp, ensure_ascii=False, indent=2)}")
                            logger.error(f"[飞书] 请求payload结构: {json.dumps(payload, ensure_ascii=False)[:500]}...")
                            
                            # 基于飞书API文档的结构化400错误处理
                            if resp.status_code == 400:
                                logger.error(f"[飞书] API格式错误，开始智能修复...")
                                
                                # 分析具体错误类型
                                error_details = error_resp
                                if isinstance(error_resp, dict) and "msg" in error_resp:
                                    error_msg_lower = str(error_resp.get("msg", "")).lower()
                                    
                                    # 字段格式错误处理
                                    if any(keyword in error_msg_lower for keyword in ["field", "column", "format", "type"]):
                                        logger.info("[飞书] 检测到字段格式问题，执行自动修复")
                                        
                                        # 结构化数据清理
                                        def clean_field_name(name: str) -> str:
                                            """清理字段名，符合飞书要求"""
                                            name = str(name).strip()
                                            # 移除控制字符和特殊符号
                                            name = ''.join(c for c in name if c.isprintable() and c not in '\n\r\t\\')
                                            # 限制长度（飞书建议50字符以内）
                                            return name[:50]
                                        
                                        def clean_field_value(value):
                                            """清理字段值"""
                                            if value is None or str(value).lower() in ['nan', 'none', 'null', '']:
                                                return None
                                            
                                            if isinstance(value, float):
                                                # 处理浮点特殊值
                                                if str(value) in ['nan', 'inf', '-inf']:
                                                    return None
                                                # 限制精度避免科学计数法
                                                return round(value, 6) if abs(value) < 1e15 else round(value, 2)
                                            
                                            if isinstance(value, str):
                                                # 清理字符串
                                                value = value.strip()
                                                if len(value) > 500:  # 飞书文本字段限制
                                                    value = value[:497] + "..."
                                                return value
                                            
                                            return value
                                        
                                        # 应用清理
                                        cleaned_chunk = []
                                        for record in chunk:
                                            cleaned_record = {}
                                            for key, value in record.items():
                                                clean_key = clean_field_name(key)
                                                clean_value = clean_field_value(value)
                                                if clean_key and clean_value is not None:
                                                    cleaned_record[clean_key] = clean_value
                                            
                                            if cleaned_record:  # 非空记录
                                                cleaned_chunk.append(cleaned_record)
                                        
                                        if cleaned_chunk:
                                            logger.info(f"[飞书] 数据清理完成：{len(chunk)}→{len(cleaned_chunk)}条")
                                            retry_payload = {"records": [{"fields": item} for item in cleaned_chunk]}
                                            retry_resp = await client.post(self.base_url, json=retry_payload, headers=headers, timeout=30)
                                            
                                            if retry_resp.status_code == 200:
                                                logger.info(f"[飞书] 清理后重试成功：{len(cleaned_chunk)}条")
                                                success_count += len(cleaned_chunk)
                                                continue
                                            else:
                                                retry_error = retry_resp.json()
                                                logger.error(f"[飞书] 清理重试失败: {retry_error}")
                                                
                                    else:
                                        logger.error(f"[飞书] 未知400错误类型: {error_msg_lower}")
                                
                                # 记录具体错误信息用于调试
                                logger.error(f"[飞书] 错误详情: {json.dumps(error_details, ensure_ascii=False, indent=2)}")
                            
                            # 智能重试：逐条写入以跳过问题记录（优化日志输出）
                            chunk_success = 0
                            chunk_failed = 0
                            error_summary = []
                            
                            for j, single_record in enumerate(chunk):
                                # 再次验证单条记录不为空
                                if not single_record:
                                    chunk_failed += 1
                                    if j < 3:  # 前3个空记录显示详细信息
                                        logger.warning(f"[飞书] 跳过空记录 - 批次{i+1}索引{j}")
                                    continue
                                    
                                single_payload = {"records": [{"fields": single_record}]}
                                try:
                                    single_resp = await client.post(self.base_url, json=single_payload, headers=headers, timeout=30)
                                    if single_resp.status_code == 200:
                                        success_count += 1
                                        chunk_success += 1
                                    else:
                                        chunk_failed += 1
                                        single_error = single_resp.json()
                                        error_msg = single_error.get('msg', '未知错误')
                                        error_summary.append(f"索引{j}: {error_msg}")
                                        
                                        # 只记录前10个错误详情，其余汇总
                                        if j < 10:
                                            logger.error(f"[飞书] 跳过问题记录 - 批次{i+1}索引{j}: {error_msg}")
                                except Exception as single_e:
                                    chunk_failed += 1
                                    error_summary.append(f"索引{j}: {str(single_e)[:50]}")
                                    if j < 10:
                                        logger.error(f"[飞书] 单条记录异常 - 批次{i+1}索引{j}: {str(single_e)[:100]}")
                            
                            # 汇总剩余错误
                            if chunk_failed > 10:
                                logger.error(f"[飞书] 批次{i+1}还有{chunk_failed-10}条记录错误（已显示前10个详情）")
                            
                            if error_summary:
                                logger.info(f"[飞书] 批次{i+1}逐条重试完成：成功{chunk_success}条，跳过{chunk_failed}条")
                            
                            total_processed += len(chunk)
                            logger.info(f"[飞书] 第{i+1}批逐条重试完成：成功{chunk_success}条，跳过{chunk_failed}条")
                            
                    except Exception as e:
                        logger.error(f"[飞书] 第{i+1}批写入异常: {str(e)[:200]}")
                        total_processed += len(chunk)
                        
            logger.info(f"[飞书] 写入总结：原始{len(records)}条 → 清理后{len(cleaned_records)}条 → 成功{success_count}条 → 失败{len(cleaned_records) - success_count}条")
            return success_count > 0
            
        except httpx.HTTPStatusError as e:
            logger.error(f"[飞书] HTTP错误：{e.response.status_code} - {e.response.text}")
            return False
        except Exception as e:
            logger.error(f"[飞书] 写入失败：{type(e).__name__}: {e}")
            return False