import httpx
import asyncio
import json
from typing import List, Dict
import logging

logger = logging.getLogger("feishu")

class FeishuWriter:
    def __init__(self, config: dict):
        self.enabled = config.get("enabled", False)
        self.app_id = config.get("app_id", "")
        self.app_secret = config.get("app_secret", "")
        self.app_token = config.get("app_token", "")
        self.table_id = config.get("table_id", "")
        self.base_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        if not all([self.app_id, self.app_secret, self.app_token, self.table_id]):
            self.enabled = False
            logger.warning("[飞书] 配置不完整 (app_id, app_secret, app_token, table_id)，写入功能已禁用。")

    async def get_tenant_token(self) -> str:
        """获取tenant_access_token"""
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        payload = {"app_id": self.app_id, "app_secret": self.app_secret}
        
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            result = resp.json()
            if "tenant_access_token" not in result:
                raise ValueError(f"获取token失败: {result.get('msg', '未知错误')}")
            return result["tenant_access_token"]

    async def get_table_schema(self) -> Dict[str, Dict]:
        """
        获取并解析飞书多维表格的结构 (schema)，包含重试逻辑。
        如果多次尝试后仍然失败，则会抛出异常。
        """
        if not self.enabled:
            logger.warning("[飞书] 功能未启用，无法获取schema。")
            return {}

        schema_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields"
        
        retries = 3
        last_exception = None

        for attempt in range(retries):
            try:
                logger.info(f"[飞书] 开始获取表格 {self.table_id} 的 schema (尝试 {attempt + 1}/{retries})...")
                token = await self.get_tenant_token()
                headers = {"Authorization": f"Bearer {token}"}

                simplified_schema = {}
                page_token = None
                
                async with httpx.AsyncClient() as client:
                    while True:
                        params = {'page_size': 100}
                        if page_token:
                            params['page_token'] = page_token
                        
                        resp = await client.get(schema_url, headers=headers, params=params, timeout=30)
                        resp.raise_for_status()
                        data = resp.json().get("data", {})
                        
                        items = data.get("items", [])
                        if not items and not simplified_schema: # Only warn if it's the first empty response
                            logger.warning("[飞书] API首次返回的字段列表为空。")
                        
                        for field in items:
                            field_name = field.get("field_name")
                            field_type = field.get("type")
                            options = []
                            
                            if field_type in [3, 4]: # 3: 单选, 4: 多选
                                options = [opt["name"] for opt in field.get("property", {}).get("options", [])]

                            if field_name:
                                simplified_schema[field_name] = {
                                    "type": field_type,
                                    "options": options
                                }
                        
                        if data.get("has_more") and data.get("page_token"):
                            page_token = data.get("page_token")
                        else:
                            break # Exit pagination loop on success
                
                logger.info(f"[飞书] Schema获取成功，共解析 {len(simplified_schema)} 个字段。")
                return simplified_schema # Return on success

            except httpx.HTTPStatusError as e:
                last_exception = e
                logger.warning(f"[飞书] 获取schema API请求失败: {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 404:
                    logger.error("[飞书] 收到 404 错误，可能是 app_token 或 table_id 无效，不再重试。")
                    break # Stop retrying on 404
            except (httpx.RequestError, httpx.TimeoutException) as e:
                last_exception = e
                logger.warning(f"[飞书] 获取schema时发生网络错误: {e}")
            except Exception as e:
                last_exception = e
                logger.error(f"[飞书] 解析schema时发生未知错误: {e}", exc_info=True)
            
            # If we are here, it means an exception occurred. Wait before retrying.
            if attempt < retries - 1:
                wait_time = 2 ** attempt
                logger.info(f"[飞书] 将在 {wait_time} 秒后重试...")
                await asyncio.sleep(wait_time)

        # If loop finishes, all retries failed
        logger.error(f"[飞书] 获取 schema 彻底失败，已重试 {retries} 次。")
        raise RuntimeError(f"获取飞书表格结构失败: {last_exception}")

    async def write_records(self, records: List[Dict], schema: Dict = None) -> bool:
        """
        分批写入记录到飞书多维表格。
        如果批量写入失败，则会尝试逐条写入以跳过有问题的记录。
        """
        if not self.enabled:
            logger.info("[飞书] 写入未启用或配置不完整，跳过写入。")
            return True
        
        if not records:
            logger.info("[飞书] 没有记录可写入。")
            return True

        logger.info(f"[飞书] 开始写入，共 {len(records)} 条记录。")
        
        # 根据schema修正数据类型
        if schema:
            original_count = len(records)
            records = self._fix_data_types(records, schema)
            logger.info(f"[飞书] 数据类型修正完成，从 {original_count} 条记录修正为 {len(records)} 条。")
        
        try:
            token = await self.get_tenant_token()
            headers = {"Authorization": f"Bearer {token}"}
            logger.info("[飞书] 获取token成功。")
        except (httpx.HTTPError, ValueError) as e:
            logger.error(f"[飞书] 获取token失败，写入中止: {e}")
            return False

        # 过滤掉空记录
        records = [r for r in records if r and any(r.values())]
        if not records:
            logger.info("[飞书] 过滤后没有有效记录可写入。")
            return True
            
        chunks = [records[i:i + 480] for i in range(0, len(records), 480)]
        logger.info(f"[飞书] 数据分 {len(chunks)} 批写入，过滤后共 {len(records)} 条记录。")

        total_success_count = 0
        total_failed_count = 0
        
        async with httpx.AsyncClient() as client:
            for i, chunk in enumerate(chunks):
                # 验证chunk中的每个记录
                valid_chunk = []
                for item in chunk:
                    if item and any(item.values()):
                        valid_chunk.append(item)
                    else:
                        logger.warning(f"[飞书] 跳过无效记录: {item}")
                        total_failed_count += 1
                
                if not valid_chunk:
                    logger.warning(f"[飞书] 第 {i+1} 批所有记录无效，跳过")
                    continue
                
                payload = {"records": [{"fields": item} for item in valid_chunk]}
                
                try:
                    resp = await client.post(self.base_url, json=payload, headers=headers, timeout=60)
                    
                    if resp.status_code == 200:
                        result = resp.json()
                        added_records = result.get("data", {}).get("records", [])
                        batch_success_count = len(added_records)
                        total_success_count += batch_success_count
                        logger.info(f"[飞书] 第 {i+1}/{len(chunks)} 批写入成功: {batch_success_count}/{len(chunk)} 条。")
                    else:
                        # 批量写入失败，启动单条重试
                        error_resp = resp.json()
                        logger.error(f"[飞书] 第 {i+1}/{len(chunks)} 批写入失败: {resp.status_code} - {error_resp.get('msg', '未知错误')}")
                        logger.info(f"[飞书] 开始对第 {i+1} 批进行逐条写入...")
                        
                        batch_success_count, batch_failed_count = await self._write_single_records(client, chunk, headers)
                        total_success_count += batch_success_count
                        total_failed_count += batch_failed_count
                        logger.info(f"[飞书] 第 {i+1} 批逐条写入完成: 成功 {batch_success_count} 条, 失败 {batch_failed_count} 条。")

                except httpx.HTTPError as e:
                    logger.error(f"[飞书] 第 {i+1} 批发生网络错误: {e}")
                    logger.info(f"[飞书] 开始对第 {i+1} 批进行逐条写入...")
                    batch_success_count, batch_failed_count = await self._write_single_records(client, chunk, headers)
                    total_success_count += batch_success_count
                    total_failed_count += batch_failed_count
                except Exception as e:
                    logger.error(f"[飞书] 第 {i+1} 批发生未知异常: {e}")
                    total_failed_count += len(chunk)

        logger.info(f"[飞书] 写入总结: 总记录数 {len(records)}，成功 {total_success_count} 条，失败 {total_failed_count} 条。")
        return total_failed_count == 0

    def _fix_data_types(self, records: List[Dict], schema: Dict) -> List[Dict]:
        """
        根据飞书schema修正数据类型和字段名，返回修正后的数据副本。
        不修改原始数据。
        """
        if not schema or not records:
            return records
            
        # 创建字段映射（中英文互转）
        field_mapping = {}
        reverse_mapping = {}
        
        for field_name, field_info in schema.items():
            # 使用字段名本身作为key
            field_mapping[field_name] = field_name
            
        fixed_records = []
        schema_fields = set(schema.keys())
        
        for record in records:
            if not record:
                continue
                
            fixed_record = {}
            
            # 记录不匹配的字段用于调试
            missing_fields = []
            matched_fields = []
            
            for key, value in record.items():
                # 尝试精确匹配
                if key in schema:
                    target_key = key
                    matched_fields.append(key)
                else:
                    # 记录不匹配的字段名
                    missing_fields.append(key)
                    logger.warning(f"[飞书] 字段名不匹配: '{key}' 不在飞书表格中，可用字段: {list(schema_fields)}")
                    continue
                    
                target_key = field_mapping.get(key, key)
                    
                field_schema = schema[key]
                field_type = field_schema.get('type', 'text')
                
                try:
                    if field_type == 'number' and value is not None:
                        # 数字类型转换
                        if isinstance(value, (int, float)):
                            fixed_record[key] = value
                        elif isinstance(value, str) and value.strip():
                            fixed_record[key] = float(value.strip())
                        else:
                            fixed_record[key] = 0
                            
                    elif field_type == 'text' and value is not None:
                        # 文本类型转换
                        fixed_record[key] = str(value)
                        
                    elif field_type == 'url' and value is not None:
                        # URL类型转换
                        fixed_record[key] = str(value).strip()
                        
                    elif field_type == 'date' and value is not None:
                        # 日期类型转换 - 保持原格式或转为ISO
                        if isinstance(value, str) and value.strip():
                            fixed_record[key] = value.strip()
                        else:
                            fixed_record[key] = str(value)
                            
                    elif field_type == 'checkbox' and value is not None:
                        # 复选框类型转换
                        if isinstance(value, bool):
                            fixed_record[key] = value
                        elif isinstance(value, str):
                            str_value = value.strip().lower()
                            fixed_record[key] = str_value in ['true', '1', 'yes', '是', 'on']
                        elif isinstance(value, (int, float)):
                            fixed_record[key] = bool(value)
                        else:
                            fixed_record[key] = False
                            
                    else:
                        # 其他类型保留原值
                        fixed_record[key] = value
                        
                except (ValueError, TypeError) as e:
                    logger.warning(f"[飞书] 字段 '{key}' 类型转换失败: {value} -> {field_type}, 使用默认值")
                    
                    # 根据类型提供默认值
                    if field_type == 'number':
                        fixed_record[key] = 0
                    elif field_type == 'text':
                        fixed_record[key] = str(value) if value is not None else ''
                    elif field_type == 'url':
                        fixed_record[key] = ''
                    elif field_type == 'date':
                        fixed_record[key] = ''
                    elif field_type == 'checkbox':
                        fixed_record[key] = False
                    else:
                        fixed_record[key] = value
            
            fixed_records.append(fixed_record)
            
            # 记录匹配情况
            if matched_fields:
                logger.debug(f"[飞书] 记录匹配了 {len(matched_fields)} 个字段: {matched_fields}")
            if missing_fields:
                logger.warning(f"[飞书] 记录中 {len(missing_fields)} 个字段不匹配: {missing_fields}")
        
        # 如果没有任何有效记录，返回空列表
        if not fixed_records:
            logger.error("[飞书] 没有记录匹配飞书表格字段，无数据可写入")
            logger.error(f"[飞书] 飞书表格包含的字段: {sorted(schema_fields)}")
        else:
            logger.info(f"[飞书] 成功匹配 {len(fixed_records)} 条记录到飞书表格字段")
            
        return fixed_records

    async def _write_single_records(self, client: httpx.AsyncClient, records: List[Dict], headers: Dict) -> (int, int):
        """私有方法，用于逐条写入记录并返回成功和失败的计数"""
        success_count = 0
        failed_count = 0
        
        for record in records:
            if not record or not any(record.values()):  # 跳过空记录或所有字段都空的记录
                failed_count += 1
                continue
            
            single_payload = {"records": [{"fields": record}]}
            
            # 验证payload完整性
            if not record or not any(record.values()):
                logger.warning(f"[飞书] 跳过无效记录: {record}")
                failed_count += 1
                continue
                
            try:
                logger.debug(f"[飞书] 写入单条记录: {single_payload}")
                single_resp = await client.post(self.base_url, json=single_payload, headers=headers, timeout=30)
                if single_resp.status_code == 200:
                    success_count += 1
                else:
                    failed_count += 1
                    error_detail = single_resp.text
                    logger.warning(f"[飞书] 跳过问题记录: {error_detail} | 数据: {json.dumps(record, ensure_ascii=False)[:200]}...")
            except Exception as e:
                failed_count += 1
                logger.error(f"[飞书] 单条记录写入异常: {e} | 数据: {json.dumps(record, ensure_ascii=False)[:100]}...")
        
        return success_count, failed_count
