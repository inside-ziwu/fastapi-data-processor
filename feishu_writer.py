import httpx
import asyncio
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
            
            # 调试：第一批前5条记录
            if cn_records:
                logger.info(f"[调试] 第一批前5条记录: {cn_records[:5]}")
            
            # 调试：检查字段类型和空值，以及数值范围
            problematic_fields = []
            for i, record in enumerate(cn_records):
                for key, value in record.items():
                    if value is None or value == "":
                        logger.warning(f"[调试] 记录{i+1}字段{key}为空值")
                        problematic_fields.append(key)
                    elif isinstance(value, float):
                        if str(value) == "nan" or str(value) == "NaN" or str(value) == "inf" or str(value) == "-inf":
                            logger.warning(f"[调试] 记录{i+1}字段{key}为无效浮点数: {value}")
                            problematic_fields.append(key)
                        elif abs(value) > 1e15:  # 飞书可能限制大数值
                            logger.warning(f"[调试] 记录{i+1}字段{key}数值过大: {value}")
                            problematic_fields.append(key)
                        elif abs(value) < 1e-15 and value != 0:  # 极小数值
                            logger.warning(f"[调试] 记录{i+1}字段{key}数值过小: {value}")
                            problematic_fields.append(key)
                    elif isinstance(value, str) and len(value) > 500:  # 飞书文本字段长度限制
                        logger.warning(f"[调试] 记录{i+1}字段{key}文本过长: {len(value)}字符")
                        problematic_fields.append(key)
            
            if problematic_fields:
                logger.warning(f"[调试] 发现问题字段: {set(problematic_fields)}")
            else:
                logger.info("[调试] 所有字段检查通过")
            
            # 清理和格式化数据，处理飞书API限制
            cleaned_records = []
            for record in cn_records:
                cleaned = {}
                for key, value in record.items():
                    if value is None or value == "" or str(value).lower() == "nan" or str(value) == "NaN":
                        continue  # 跳过空值和NaN字段
                    
                    # 处理浮点数精度问题
                    if isinstance(value, float):
                        # 限制小数位数，避免科学计数法
                        if abs(value) < 1e-10:  # 处理极小的数值
                            value = 0.0
                        elif abs(value) >= 1e6:  # 处理大数值
                            value = round(value, 2)
                        else:
                            value = round(value, 6)  # 一般数值保留6位小数
                    
                    cleaned[key] = value
                cleaned_records.append(cleaned)
            
            chunks = [cleaned_records[i:i+480] for i in range(0, len(cleaned_records), 480)]
            logger.info(f"[飞书] 分{len(chunks)}批写入，每批{len(chunks[0]) if chunks else 0}条")
            
            # 第2步：批量写入，带重试和详细错误处理
            success_count = 0
            async with httpx.AsyncClient() as client:
                for i, chunk in enumerate(chunks):
                    payload = {"records": [{"fields": item} for item in chunk]}
                    
                    # 调试：检查payload结构
                    if i == 0:
                        logger.info(f"[调试] 第{i+1}批payload结构: {payload['records'][:2] if payload['records'] else '空数据'}")
                        # 检查第一条记录的字段类型
                        if payload['records']:
                            first_record = payload['records'][0]['fields']
                            logger.info(f"[调试] 第一条记录字段类型: {[(k, type(v).__name__, str(v)[:50]) for k, v in first_record.items()]}")
                    
                    try:
                        resp = await client.post(self.base_url, json=payload, headers=headers, timeout=30)
                        
                        if resp.status_code == 200:
                            result = resp.json()
                            added_count = result.get('data', {}).get('records', [])
                            success_count += len(added_count)
                            logger.info(f"[飞书] 第{i+1}批写入成功：{len(added_count)}/{len(chunk)}条")
                        else:
                            error_resp = resp.json()
                            error_msg = error_resp.get("msg", "未知错误")
                            error_code = error_resp.get("code", "未知错误码")
                            
                            # 尝试逐条写入以定位问题记录
                            logger.error(f"[飞书] 第{i+1}批批量写入失败：{resp.status_code} - 错误码{error_code} - {error_msg}")
                            
                            # 如果批量失败，尝试逐条写入并记录失败的具体记录
                            failed_indices = []
                            for j, single_record in enumerate(chunk):
                                single_payload = {"records": [{"fields": single_record}]}
                                try:
                                    single_resp = await client.post(self.base_url, json=single_payload, headers=headers, timeout=30)
                                    if single_resp.status_code == 200:
                                        success_count += 1
                                    else:
                                        failed_indices.append(j)
                                        single_error = single_resp.json()
                                        logger.error(f"[飞书] 单条记录失败 - 记录索引{j}: {single_error}")
                                except Exception as single_e:
                                    failed_indices.append(j)
                                    logger.error(f"[飞书] 单条记录异常 - 记录索引{j}: {single_e}")
                            
                            logger.warning(f"[飞书] 第{i+1}批逐条写入完成：成功{len(chunk) - len(failed_indices)}/{len(chunk)}条")
                            
                    except Exception as e:
                        logger.error(f"[飞书] 第{i+1}批写入异常: {e}")
                        
            logger.info(f"[飞书] 写入完成：成功{success_count}/{len(cleaned_records)}条 (原始{len(records)}条，清理后{len(cleaned_records)}条)")
            return success_count == len(records)
            
        except httpx.HTTPStatusError as e:
            logger.error(f"[飞书] HTTP错误：{e.response.status_code} - {e.response.text}")
            return False
        except Exception as e:
            logger.error(f"[飞书] 写入失败：{type(e).__name__}: {e}")
            return False