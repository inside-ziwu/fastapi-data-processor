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
            
            # 调试：检查字段类型和空值
            for i, record in enumerate(cn_records):
                for key, value in record.items():
                    if value is None or value == "":
                        logger.warning(f"[调试] 记录{i+1}字段{key}为空值")
                    elif isinstance(value, float) and (str(value) == "nan" or str(value) == "NaN"):
                        logger.warning(f"[调试] 记录{i+1}字段{key}为NaN")
                    elif str(value).lower() == "nan":
                        logger.warning(f"[调试] 记录{i+1}字段{key}为字符串NaN")
            
            # 清理空值和NaN的临时解决方案
            cleaned_records = []
            for record in cn_records:
                cleaned = {}
                for key, value in record.items():
                    if value is None or value == "" or str(value).lower() == "nan" or str(value) == "NaN":
                        continue  # 跳过空值和NaN字段
                    cleaned[key] = value
                cleaned_records.append(cleaned)
            
            chunks = [cleaned_records[i:i+480] for i in range(0, len(cleaned_records), 480)]
            logger.info(f"[飞书] 分{len(chunks)}批写入，每批{len(chunks[0]) if chunks else 0}条")
            
            # 第2步：批量写入
            success_count = 0
            async with httpx.AsyncClient() as client:
                for i, chunk in enumerate(chunks):
                    payload = {"records": [{"fields": item} for item in chunk]}
                    
                    # 调试：检查payload结构
                    if i == 0:
                        logger.info(f"[调试] 第{i+1}批payload结构: {payload['records'][:2] if payload['records'] else '空数据'}")
                    
                    resp = await client.post(self.base_url, json=payload, headers=headers, timeout=30)
                    
                    if resp.status_code == 200:
                        success_count += len(chunk)
                        logger.info(f"[飞书] 第{i+1}批写入成功：{len(chunk)}条")
                    else:
                        try:
                            error_resp = resp.json()
                            error_msg = error_resp.get("msg", "未知错误")
                            error_code = error_resp.get("code", "未知错误码")
                            logger.error(f"[飞书] 第{i+1}批写入失败：{resp.status_code} - 错误码{error_code} - {error_msg}")
                            
                            # 调试：失败时打印详细错误信息
                            if "records" in error_resp:
                                logger.error(f"[调试] 失败详情: {error_resp}")
                        except Exception as e:
                            logger.error(f"[飞书] 第{i+1}批写入失败：{resp.status_code} - 无法解析错误响应: {resp.text}")
                        
            logger.info(f"[飞书] 写入完成：成功{success_count}/{len(cleaned_records)}条 (原始{len(records)}条，清理后{len(cleaned_records)}条)")
            return success_count == len(records)
            
        except httpx.HTTPStatusError as e:
            logger.error(f"[飞书] HTTP错误：{e.response.status_code} - {e.response.text}")
            return False
        except Exception as e:
            logger.error(f"[飞书] 写入失败：{type(e).__name__}: {e}")
            return False