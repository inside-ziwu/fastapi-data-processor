import httpx
import asyncio
from typing import List, Dict
from field_mapping import EN_TO_CN_MAP

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
            print("[飞书] 写入未启用或无数据")
            return True
            
        try:
            print(f"[飞书] 开始写入，共{len(records)}条记录")
            
            # 第1步：获取token
            token = await self.get_tenant_token()
            print("[飞书] 获取token成功")
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
            
            chunks = [cn_records[i:i+480] for i in range(0, len(cn_records), 480)]
            print(f"[飞书] 分{len(chunks)}批写入，每批{len(chunks[0]) if chunks else 0}条")
            
            # 第2步：批量写入
            success_count = 0
            async with httpx.AsyncClient() as client:
                for i, chunk in enumerate(chunks):
                    payload = {"records": [{"fields": item} for item in chunk]}
                    resp = await client.post(self.base_url, json=payload, headers=headers, timeout=30)
                    
                    if resp.status_code == 200:
                        success_count += len(chunk)
                        print(f"[飞书] 第{i+1}批写入成功：{len(chunk)}条")
                    else:
                        error_msg = resp.json().get("msg", "未知错误")
                        print(f"[飞书] 第{i+1}批写入失败：{resp.status_code} - {error_msg}")
                        
            print(f"[飞书] 写入完成：成功{success_count}/{len(records)}条")
            return success_count == len(records)
            
        except httpx.HTTPStatusError as e:
            print(f"[飞书] HTTP错误：{e.response.status_code} - {e.response.text}")
            return False
        except Exception as e:
            print(f"[飞书] 写入失败：{type(e).__name__}: {e}")
            return False