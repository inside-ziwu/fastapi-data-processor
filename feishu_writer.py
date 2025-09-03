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
        """第2步：批量写入飞书，480条分批"""
        if not self.enabled or not records:
            return True
            
        try:
            # 第1步：获取token
            token = await self.get_tenant_token()
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
            
            # 第2步：批量写入
            async with httpx.AsyncClient() as client:
                for chunk in chunks:
                    payload = {"records": [{"fields": item} for item in chunk]}
                    await client.post(self.base_url, json=payload, headers=headers, timeout=30)
            return True
        except Exception as e:
            print(f"Feishu write failed: {e}")
            return False