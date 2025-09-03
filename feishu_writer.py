import httpx
import asyncio
from typing import List, Dict

class FeishuWriter:
    def __init__(self, config: dict):
        self.enabled = config.get("enabled", False)
        self.app_token = config.get("app_token", "")
        self.table_id = config.get("table_id", "")
        self.token = config.get("token", "")
        self.base_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        self.headers = {"Authorization": f"Bearer {self.token}"}
    
    async def write_records(self, records: List[str]) -> bool:
        """写入飞书，分批480条，失败不影响主流程"""
        if not self.enabled or not records:
            return True
            
        try:
            # 解析JSON字符串
            data_records = [eval(r) if isinstance(r, str) else r for r in records]
            chunks = [data_records[i:i+480] for i in range(0, len(data_records), 480)]
            
            async with httpx.AsyncClient() as client:
                for chunk in chunks:
                    payload = {"records": [{"fields": item} for item in chunk]}
                    await client.post(self.base_url, json=payload, headers=self.headers, timeout=30)
            return True
        except Exception as e:
            print(f"Feishu write failed: {e}")
            return False