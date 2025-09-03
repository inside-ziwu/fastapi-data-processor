import httpx
import asyncio
from typing import List, Dict
from field_mapping import EN_TO_CN_MAP

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
            # 解析JSON字符串并转换为中文字段
            data_records = [eval(r) if isinstance(r, str) else r for r in records]
            
            # 英文字段转中文字段
            cn_records = []
            for record in data_records:
                cn_record = {}
                for en_key, value in record.items():
                    cn_key = EN_TO_CN_MAP.get(en_key, en_key)  # 映射中文
                    cn_record[cn_key] = value
                cn_records.append(cn_record)
            
            chunks = [cn_records[i:i+480] for i in range(0, len(cn_records), 480)]
            
            async with httpx.AsyncClient() as client:
                for chunk in chunks:
                    payload = {"records": [{"fields": item} for item in chunk]}
                    await client.post(self.base_url, json=payload, headers=self.headers, timeout=30)
            return True
        except Exception as e:
            print(f"Feishu write failed: {e}")
            return False