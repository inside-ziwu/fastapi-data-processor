import asyncio
import httpx
import json
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 测试数据（前几条记录）
test_records = [
    {
        '主机厂经销商ID': '20970',
        '层级': 'A',
        '门店名': '四川中达成宝',
        '自然线索总量': 363,
        'T月自然线索量': 140,
        'T-1月自然线索量': 223,
        '广告线索总量': 1757,
        'T月广告线索量': 472,
        'T-1月广告线索量': 1285,
        '总消耗': 61733.26,
        'T月消耗': 18560.89,
        'T-1月消耗': 43172.37,
        '付费线索总量': 1244,
        'T月付费线索量': 289,
        'T-1月付费线索量': 955,
        '区域线索总量': 512,
        'T月区域线索量': 183,
        'T-1月区域线索量': 329,
        '本地线索总量': 1503,
        'T月本地线索量': 459,
        'T-1月本地线索量': 1044,
        '有效直播时长总量(小时)': 220.5,
        'T月有效直播时长(小时)': 81.4,
        'T-1月有效直播时长(小时)': 139.1,
        '有效直播场次总量': 214,
        'T月有效直播场次': 79,
        'T-1月有效直播场次': 135,
        '总曝光人数': 1763660,
        'T月曝光人数': 576079,
        'T-1月曝光人数': 1187581,
        '总场观': 71506,
        'T月场观': 25557,
        'T-1月场观': 45949,
        '小风车点击总量': 8686,
        'T月小风车点击': 2600,
        'T-1月小风车点击': 6086,
        '小风车留资总量': 1631,
        'T月小风车留资': 429,
        'T-1月小风车留资': 1202,
        '直播线索总量': 1993,
        'T月直播线索': 546,
        'T-1月直播线索': 1447,
        '锚点曝光总量': 8,
        'T月锚点曝光': 0,
        'T-1月锚点曝光': 8,
        '组件点击总量': 0,
        'T月组件点击': 0,
        'T-1月组件点击': 0,
        '短视频留资总量': 0,
        'T月短视频留资': 0,
        'T-1月短视频留资': 0,
        '短视频发布总量': 49,
        'T月短视频发布': 19,
        'T-1月短视频发布': 30,
        '短视频播放总量': 240410,
        'T月短视频播放': 109468,
        'T-1月短视频播放': 130942,
        '进私总量': 4491.0,
        'T月进私': 1552.0,
        'T-1月进私': 2939.0,
        '私信开口总量': 2448.0,
        'T月私信开口': 800.0,
        'T-1月私信开口': 1648.0,
        '私信留资总量': 1610.0,
        'T月私信留资': 484.0,
        'T-1月私信留资': 1126.0
    }
]

async def test_feishu_api():
    """测试飞书API并获取详细错误信息"""
    
    # 配置信息 - 从实际配置读取
    import os
    config = {
        "enabled": True,
        "app_id": os.getenv("FEISHU_APP_ID", "cli_a69e3c6b73f9900c"),
        "app_secret": os.getenv("FEISHU_APP_SECRET", "tB9V1DbZcF7LqhHa6d0Wlh0fI5zVhHQ5"),
        "app_token": os.getenv("FEISHU_APP_TOKEN", "DbMdbtIIYaY6CCsxgUncFId8n1c"),
        "table_id": os.getenv("FEISHU_TABLE_ID", "tblQ8Yq1W7k0JVlO")
    }
    
    try:
        # 1. 获取token
        token_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        payload = {"app_id": config["app_id"], "app_secret": config["app_secret"]}
        
        async with httpx.AsyncClient() as client:
            token_resp = await client.post(token_url, json=payload)
            token_data = token_resp.json()
            logger.info(f"Token响应: {json.dumps(token_data, ensure_ascii=False, indent=2)}")
            token = token_data.get("tenant_access_token", token_data.get("data", {}).get("tenant_access_token"))
            if not token:
                raise Exception("无法获取tenant_access_token")
            logger.info(f"获取token成功: {token}")
            
            # 2. 构造请求URL
            base_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{config['app_token']}/tables/{config['table_id']}/records"
            headers = {"Authorization": f"Bearer {token}"}
            
            # 3. 构造payload
            payload = {"records": [{"fields": item} for item in test_records]}
            logger.info(f"发送payload结构: {json.dumps(payload, ensure_ascii=False, indent=2)}")
            
            # 4. 发送请求
            resp = await client.post(base_url, json=payload, headers=headers, timeout=30)
            
            logger.info(f"响应状态码: {resp.status_code}")
            logger.info(f"响应内容: {resp.text}")
            
            if resp.status_code != 200:
                try:
                    error_data = resp.json()
                    logger.error(f"错误详情: {json.dumps(error_data, ensure_ascii=False, indent=2)}")
                    
                    # 检查具体错误
                    if "error" in error_data:
                        logger.error(f"错误信息: {error_data['error']}")
                    if "msg" in error_data:
                        logger.error(f"错误消息: {error_data['msg']}")
                    if "code" in error_data:
                        logger.error(f"错误码: {error_data['code']}")
                        
                except Exception as e:
                    logger.error(f"无法解析响应: {e}")
                    logger.error(f"原始响应: {resp.text}")
            else:
                logger.info("测试写入成功！")
                
    except Exception as e:
        logger.error(f"测试失败: {e}")

if __name__ == "__main__":
    asyncio.run(test_feishu_api())