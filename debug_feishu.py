#!/usr/bin/env python3
import requests
import json
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 测试数据（简化版，用于调试）
test_records = [
    {
        '主机厂经销商ID': '20970',
        '层级': 'A',
        '门店名': '四川中达成宝',
        '自然线索总量': 363,
        'T月自然线索量': 140,
        'T-1月自然线索量': 223
    }
]

def debug_feishu_api():
    """调试飞书API并获取详细错误信息"""
    
    # 配置信息 - 从环境变量读取
    import os
    config = {
        "app_id": os.getenv("FEISHU_APP_ID", "cli_a69e3c6b73f9900c"),
        "app_secret": os.getenv("FEISHU_APP_SECRET", "tB9V1DbZcF7LqhHa6d0Wlh0fI5zVhHQ5"),
        "app_token": os.getenv("FEISHU_APP_TOKEN", "DbMdbtIIYaY6CCsxgUncFId8n1c"),
        "table_id": os.getenv("FEISHU_TABLE_ID", "tblQ8Yq1W7k0JVlO")
    }
    
    logger.info("=== 飞书API调试开始 ===")
    logger.info(f"配置: app_id={config['app_id'][:10]}..., table_id={config['table_id']}")
    
    try:
        # 1. 获取token
        token_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        payload = {"app_id": config["app_id"], "app_secret": config["app_secret"]}
        
        logger.info(f"1. 获取token: {token_url}")
        token_resp = requests.post(token_url, json=payload, timeout=10)
        token_data = token_resp.json()
        
        logger.info(f"Token响应: {json.dumps(token_data, ensure_ascii=False, indent=2)}")
        
        if token_resp.status_code != 200 or 'tenant_access_token' not in token_data:
            logger.error(f"获取token失败: {token_data}")
            return
            
        token = token_data["tenant_access_token"]
        logger.info(f"✅ 获取token成功")
        
        # 2. 构造请求URL
        base_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{config['app_token']}/tables/{config['table_id']}/records"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        logger.info(f"2. 请求URL: {base_url}")
        
        # 3. 构造payload - 重点检查结构
        payload = {"records": [{"fields": item} for item in test_records]}
        
        logger.info(f"3. 发送payload:")
        logger.info(json.dumps(payload, ensure_ascii=False, indent=2))
        
        # 4. 发送请求
        logger.info(f"4. 发送POST请求...")
        resp = requests.post(base_url, json=payload, headers=headers, timeout=30)
        
        logger.info(f"响应状态码: {resp.status_code}")
        logger.info(f"响应内容: {resp.text}")
        
        if resp.status_code != 200:
            try:
                error_data = resp.json()
                logger.error(f"❌ 错误详情:")
                logger.error(json.dumps(error_data, ensure_ascii=False, indent=2))
                
                # 分析具体错误
                if "msg" in error_data:
                    logger.error(f"错误消息: {error_data['msg']}")
                if "code" in error_data:
                    logger.error(f"错误码: {error_data['code']}")
                    
            except Exception as e:
                logger.error(f"无法解析响应: {e}")
                logger.error(f"原始响应: {resp.text}")
        else:
            result = resp.json()
            logger.info("✅ 测试写入成功！")
            logger.info(f"响应: {json.dumps(result, ensure_ascii=False, indent=2)}")
            
    except Exception as e:
        logger.error(f"💥 测试失败: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    debug_feishu_api()