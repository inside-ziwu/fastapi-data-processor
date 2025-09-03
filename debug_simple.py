#!/usr/bin/env python3
import json
import os
import urllib.request
import urllib.parse

def debug_feishu_simple():
    """使用内置模块调试飞书API"""
    
    config = {
        "app_id": os.getenv("FEISHU_APP_ID", "cli_a69e3c6b73f9900c"),
        "app_secret": os.getenv("FEISHU_APP_SECRET", "tB9V1DbZcF7LqhHa6d0Wlh0fI5zVhHQ5"),
        "app_token": os.getenv("FEISHU_APP_TOKEN", "DbMdbtIIYaY6CCsxgUncFId8n1c"),
        "table_id": os.getenv("FEISHU_TABLE_ID", "tblQ8Yq1W7k0JVlO")
    }
    
    print("=== 飞书API调试开始 ===")
    
    try:
        # 1. 获取token
        token_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        token_payload = json.dumps({
            "app_id": config["app_id"], 
            "app_secret": config["app_secret"]
        }).encode('utf-8')
        
        print("1. 获取token...")
        token_req = urllib.request.Request(
            token_url,
            data=token_payload,
            headers={'Content-Type': 'application/json; charset=utf-8'}
        )
        
        with urllib.request.urlopen(token_req) as response:
            token_data = json.loads(response.read().decode())
        
        print(f"Token响应: {json.dumps(token_data, ensure_ascii=False, indent=2)}")
        
        if 'tenant_access_token' not in token_data:
            print("❌ 获取token失败")
            return
            
        token = token_data["tenant_access_token"]
        print("✅ 获取token成功")
        
        # 2. 测试API调用
        base_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{config['app_token']}/tables/{config['table_id']}/records"
        
        # 构造测试数据
        test_data = {
            "records": [{
                "fields": {
                    "主机厂经销商ID": "20970",
                    "层级": "A",
                    "门店名": "测试门店"
                }
            }]
        }
        
        payload = json.dumps(test_data, ensure_ascii=False).encode('utf-8')
        
        print(f"2. 请求URL: {base_url}")
        print(f"3. 发送数据: {json.dumps(test_data, ensure_ascii=False, indent=2)}")
        
        # 发送请求
        req = urllib.request.Request(
            base_url,
            data=payload,
            headers={
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json; charset=utf-8'
            }
        )
        
        try:
            with urllib.request.urlopen(req) as response:
                result = json.loads(response.read().decode())
                print("✅ 请求成功")
                print(f"响应: {json.dumps(result, ensure_ascii=False, indent=2)}")
                
        except urllib.error.HTTPError as e:
            error_body = e.read().decode()
            print(f"❌ HTTP错误 {e.code}: {e.reason}")
            print(f"错误详情: {error_body}")
            
            try:
                error_json = json.loads(error_body)
                if "msg" in error_json:
                    print(f"错误消息: {error_json['msg']}")
                if "code" in error_json:
                    print(f"错误码: {error_json['code']}")
            except:
                print(f"原始错误: {error_body}")
            
    except Exception as e:
        print(f"💥 测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_feishu_simple()