import requests
import json

def handler(args):
    """
    Coze handler v6: 支持飞书多维表格写入
    """
    input_obj = getattr(args, 'input', None)
    def get_input_arg(name):
        return getattr(input_obj, name, '') if input_obj is not None else ''

    data = {
        k: v for k in [
            "video_excel_file", "live_bi_file", "msg_excel_file", 
            "DR1_file", "DR2_file", "account_base_file", 
            "leads_file", "account_bi_file", "Spending_file",
            "spending_sheet_names", "dimension"
        ]
        if (v := get_input_arg(k))
    }

    # 飞书配置参数
    feishu_enabled = getattr(input_obj, 'feishu_enabled', False)
    feishu_app_token = getattr(input_obj, 'feishu_app_token', '')
    feishu_table_id = getattr(input_obj, 'feishu_table_id', '')
    feishu_token = getattr(input_obj, 'feishu_token', '')
    
    if feishu_enabled:
        data["feishu_config"] = {
            "enabled": True,
            "app_token": feishu_app_token,
            "table_id": feishu_table_id,
            "token": feishu_token
        }

    api_url = "https://dtc.zeabur.app/process-files"
    headers = {"Content-Type": "application/json", "x-api-key": "coze-api-key-2024"}

    try:
        resp = requests.post(api_url, json=data, headers=headers, timeout=400)
        resp.raise_for_status()
        api_response = resp.json()

        string_records = api_response.get("records", [])
        
        return {
            "code": 200,
            "msg": f"处理完成，共{len(string_records)}条记录",
            "records": string_records
        }

    except requests.exceptions.HTTPError as e:
        return {
            "code": e.response.status_code,
            "msg": f"处理失败: {e.response.text}",
            "records": []
        }
    except Exception as e:
        return {
            "code": 500,
            "msg": str(e),
            "records": []
        }