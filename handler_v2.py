import requests
import json

def handler(args):
    """
    Coze handler v3: 直接获取完整数据数组，消除所有中间层
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

    api_url = "https://dtc.zeabur.app/process-files"
    headers = {"Content-Type": "application/json", "x-api-key": "coze-api-key-2024"}

    try:
        # 单次请求获取完整数据数组
        resp = requests.post(api_url, json=data, headers=headers, timeout=400)  # 略大于6分钟
        resp.raise_for_status()
        api_response = resp.json()

        # 直接返回Coze格式，无需二次请求
        return {
            "code": api_response.get("code", 200),
            "msg": api_response.get("msg", ""),
            "records": api_response.get("records", [])
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