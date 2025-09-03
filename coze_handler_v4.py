import requests
import json

def handler(args):
    """
    Coze handler v4: 适配直接返回字符串数组的新格式
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
        # 单次请求获取字符串数组（每个元素是JSON对象字符串）
        resp = requests.post(api_url, json=data, headers=headers, timeout=400)
        resp.raise_for_status()
        
        # API现在直接返回字符串数组，无需解析
        string_array = resp.json()
        
        # 直接返回字符串数组给循环节点
        return string_array

    except requests.exceptions.HTTPError as e:
        # 返回空数组作为错误处理
        return []
    except Exception as e:
        # 返回空数组作为错误处理
        return []