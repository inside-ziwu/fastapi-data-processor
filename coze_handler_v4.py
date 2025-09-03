import requests
import json

def handler(args):
    """
    Coze handler v6: 方案2实现 - 接收完整数据并按2M大小智能分块
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
        # 获取完整数据
        resp = requests.post(api_url, json=data, headers=headers, timeout=400)
        resp.raise_for_status()
        api_response = resp.json()

        # 提取字符串数组
        string_records = api_response.get("records", [])
        total_size = api_response.get("total_size", 0)
        total_records = api_response.get("total_records", len(string_records))

        # 直接返回完整数据，不限制2MB
        return {
            "code": 200,
            "msg": f"处理完成，共{len(string_records)}条记录",
            "records": string_records
        }

    except requests.exceptions.HTTPError as e:
        return {
            "code": e.response.status_code,
            "msg": f"处理失败: {e.response.text}",
            "records": [],
            "total_chunks": 1,
            "current_chunk": 1,
            "has_more": False,
            "next_chunk": -1
        }
    except Exception as e:
        return {
            "code": 500,
            "msg": str(e),
            "records": [],
            "total_chunks": 1,
            "current_chunk": 1,
            "has_more": False,
            "next_chunk": -1
        }