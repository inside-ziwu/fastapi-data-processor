import requests
import json

def handler(args):
    """
    Coze handler v5: 保持对象结构，records改为字符串数组
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
        # 单次请求获取数据
        resp = requests.post(api_url, json=data, headers=headers, timeout=400)
        resp.raise_for_status()
        api_response = resp.json()

        # 检查API返回的是数组还是对象
        if isinstance(api_response, list):
            # API直接返回字符串数组
            string_records = api_response
        else:
            # API返回对象，提取records并转换为字符串数组
            records = api_response.get("records", [])
            string_records = []
            for record in records:
                if isinstance(record, str):
                    # 已经是字符串
                    string_records.append(record)
                elif isinstance(record, dict):
                    # 转换为JSON字符串
                    string_records.append(json.dumps(record, ensure_ascii=False))
                else:
                    # 其他类型强制转换
                    string_records.append(str(record))

        # 保持对象结构，records是字符串数组
        return {
            "code": api_response.get("code", 200) if isinstance(api_response, dict) else 200,
            "msg": api_response.get("msg", "处理完成") if isinstance(api_response, dict) else "处理完成",
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