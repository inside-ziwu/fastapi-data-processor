
import requests

def handler(args):
    """Coze handler function for data processing"""
    input_obj = getattr(args, 'input', None)

    def get_input_arg(name):
        return getattr(input_obj, name, '') if input_obj is not None else ''

    # 文件参数收集
    data = {
        "video_excel_file": get_input_arg("video_excel_file"),
        "live_bi_file": get_input_arg("live_bi_file"),
        "msg_excel_file": get_input_arg("msg_excel_file"),
        "DR1_file": get_input_arg("DR1_file"),
        "DR2_file": get_input_arg("DR2_file"),
        "account_base_file": get_input_arg("account_base_file"),
        "leads_file": get_input_arg("leads_file"),
        "account_bi_file": get_input_arg("account_bi_file"),
        "Spending_file": get_input_arg("Spending_file"),
    }

    # 辅助参数处理
    spending_sheet_names = get_input_arg("spending_sheet_names")
    if spending_sheet_names:
        data["spending_sheet_names"] = spending_sheet_names

    dimension = get_input_arg("dimension")
    if dimension:
        data["dimension"] = dimension

    # 清理空值
    data = {k: v for k, v in data.items() if v}

    # API配置
    api_url = "https://dtc.zeabur.app/process-files"
    headers = {"Content-Type": "application/json", "x-api-key": "coze-api-key-2024"}

    try:
        resp = requests.post(api_url, json=data, headers=headers, timeout=300)
        resp.raise_for_status()  # 检查下游API是否成功

        # 合并字典，而不是嵌套
        response_data = resp.json()
        return {
            "status": "success",
            "response_status_code": resp.status_code,
            **response_data
        }
    except requests.exceptions.HTTPError as e:
        # 更具体地处理HTTP错误
        return {
            "status": "error",
            "message": f"Downstream API error: {e.response.status_code}",
            "response_data": e.response.text
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

