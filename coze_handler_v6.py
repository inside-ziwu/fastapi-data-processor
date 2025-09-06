import requests
import json

def handler(args):
    """
    Coze handler v6: 支持飞书多维表格写入（新版认证方式）
    """
    input_obj = getattr(args, 'input', None)
    def get_input_arg(name):
        return getattr(input_obj, name, '') if input_obj is not None else ''

    # 构造请求体（仅携带非空字段）
    data = {
        k: v for k in [
            "video_excel_file", "live_bi_file", "msg_excel_file", 
            "DR1_file", "DR2_file", "account_base_file", 
            "leads_file", "account_bi_file", "Spending_file",
            "spending_sheet_names", "dimension"
        ]
        if (v := get_input_arg(k))
    }

    # 默认维度：若未提供则按 NSC_CODE 聚合
    if not data.get("dimension"):
        data["dimension"] = "NSC_CODE"

    # 新版飞书配置参数（支持双维度）
    feishu_enabled = getattr(input_obj, 'feishu_enabled', False)
    feishu_app_id = getattr(input_obj, 'feishu_app_id', '')
    feishu_app_secret = getattr(input_obj, 'feishu_app_secret', '')
    feishu_app_token = getattr(input_obj, 'feishu_app_token', '')
    feishu_table_id = getattr(input_obj, 'feishu_table_id', '')  # 根据dimension自动选择
    
    if feishu_enabled:
        data["feishu_enabled"] = True
        data["feishu_app_id"] = feishu_app_id
        data["feishu_app_secret"] = feishu_app_secret
        data["feishu_app_token"] = feishu_app_token
        data["feishu_table_id"] = feishu_table_id

    api_url = "https://dtc.zeabur.app/process-files"
    headers = {"Content-Type": "application/json", "x-api-key": "coze-api-key-2024"}

    try:
        # 超时：连接60s，读取360s（与服务端总超时对齐）
        resp = requests.post(api_url, json=data, headers=headers, timeout=(60, 360))
        resp.raise_for_status()
        try:
            api_response = resp.json()
        except ValueError:
            return {
                "code": 500,
                "msg": f"处理失败：服务端返回非JSON响应：{resp.text[:200]}",
                "records": []
            }

        # 使用服务端返回的 code/msg；为避免超大payload，默认不回传 records
        code = api_response.get("code", 200)
        msg = api_response.get("msg", "处理完成")

        # 可选预览条数（避免返回海量records导致Coze卡顿）
        preview_n = 0
        try:
            preview_n = int(get_input_arg("preview_records") or 0)
        except Exception:
            preview_n = 0

        records = []
        if preview_n > 0:
            records = api_response.get("records", [])[:preview_n]

        return {
            "code": code,
            "msg": msg,
            "records": records
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
