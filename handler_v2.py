import requests
import json

def handler(args):
    """
    Coze handler v2: Relies on the backend to do all heavy lifting, including pagination.
    This handler only calls the API, fetches the pre-paginated result, and adapts it.
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
        # --- 第一次网络请求：触发处理并获取URL ---
        resp = requests.post(api_url, json=data, headers=headers, timeout=300)
        resp.raise_for_status()
        api_response = resp.json()

        msg = api_response.get("msg", "")
        download_urls = api_response.get("download_urls", {})
        standard_json_url = download_urls.get("standard_json")
        # 这是关键：获取包含所有分页数据的文件的URL
        feishu_pages_url = download_urls.get("feishu_pages")

        if not feishu_pages_url:
            raise ValueError("feishu_pages_url not found in API response. The backend did not provide the paginated data URL.")

        # --- 第二次网络请求：下载已经由后端处理并分页好的数据 ---
        pages_resp = requests.get(feishu_pages_url, timeout=120)
        pages_resp.raise_for_status()
        # feishu_pages_data 是一个列表，其中每个元素都是一页数据
        # e.g., [{"page": 1, "records": [...]}, {"page": 2, "records": [...]}]
        feishu_pages_data = pages_resp.json()

        # --- 构建输出：将后端返回的每一页适配成Coze的输出格式 ---
        output = []
        for page_block in feishu_pages_data:
            # page_block 的结构是 {"page": 1, "total_pages": 2, ..., "records": [...]}
            output.append({
                "standard_json_url": standard_json_url,
                "msg": msg,
                "records": page_block.get("records", []) # 从每一页中提取records
            })
        
        if not output:
             return [{
                "standard_json_url": standard_json_url,
                "msg": "处理完成，但后端未返回任何数据页。",
                "records": []
            }]

        return output

    except requests.exceptions.HTTPError as e:
        return [{
            "code": e.response.status_code,
            "msg": f"处理失败: {e.response.text}",
            "records": []
        }]
    except Exception as e:
        return [{
            "code": 500,
            "msg": str(e),
            "records": []
        }]
