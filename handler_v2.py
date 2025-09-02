import requests
import json

def handler(args):
    """
    Coze handler v7: Final, simplified version for maximum compatibility.
    This handler only acts as a trigger. It calls the backend API and returns
    a simple object containing the URL to the final results file.
    A downstream HTTP node in the Coze workflow is now responsible for fetching the data from that URL.
    This solves all known Coze schema parser limitations.
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
        # --- Single API call to trigger processing and get the results URL ---
        resp = requests.post(api_url, json=data, headers=headers, timeout=300)
        resp.raise_for_status()
        api_response = resp.json()

        msg = api_response.get("msg", "")
        download_urls = api_response.get("download_urls", {})
        feishu_pages_url = download_urls.get("feishu_pages")

        if not feishu_pages_url:
            raise ValueError("feishu_pages_url not found in API response.")

        # --- Return a very simple object with just the URL ---
        final_output = {
            "code": 200,
            "msg": msg,
            "result_url": feishu_pages_url
        }
        
        return final_output

    except requests.exceptions.HTTPError as e:
        try:
            error_details = e.response.json()
        except json.JSONDecodeError:
            error_details = e.response.text
        return {
            "code": e.response.status_code,
            "msg": f"处理失败: {error_details}",
            "result_url": ""
        }
    except Exception as e:
        return {
            "code": 500,
            "msg": f"执行handler时发生意外错误: {str(e)}",
            "result_url": ""
        }