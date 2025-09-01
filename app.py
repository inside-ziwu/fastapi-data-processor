import os
import tempfile
import shutil
import json
import time
import logging
from typing import Optional, Dict
from fastapi import FastAPI, Body, HTTPException, Header, Response, Request
from pydantic import BaseModel
import requests
from datetime import datetime, date

def json_date_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError (f"Type {type(obj)} not serializable")
import polars as pl

from data_processor import process_all_files

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Coze-Compatible Data Processor")

TMP_ROOT = os.environ.get("TMP_ROOT", "/tmp/fastapi_data_proc")
os.makedirs(TMP_ROOT, exist_ok=True)
API_KEY = os.environ.get("PROCESSOR_API_KEY", None)

def auth_ok(x_api_key: Optional[str]):
    if API_KEY is None:
        return True
    return x_api_key == API_KEY

def download_to_file(url_or_path: str, target_dir: str) -> str:
    os.makedirs(target_dir, exist_ok=True)
    logger.info(f"Processing URL/path: {url_or_path}")
    if url_or_path.startswith("file://") or os.path.exists(url_or_path):
        if url_or_path.startswith("file://"):
            local_src = url_or_path[len("file://"):]
        else:
            local_src = url_or_path
        if not os.path.exists(local_src):
            raise FileNotFoundError(f"Local file not found: {local_src}")
        dest = os.path.join(target_dir, os.path.basename(local_src))
        shutil.copy(local_src, dest)
        logger.info(f"Copied local file from {local_src} to {dest}")
        return dest
    resp = requests.get(url_or_path, stream=True, timeout=120)
    if resp.status_code != 200:
        logger.error(f"Failed to download {url_or_path}: HTTP {resp.status_code}")
        raise HTTPException(status_code=502, detail=f"Failed to download {url_or_path}: {resp.status_code}")
    fname = url_or_path.split("/")[-1].split("?")[0] or f"file_{int(time.time())}"
    dest = os.path.join(target_dir, fname)
    logger.info(f"Saving downloaded file to: {dest}")
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=4*1024*1024):
            if chunk:
                f.write(chunk)
    file_size = os.path.getsize(dest)
    logger.info(f"Successfully downloaded {fname} ({file_size} bytes)")
    return dest

class ProcessRequest(BaseModel):
    video_excel_file: Optional[str] = None
    live_bi_file: Optional[str] = None
    msg_excel_file: Optional[str] = None
    DR1_file: Optional[str] = None
    DR2_file: Optional[str] = None
    account_base_file: Optional[str] = None
    leads_file: Optional[str] = None
    account_bi_file: Optional[str] = None
    Spending_file: Optional[str] = None
    spending_sheet_names: Optional[str] = None
    save_to_disk: Optional[bool] = False

@app.post("/process-files")
async def process_files(request: Request, payload: ProcessRequest = Body(...), x_api_key: Optional[str] = Header(None)):
    if not auth_ok(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    run_dir = os.path.join(TMP_ROOT, f"run_{int(time.time()*1000)}")
    os.makedirs(run_dir, exist_ok=True)
    raw_body = await request.body()
    logger.info(f"RAW REQUEST BODY: {raw_body.decode()}")
    file_keys = [
        "video_excel_file", "live_bi_file", "msg_excel_file", "DR1_file", "DR2_file",
        "account_base_file", "leads_file", "account_bi_file", "Spending_file"
    ]
    provided = payload.dict()
    local_paths = {}
    logger.info(f"Starting file processing with payload: {list(provided.keys())}")
    for key in file_keys:
        val = provided.get(key)
        logger.info(f"DEBUG: {key} raw value = '{repr(val)}' (type: {type(val)})")
        if val is not None and str(val).strip() != "" and str(val).strip() != "None":
            try:
                logger.info(f"Downloading file: {key} = {val}")
                local_paths[key] = download_to_file(val, run_dir)
                logger.info(f"Successfully downloaded {key} to {local_paths[key]}")
            except Exception as e:
                shutil.rmtree(run_dir, ignore_errors=True)
                raise HTTPException(status_code=500, detail=f"Failed to download {key}: {str(e)}")
        else:
            logger.info(f"❌ Skipping {key}: value is '{repr(val)}'")
    logger.info(f"Valid files to process: {list(local_paths.keys())}")
    if not local_paths:
        shutil.rmtree(run_dir, ignore_errors=True)
        raise HTTPException(status_code=400, detail="No valid file URLs provided. Please provide at least one file URL.")
    spending_sheet_names = provided.get("spending_sheet_names")
    try:
        logger.info(f"Starting core processing with {len(local_paths)} files: {list(local_paths.keys())}")
        result_df = process_all_files(local_paths, spending_sheet_names=spending_sheet_names)

        # --- UNIFIED FINAL LOGIC ---
        # 1. Get shape for size hint
        num_rows, num_cols = result_df.shape

        # 2. Count non-compliant floats before cleaning
        nan_count = 0
        inf_count = 0
        for col_name in result_df.columns:
            if result_df[col_name].dtype in [pl.Float32, pl.Float64]:
                nan_count += result_df[col_name].is_nan().sum()
                inf_count += result_df[col_name].is_infinite().sum()
        cleaned_count = nan_count + inf_count

        # 3. Clean the dataframe
        for col_name in result_df.columns:
            if result_df[col_name].dtype in [pl.Float32, pl.Float64]:
                result_df = result_df.with_columns(
                    pl.when(pl.col(col_name).is_infinite())
                    .then(None)
                    .otherwise(pl.col(col_name))
                    .fill_nan(None)
                    .alias(col_name)
                )

        # 4. Create standard and Feishu data formats
        results_data_standard = result_df.to_dicts()
        
        feishu_records = []
        for row_dict in results_data_standard:
            fields_str = json.dumps(row_dict, ensure_ascii=False, default=json_date_serializer)
            feishu_records.append({"fields": fields_str})
        feishu_output = {"records": feishu_records}

        # 5. Calculate final size and construct message
        json_string_for_size_calc = json.dumps(results_data_standard, default=json_date_serializer)
        data_size_bytes = len(json_string_for_size_calc.encode('utf-8'))
        data_size_mb = data_size_bytes / (1024 * 1024)
        size_warning = " (超过2MB)" if data_size_mb > 2 else ""
        message = (
            f"处理完成，生成数据 {num_rows} 行，{num_cols} 列，"
            f"数据大小约 {data_size_mb:.2f} MB{size_warning}。"
            f"清理了 {cleaned_count} 个无效数值。"
        )
        logger.info(message)

    except Exception as e:
        logger.error(f"Processing failed: {str(e)}", exc_info=True)
        shutil.rmtree(run_dir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

    # --- FINAL RETURN LOGIC ---
    if payload.save_to_disk:
        # Save both files
        out_path_standard = os.path.join(run_dir, "result.json")
        with open(out_path_standard, "w", encoding="utf-8") as f:
            json.dump({"message": message, "data": results_data_standard}, f, ensure_ascii=False, indent=2, default=json_date_serializer)

        out_path_feishu = os.path.join(run_dir, "feishu_import.json")
        with open(out_path_feishu, "w", encoding="utf-8") as f:
            json.dump(feishu_output, f, ensure_ascii=False, indent=2)

        # Construct downloadable URLs
        from urllib.parse import urljoin, quote
        base_url = str(request.base_url)
        download_endpoint = "get-result-file"
        url_standard = f"{urljoin(base_url, download_endpoint)}?file_path={quote(out_path_standard)}"
        url_feishu = f"{urljoin(base_url, download_endpoint)}?file_path={quote(out_path_feishu)}"
        logger.info(f"Returning downloadable URLs: {url_standard}, {url_feishu}")

        return {
            "status": "ok",
            "message": message,
            "result_path": {
                "standard_json": url_standard,
                "feishu_import_json": url_feishu
            },
            "results_preview": results_data_standard[:3],
            "feishu_data": feishu_output
        }
    else:
        # Return Feishu data directly, but no file is saved
        shutil.rmtree(run_dir, ignore_errors=True)
        final_response = {
            "message": message,
            "results_preview": results_data_standard[:3],
            "feishu_data": feishu_output
        }
        return Response(content=json.dumps(final_response, ensure_ascii=False, default=json_date_serializer), media_type="application/json")

from urllib.parse import urljoin, quote
from fastapi.responses import FileResponse

@app.get("/health")
def health():
    return {"status":"ok","time": datetime.utcnow().isoformat()}

@app.get("/get-result-file")
async def get_result_file(file_path: str, x_api_key: Optional[str] = Header(None)):
    if not auth_ok(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    # Security check: ensure the path is within the allowed directory
    if not os.path.abspath(file_path).startswith(os.path.abspath(TMP_ROOT)):
        raise HTTPException(status_code=403, detail="Access denied.")

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found.")

    return FileResponse(path=file_path, media_type='application/octet-stream', filename=os.path.basename(file_path))

@app.get("/get-result-file")
async def get_result_file(file_path: str, x_api_key: Optional[str] = Header(None)):
    if not auth_ok(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    # Security check: ensure the path is within the allowed directory
    if not os.path.abspath(file_path).startswith(os.path.abspath(TMP_ROOT)):
        raise HTTPException(status_code=403, detail="Access denied.")

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found.")

    return FileResponse(path=file_path, media_type='application/octet-stream', filename=os.path.basename(file_path))
