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
from datetime import datetime

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
        # 这里返回 polars DataFrame 的 to_dicts() 结果
        results = result_df.to_dicts()
        logger.info(f"Processing completed successfully, returning {len(results)} results")
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}", exc_info=True)
        shutil.rmtree(run_dir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")
    if payload.save_to_disk:
        out_path = os.path.join(run_dir, "result.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        return {"status":"ok","result_path":out_path, "results_preview": results[:3]}
    shutil.rmtree(run_dir, ignore_errors=True)
    return results

@app.get("/health")
def health():
    return {"status":"ok","time": datetime.utcnow().isoformat()}
