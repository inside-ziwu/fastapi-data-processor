import os
import shutil
import json
import time
import logging
import asyncio
import concurrent.futures
from typing import Optional, Dict
from fastapi import FastAPI, Body, HTTPException, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import datetime, date

# Import the new unified processor and finalizer
from src.processor import DataProcessor
from src.finalize import finalize_output

def json_date_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Coze-Compatible Data Processor")

# Global dictionary to track active tasks by user_id
ACTIVE_TASKS: Dict[str, asyncio.Task] = {}

TMP_ROOT = os.environ.get("TMP_ROOT", "/tmp/fastapi_data_proc")
os.makedirs(TMP_ROOT, exist_ok=True)
API_KEY = os.environ.get("PROCESSOR_API_KEY", None)

# Initialize the processor once
processor = DataProcessor()

def auth_ok(x_api_key: Optional[str]):
    if API_KEY is None:
        return True
    return x_api_key == API_KEY

def create_robust_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def download_to_file(url_or_path: str, target_dir: str) -> str:
    # ... (download logic remains the same)
    pass # Placeholder for brevity

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
    dimension: Optional[str] = None

@app.post("/process-files")
async def process_files(request: Request, payload: ProcessRequest = Body(...), x_api_key: Optional[str] = Header(None)):
    from concurrent.futures import TimeoutError as AsyncTimeoutError

    TOTAL_TIMEOUT = 360
    request_start_time = time.time()

    if not auth_ok(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")

    run_dir = os.path.join(TMP_ROOT, f"run_{int(time.time()*1000)}")
    os.makedirs(run_dir, exist_ok=True)

    # ... (Task Cancellation Logic remains the same) ...

    provided = payload.dict()
    local_paths = {}
    # ... (File download logic remains the same) ...

    if not local_paths:
        shutil.rmtree(run_dir, ignore_errors=True)
        raise HTTPException(status_code=400, detail="No valid file URLs provided.")

    dimension = provided.get("dimension", "经销商ID")
    if dimension in [None, "", "None"]:
        dimension = "经销商ID"

    try:
        async def core_processing_task():
            loop = asyncio.get_running_loop()
            # Run the synchronous, CPU-bound data processing in a thread pool
            def run_sync_processing():
                # The processor now handles the entire pipeline
                return processor.run_full_analysis(local_paths, dimension)
            
            result_df = await loop.run_in_executor(None, run_sync_processing)
            return result_df

        # ... (Task execution with timeout and cancellation logic remains the same) ...
        result_df = await asyncio.wait_for(asyncio.create_task(core_processing_task()), timeout=TOTAL_TIMEOUT)

        if result_df.is_empty():
            return {"code": 200, "msg": "处理完成，但未生成有效数据。", "records": []}

        # Finalize output: rename to Chinese, order columns
        final_df = finalize_output(result_df)

        num_rows, num_cols = final_df.shape
        results_data_standard = final_df.to_dicts()

        # ... (Message creation, file saving, and Feishu logic remains the same) ...
        message = f"处理完成，生成数据 {num_rows} 行，{num_cols} 列。"

        return {
            "code": 200,
            "msg": message,
            "records": [json.dumps(row, ensure_ascii=False, default=json_date_serializer) for row in results_data_standard]
        }

    except Exception as e:
        logger.error(f"Processing failed: {str(e)}", exc_info=True)
        shutil.rmtree(run_dir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

# ... (cleanup, health, and exception handlers remain the same) ...