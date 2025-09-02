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
    dimension: Optional[str] = None  # 新增：聚合维度，支持 NSC_CODE 或 level
    save_to_disk: Optional[bool] = False

@app.post("/process-files")
async def process_files(request: Request, payload: ProcessRequest = Body(...), x_api_key: Optional[str] = Header(None)):
    # PROFILING: Start
    request_start_time = time.time()
    logger.info(f"PROFILING: Request received at {request_start_time}")

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
    
    download_start_time = time.time()
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
    
    # PROFILING: After Downloads
    download_end_time = time.time()
    logger.info(f"PROFILING: File downloads finished. Total download time: {download_end_time - download_start_time:.2f} seconds.")

    logger.info(f"Valid files to process: {list(local_paths.keys())}")
    if not local_paths:
        shutil.rmtree(run_dir, ignore_errors=True)
        raise HTTPException(status_code=400, detail="No valid file URLs provided. Please provide at least one file URL.")
    spending_sheet_names = provided.get("spending_sheet_names")
    dimension_raw = provided.get("dimension")
    dimension = str(dimension_raw).strip() if dimension_raw is not None and str(dimension_raw).strip() != "" else "NSC_CODE"
    logger.info(f"从请求中提取的dimension参数: '{dimension}' (原始: '{dimension_raw}', 类型: {type(dimension_raw)})")
    logger.info(f"维度验证: dimension == '层级' -> {dimension == '层级'}")
    logger.info(f"维度验证: dimension == 'level' -> {dimension == 'level'}")
    try:
        logger.info(f"Starting core processing with {len(local_paths)} files: {list(local_paths.keys())}")
        # PROFILING: Before Core Processing
        core_processing_start_time = time.time()
        try:
            result_df, en_to_cn_map, type_mapping = process_all_files(local_paths, spending_sheet_names=spending_sheet_names, dimension=dimension)
        except Exception as e:
            logger.error(f"Core processing failed: {str(e)}", exc_info=True)
            raise
        # PROFILING: After Core Processing
        core_processing_end_time = time.time()
        logger.info(f"PROFILING: Core processing finished. Total core processing time: {core_processing_end_time - core_processing_start_time:.2f} seconds.")

        # PROFILING: Before Output Prep
        output_prep_start_time = time.time()

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
        results_data_standard = result_df.to_dicts()  # 标准JSON保持英文
        
        # 飞书格式使用中文字段名
        results_data_chinese = []
        for row in results_data_standard:
            chinese_row = {}
            for en_key, value in row.items():
                cn_key = en_to_cn_map.get(en_key, en_key)
                chinese_row[cn_key] = value
            results_data_chinese.append(chinese_row)
        
        feishu_records = []
        for chinese_row in results_data_chinese:
            fields_str = json.dumps(chinese_row, ensure_ascii=False, default=json_date_serializer)
            feishu_records.append({"fields": fields_str})
        feishu_output = {
            "records": feishu_records,
            "field_mapping": en_to_cn_map,
            "field_types": type_mapping
        }

        # 5. Calculate final size and construct message
        json_string_for_size_calc = json.dumps(results_data_standard, default=json_date_serializer)
        data_size_bytes = len(json_string_for_size_calc.encode('utf-8'))
        data_size_mb = data_size_bytes / (1024 * 1024)
        size_warning = " (超过1.5MB)" if data_size_mb > 1.5 else ""
        message = (
            f"处理完成，生成数据 {num_rows} 行，{num_cols} 列，"
            f"数据大小约 {data_size_mb:.2f} MB{size_warning}。"
            f"清理了 {cleaned_count} 个无效数值。"
        )
        logger.info(message)

        # PROFILING: After Output Prep
        output_prep_end_time = time.time()
        logger.info(f"PROFILING: Output preparation finished. Total output prep time: {output_prep_end_time - output_prep_start_time:.2f} seconds.")

    except Exception as e:
        logger.error(f"Processing failed: {str(e)}", exc_info=True)
        shutil.rmtree(run_dir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

# 设置save_to_disk默认为true
    save_to_disk = provided.get("save_to_disk", True)
    
    # 始终保存文件（默认行为）
    out_path_standard = os.path.join(run_dir, "result.json")
    with open(out_path_standard, "w", encoding="utf-8") as f:
        json.dump({"message": message, "data": results_data_standard}, f, ensure_ascii=False, indent=2, default=json_date_serializer)

    out_path_feishu = os.path.join(run_dir, "feishu_import.json")
    with open(out_path_feishu, "w", encoding="utf-8") as f:
        json.dump(feishu_output, f, ensure_ascii=False, indent=2)

    # 构造下载URL
    from urllib.parse import urljoin, quote
    base_url = str(request.base_url)
    download_endpoint = "get-result-file"
    url_standard = f"{urljoin(base_url, download_endpoint)}?file_path={quote(out_path_standard)}"
    url_feishu = f"{urljoin(base_url, download_endpoint)}?file_path={quote(out_path_feishu)}"
    logger.info(f"Returning downloadable URLs: {url_standard}, {url_feishu}")

    # 按维度完整分组的分页逻辑
    total_records = len(results_data_chinese)
    
    if total_records == 0:
        feishu_pages = []
    else:
        # 按维度分组数据
        # 修正：确保使用正确的中文键
        if dimension == "level":
            dimension_key = "层级"
        else:  # 默认 NSC_CODE
            dimension_key = "主机厂经销商ID"
        groups = {}
        for record in results_data_chinese:
            key = record.get(dimension_key, 'unknown')
            if key not in groups:
                groups[key] = []
            groups[key].append(record)
        
        # 计算1.5MB对应的记录数
        if total_records > 0:
            sample_size = min(5, total_records)
            sample_data = results_data_standard[:sample_size]
            json_string_for_page_calc = json.dumps(sample_data, default=json_date_serializer)
            avg_record_size = len(json_string_for_page_calc.encode('utf-8')) / sample_size
            max_records_by_size = int((1.5 * 1024 * 1024) / avg_record_size) if avg_record_size > 0 else 400
        else:
            max_records_by_size = 400
        
        max_records_per_page = min(400, max_records_by_size)
        
        # 按NSC完整分组进行分页
        feishu_pages = []
        current_page_records = []
        current_page_size = 0
        page_num = 1
        
        for key, records in groups.items():
            # 计算这组NSC的大小
            group_json = json.dumps(records, default=json_date_serializer)
            group_size = len(group_json.encode('utf-8'))
            group_count = len(records)
            
            # 检查是否超过限制，如果会超过则创建新页
            if (current_page_size + group_size > 1.5 * 1024 * 1024 or 
                len(current_page_records) + group_count > max_records_per_page):
                
                if current_page_records:  # 如果当前页有数据
                    actual_page_size = len(json.dumps(current_page_records, default=json_date_serializer).encode('utf-8')) / (1024 * 1024)
                    feishu_page_records = [{"fields": json.dumps(row, ensure_ascii=False, default=json_date_serializer)} for row in current_page_records]
                    
                    feishu_page = {
                        "page": page_num,
                        "total_pages": 0,  # 稍后更新
                        "total_records": total_records,
                        "page_size": len(current_page_records),
                        "actual_size_mb": round(actual_page_size, 2),
                        "records": feishu_page_records
                    }
                    feishu_pages.append(feishu_page)
                    page_num += 1
                    current_page_records = []
                    current_page_size = 0
            
            # 添加当前组到当前页
            current_page_records.extend(records)
            current_page_size += group_size
        
        # 处理最后一页
        if current_page_records:
            actual_page_size = len(json.dumps(current_page_records, default=json_date_serializer).encode('utf-8')) / (1024 * 1024)
            feishu_page_records = [{"fields": json.dumps(row, ensure_ascii=False, default=json_date_serializer)} for row in current_page_records]
            
            feishu_page = {
                "page": page_num,
                "total_pages": len(feishu_pages) + 1,
                "total_records": total_records,
                "page_size": len(current_page_records),
                "actual_size_mb": round(actual_page_size, 2),
                "records": feishu_page_records
            }
            feishu_pages.append(feishu_page)
        
        # 更新总页数
        total_pages = len(feishu_pages)
        for page in feishu_pages:
            page["total_pages"] = total_pages

    feishu_records = []
    for page_num, page in enumerate(feishu_pages, 1):
        for idx, record in enumerate(page["records"], 1):
            record["page"] = page_num
            record["index"] = idx
            feishu_records.append(record)
    
    final_response = {
        "standard_json_url": url_standard,
        "feishu_records": feishu_records,
        "meta": {
            "total_size_mb": round(data_size_mb, 2),
            "total_rows": num_rows,
            "total_pages": len(feishu_pages),
            "page_size": len(feishu_pages[0]["records"]) if feishu_pages else 0
        }
    }
    
    # 如果save_to_disk为false，才清理临时目录
    if not save_to_disk:
        shutil.rmtree(run_dir, ignore_errors=True)
    
    logger.info(f"PROFILING: Total request time before returning response: {time.time() - request_start_time:.2f} seconds.")
    return final_response

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
