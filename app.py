import os
import tempfile
import shutil
import json
import time
import logging
from typing import Optional, Dict
from fastapi import FastAPI, Body, HTTPException, Header, Response, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
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

def create_robust_session():
    """创建健壮的HTTP会话"""
    session = requests.Session()
    retry = Retry(
        total=3,  # 最多重试3次
        backoff_factor=1,  # 退避因子：1s, 2s, 4s
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def download_to_file(url_or_path: str, target_dir: str) -> str:
    """防卡死的流式下载"""
    os.makedirs(target_dir, exist_ok=True)
    
    # 本地文件处理
    if url_or_path.startswith("file://") or os.path.exists(url_or_path):
        if url_or_path.startswith("file://"):
            local_src = url_or_path[len("file://"):]
        else:
            local_src = url_or_path
        if not os.path.exists(local_src):
            raise FileNotFoundError(f"Local file not found: {local_src}")
        dest = os.path.join(target_dir, os.path.basename(local_src))
        shutil.copy(local_src, dest)
        logger.info(f"本地文件复制完成: {dest} ({os.path.getsize(dest)} bytes)")
        return dest
    
    # 远程下载防卡死
    session = create_robust_session()
    fname = url_or_path.split("/")[-1].split("?")[0] or f"file_{int(time.time())}"
    dest = os.path.join(target_dir, fname)
    
    try:
        start_time = time.time()
        with session.get(url_or_path, stream=True, timeout=(30, 60)) as resp:
            resp.raise_for_status()
            
            content_length = int(resp.headers.get('content-length', 0))
            logger.info(f"开始下载: {fname} (大小: {content_length/1024/1024:.2f}MB)")
            
            downloaded = 0
            last_log_time = time.time()
            
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024*1024):  # 1MB分块
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # 每5秒报告一次进度
                        current_time = time.time()
                        if current_time - last_log_time > 5:
                            if content_length > 0:
                                progress = (downloaded / content_length) * 100
                                speed = downloaded / (current_time - start_time) / 1024 / 1024  # MB/s
                                logger.info(f"下载进度: {fname} - {progress:.1f}% ({speed:.2f}MB/s)")
                            else:
                                logger.info(f"下载中: {fname} - {downloaded/1024/1024:.2f}MB")
                            last_log_time = current_time
            
            file_size = os.path.getsize(dest)
            total_time = time.time() - start_time
            speed = file_size / total_time / 1024 / 1024
            
            logger.info(f"下载完成: {fname} - {file_size/1024/1024:.2f}MB in {total_time:.1f}s ({speed:.2f}MB/s)")
            return dest
            
    except requests.exceptions.Timeout:
        raise HTTPException(status_code=504, detail=f"下载超时: {url_or_path}")
    except requests.exceptions.ConnectionError:
        raise HTTPException(status_code=502, detail=f"连接失败: {url_or_path}")
    except requests.exceptions.HTTPError as e:
        raise HTTPException(status_code=e.response.status_code, detail=f"HTTP错误: {e}")
    except Exception as e:
        # 清理失败的文件
        if os.path.exists(dest):
            os.remove(dest)
        raise HTTPException(status_code=500, detail=f"下载失败: {str(e)}")

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

        # 3. Clean the dataframe - 彻底清理所有null值
        for col_name in result_df.columns:
            col_type = result_df[col_name].dtype
            if col_type in [pl.Float32, pl.Float64]:
                # 处理浮点数：NaN, Inf, -Inf → 0
                result_df = result_df.with_columns(
                    pl.when(pl.col(col_name).is_nan() | pl.col(col_name).is_infinite() | pl.col(col_name).is_null())
                    .then(0.0)
                    .otherwise(pl.col(col_name).round(6))
                    .alias(col_name)
                )
            elif col_type in [pl.Int32, pl.Int64]:
                # 处理整数null → 0
                result_df = result_df.with_columns(
                    pl.col(col_name).fill_null(0).alias(col_name)
                )
            elif col_type == pl.Utf8:
                # 处理字符串null → 空字符串
                result_df = result_df.with_columns(
                    pl.col(col_name).fill_null("").alias(col_name)
                )
            else:
                # 其他类型统一处理
                result_df = result_df.with_columns(
                    pl.col(col_name).fill_null(0).alias(col_name)
                )

        # 4. Create standard and Feishu data formats
        results_data_standard = result_df.to_dicts()  # 标准JSON保持英文
        
        # 飞书格式使用中文字段名 - 确保无null值
        results_data_chinese = []
        for row in results_data_standard:
            chinese_row = {}
            for en_key, value in row.items():
                cn_key = en_to_cn_map.get(en_key, en_key)
                # 确保value不是None/null
                if value is None:
                    if any(keyword in cn_key for keyword in ['率', '占比']):
                        value = 0.0
                    elif any(keyword in cn_key for keyword in ['量', '数', '时长', '消耗', 'CPL', '场观', '曝光', '点击']):
                        value = 0
                    else:
                        value = 0
                chinese_row[cn_key] = value
            results_data_chinese.append(chinese_row)
        
        feishu_records = []
        for chinese_row in results_data_chinese:
            feishu_records.append({"fields": chinese_row})
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
                    feishu_page_records = [{"fields": row} for row in current_page_records]
                    
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
            feishu_page_records = [{"fields": row} for row in current_page_records]
            
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
            
        # 最终检查：确保所有飞书记录中无null值
        for page in feishu_pages:
            for record in page["records"]:
                for key, value in record["fields"].items():
                    if value is None:
                        # 基于字段名确定默认值
                        if any(keyword in key for keyword in ['率', '占比']):
                            record["fields"][key] = 0.0
                        elif any(keyword in key for keyword in ['量', '数', '时长', '消耗', 'CPL', '场观', '曝光', '点击', '线索']):
                            record["fields"][key] = 0
                        else:
                            record["fields"][key] = 0

    # 智能分页逻辑已应用：按1.5MB或400条记录限制
    # Coze插件标准格式 - 包含分页结果
    if num_rows > 0:
        # 收集所有分页数据
        paginated_records = []
        for page in feishu_pages:
            if page["records"]:  # 确保有数据
                paginated_records.extend(page["records"])
        
        final_response = {
            "code": 200,
            "msg": f"处理完成：{num_rows}行数据，{round(data_size_mb, 2)}MB，分页{len(feishu_pages)}页",
            "records": paginated_records  # 已按1.5MB或400条限制分页
        }
    else:
        # 错误情况格式
        final_response = {
            "code": 500,
            "msg": "数据为空或无有效数据",
            "records": []
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
    return {
        "code": 200,
        "msg": "服务运行正常",
        "records": [
            {
                "status": "ok",
                "time": datetime.utcnow().isoformat()
            }
        ]
    }

# 全局异常处理器，确保所有错误响应符合Coze插件格式
@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "code": exc.status_code,
            "msg": str(exc.detail),
            "records": []
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "code": 500,
            "msg": f"处理失败: {str(exc)}",
            "records": []
        }
    )

@app.get("/get-result-file")
async def get_result_file(file_path: str, x_api_key: Optional[str] = Header(None)):
    if not auth_ok(x_api_key):
        return JSONResponse(
            status_code=401,
            content={
                "code": 401,
                "msg": "Unauthorized",
                "records": []
            }
        )
    
    # Security check: ensure the path is within the allowed directory
    if not os.path.abspath(file_path).startswith(os.path.abspath(TMP_ROOT)):
        return JSONResponse(
            status_code=403,
            content={
                "code": 403,
                "msg": "Access denied",
                "records": []
            }
        )

    if not os.path.exists(file_path):
        return JSONResponse(
            status_code=404,
            content={
                "code": 404,
                "msg": "File not found",
                "records": []
            }
        )

    return FileResponse(path=file_path, media_type='application/octet-stream', filename=os.path.basename(file_path))
