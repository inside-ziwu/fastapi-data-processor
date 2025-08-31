# app.py
import os
import tempfile
import shutil
import json
import time
import logging
from typing import Optional, Dict, Any
from fastapi import FastAPI, Body, HTTPException, Header, Response
from pydantic import BaseModel
import requests
import polars as pl
import pandas as pd
from io import BytesIO
from datetime import datetime, date
import math

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Coze-Compatible Data Processor")

# ---------- Config ----------
TMP_ROOT = os.environ.get("TMP_ROOT", "/tmp/fastapi_data_proc")
os.makedirs(TMP_ROOT, exist_ok=True)

# If you want simple auth:
API_KEY = os.environ.get("PROCESSOR_API_KEY", None)

# ---------- Helpers ----------
def auth_ok(x_api_key: Optional[str]):
    if API_KEY is None:
        return True
    return x_api_key == API_KEY

def download_to_file(url_or_path: str, target_dir: str) -> str:
    """Download a URL (or copy local file) to target_dir and return local file path.
       Uses streaming to avoid loading entire file into memory."""
    os.makedirs(target_dir, exist_ok=True)
    logger.info(f"Processing URL/path: {url_or_path}")
    
    if url_or_path.startswith("file://") or os.path.exists(url_or_path):
        # Local file: copy
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

    # Remote URL
    logger.info(f"Downloading from URL: {url_or_path}")
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

# Utilities to read CSV/Excel robustly
def read_csv_polars(path: str) -> pl.DataFrame:
    return pl.read_csv(path, try_parse_dates=False, low_memory=False)

def read_excel_as_pandas(path: str, sheet_name=None) -> pd.DataFrame:
    # sheet_name=None returns dict of dfs
    return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")

def df_pandas_to_polars(df: pd.DataFrame) -> pl.DataFrame:
    # Convert pandas to polars; ensure object->string conversion for safety
    return pl.from_pandas(df)

def normalize_nsc_col(df: pl.DataFrame, colname: str = "NSC_CODE") -> pl.DataFrame:
    """Ensure NSC_CODE column exists and is exploded if it contains comma/; separated lists."""
    if colname not in df.columns:
        df = df.with_columns(pl.lit(None).alias(colname))
    # convert to string
    df = df.with_columns(pl.col(colname).cast(pl.Utf8))
    # unify delimiters to comma
    df = df.with_columns(
        pl.col(colname)
        .str.replace_all(r"[；，\|\u3001/\\]+", ",")
        .alias(colname)
    )
    # split -> list -> explode
    df = df.with_columns(pl.col(colname).str.split(",").alias("_nsc_list"))
    df = df.explode("_nsc_list").with_columns(pl.col("_nsc_list").str.strip().alias("NSC_CODE")).drop(colname).drop("_nsc_list")
    # drop empty NSC_CODE rows
    df = df.filter(pl.col("NSC_CODE").is_not_null() & (pl.col("NSC_CODE") != ""))
    return df

def ensure_date_column(pl_df: pl.DataFrame, colname: str = "date") -> pl.DataFrame:
    """Robustly parse date column using pandas (handles many formats), return polars with python date type."""
    if colname not in pl_df.columns:
        pl_df = pl_df.with_columns(pl.lit(None).alias(colname))
    pdf = pl_df.to_pandas()
    pdf[colname] = pd.to_datetime(pdf[colname], errors="coerce").dt.date
    return pl.from_pandas(pdf)

def try_cast_numeric(pl_df: pl.DataFrame, cols):
    for c in cols:
        if c in pl_df.columns:
            pl_df = pl_df.with_columns(pl.col(c).cast(pl.Float64).fill_null(0))
    return pl_df

# ---------- Field mapping dictionaries (from 需求文档 -> 标准字段) ----------
VIDEO_MAP = {
    "主机厂经销商id":"NSC_CODE", "日期":"date",
    "锚点曝光次数":"anchor_exposure", "锚点点击次数":"component_clicks",
    "新发布视频数":"short_video_count", "短视频表单提交商机量":"short_video_leads",
    "短视频播放量":"short_video_plays"
}
LIVE_MAP = {
    "主机厂经销商id列表":"NSC_CODE", "开播日期":"date",
    "超25分钟直播时长(分)":"over25_min_live_mins", "直播有效时长（小时）":"live_effective_hours",
    "超25min直播总场次":"effective_live_sessions", "曝光人数":"exposures", "场观":"viewers",
    "小风车点击次数（不含小雪花）":"small_wheel_clicks"
}
MSG_MAP = {
    "主机厂经销商ID":"NSC_CODE", "进入私信客户数":"enter_private_count",
    "主动咨询客户数":"private_open_count","私信留资客户数":"private_leads_count"
}
ACCOUNT_BI_MAP = {
    "主机厂经销商id列表":"NSC_CODE","日期":"date","直播间表单提交商机量":"live_leads","短-播放量":"short_video_plays"
}
LEADS_MAP = {
    "主机厂经销商id列表":"NSC_CODE","留资日期":"date","直播间表单提交商机量(去重)":"small_wheel_leads"
}
DR_MAP = {
    "reg_dealer":"NSC_CODE","register_time":"date","leads_type":"leads_type",
    "mkt_second_channel_name":"mkt_second_channel_name","send2dealer":"send2dealer"
}
SPENDING_MAP = {"NSC CODE":"NSC_CODE","Date":"date","Spending(Net)":"spending_net"}
ACCOUNT_BASE_MAP = {"NSC_id":"NSC_CODE","第二期层级":"level","NSC Code":"NSC_Code","抖音id":"store_name"}

# Helper to rename columns case-insensitively (try all keys)
def rename_columns_loose(pl_df: pl.DataFrame, mapping: Dict[str,str]) -> pl.DataFrame:
    col_map = {}
    lc_cols = {c.lower(): c for c in pl_df.columns}
    for src, dst in mapping.items():
        if src in pl_df.columns:
            col_map[src] = dst
        else:
            ls = src.lower()
            if ls in lc_cols:
                col_map[lc_cols[ls]] = dst
    if col_map:
        pl_df = pl_df.rename(col_map)
    return pl_df

# ---------- Core processing pipeline ----------
def process_all_files(local_paths: Dict[str,str], spending_sheet_names: Optional[str]=None) -> list:
    """
    local_paths: dict mapping keys (same names as in req JSON) to local file paths (downloaded).
    returns: list of JSON-able dicts as described in your output format.
    """
    logger.info(f"Entering process_all_files with {len(local_paths)} files")
    
    # 1) Read each file into polars DataFrames (or pandas for Excel sheets)
    # We'll try to detect CSV vs Excel by extension.
    def read_generic(path, kind_hint=None):
        logger.info(f"Reading file: {path}")
        p = path.lower()
        if p.endswith(".csv") or p.endswith(".txt"):
            logger.info(f"Detected CSV/TXT file: {path}")
            return read_csv_polars(path)
        if p.endswith(".xlsx") or p.endswith(".xls"):
            logger.info(f"Detected Excel file: {path}")
            # For single-sheet small excel, read first sheet to pandas then convert.
            # But some files (msg_excel_file, spending) need multiple sheets.
            return None  # caller will handle Excel differently
        # fallback: try csv then excel
        try:
            logger.info(f"Trying CSV fallback for: {path}")
            return read_csv_polars(path)
        except Exception as e:
            logger.error(f"Failed to read file {path} as CSV: {str(e)}")
            return None

    # Read video
    dfs = {}
    # video_excel_file
    if "video_excel_file" in local_paths:
        p = local_paths["video_excel_file"]
        df = read_generic(p)
        if df is None:
            # fallback: pandas read first sheet
            pdf = read_excel_as_pandas(p, sheet_name=0)
            df = df_pandas_to_polars(pdf)
        df = rename_columns_loose(df, VIDEO_MAP)
        df = normalize_nsc_col(df, "NSC_CODE") if "NSC_CODE" in df.columns or "主机厂经销商id" in df.columns else df
        df = ensure_date_column(df, "date")
        dfs["video"] = try_cast_numeric(df, ["anchor_exposure","component_clicks","short_video_count","short_video_leads","short_video_plays"])
    # live_bi_file
    if "live_bi_file" in local_paths:
        p = local_paths["live_bi_file"]
        df = read_generic(p)
        if df is None:
            pdf = read_excel_as_pandas(p, sheet_name=0)
            df = df_pandas_to_polars(pdf)
        df = rename_columns_loose(df, LIVE_MAP)
        df = normalize_nsc_col(df, "NSC_CODE")
        df = ensure_date_column(df, "date")
        dfs["live"] = try_cast_numeric(df, ["over25_min_live_mins","live_effective_hours","effective_live_sessions","exposures","viewers","small_wheel_clicks"])
    # msg_excel_file (requires merging sheets and adding date=sheetname)
    if "msg_excel_file" in local_paths:
        p = local_paths["msg_excel_file"]
        # read all sheets via pandas
        all_sheets = read_excel_as_pandas(p, sheet_name=None)
        rows = []
        for sheetname, pdf in all_sheets.items():
            pdf = pdf.copy()
            pdf["date"] = sheetname
            rows.append(pdf)
        if rows:
            big_pdf = pd.concat(rows, ignore_index=True)
            df = df_pandas_to_polars(big_pdf)
            df = rename_columns_loose(df, MSG_MAP)
            df = normalize_nsc_col(df, "NSC_CODE")
            df = ensure_date_column(df, "date")
            dfs["msg"] = try_cast_numeric(df, ["enter_private_count","private_open_count","private_leads_count"])
    # account_bi_file
    if "account_bi_file" in local_paths:
        p = local_paths["account_bi_file"]
        df = read_generic(p)
        if df is None:
            pdf = read_excel_as_pandas(p, sheet_name=0)
            df = df_pandas_to_polars(pdf)
        df = rename_columns_loose(df, ACCOUNT_BI_MAP)
        df = normalize_nsc_col(df, "NSC_CODE")
        df = ensure_date_column(df, "date")
        dfs["account_bi"] = try_cast_numeric(df, ["live_leads","short_video_plays"])
    # leads_file
    if "leads_file" in local_paths:
        p = local_paths["leads_file"]
        df = read_generic(p)
        if df is None:
            pdf = read_excel_as_pandas(p, sheet_name=0)
            df = df_pandas_to_polars(pdf)
        df = rename_columns_loose(df, LEADS_MAP)
        df = normalize_nsc_col(df, "NSC_CODE")
        df = ensure_date_column(df, "date")
        dfs["leads"] = try_cast_numeric(df, ["small_wheel_leads"])
    # DR1_file and DR2_file
    dr_frames = []
    for key in ["DR1_file","DR2_file"]:
        if key in local_paths:
            p = local_paths[key]
            df = read_generic(p)
            if df is None:
                pdf = read_excel_as_pandas(p, sheet_name=0)
                df = df_pandas_to_polars(pdf)
            df = rename_columns_loose(df, DR_MAP)
            df = normalize_nsc_col(df, "NSC_CODE")
            df = ensure_date_column(df, "date")
            dr_frames.append(df)
    if dr_frames:
        dr_all = pl.concat(dr_frames, how="vertical").lazy().collect()
        # ensure columns present
        for c in ["leads_type","mkt_second_channel_name","send2dealer"]:
            if c not in dr_all.columns:
                dr_all = dr_all.with_columns(pl.lit(None).alias(c))
        dfs["dr"] = dr_all
    # Spending_file (support multiple sheets)
    if "Spending_file" in local_paths:
        p = local_paths["Spending_file"]
        if p.lower().endswith(".csv"):
            df = read_csv_polars(p)
            df = rename_columns_loose(df, SPENDING_MAP)
            df = normalize_nsc_col(df, "NSC_CODE")
            df = ensure_date_column(df, "date")
            dfs["spending"] = try_cast_numeric(df, ["spending_net"])
        else:
            # Excel: read specified sheets or all
            sheet_names = None
            if spending_sheet_names:
                sheet_names = [s.strip() for s in spending_sheet_names.split(",") if s.strip()]
            if sheet_names:
                pdfs = []
                for s in sheet_names:
                    try:
                        pdf = read_excel_as_pandas(p, sheet_name=s)
                        pdfs.append(pdf)
                    except Exception as e:
                        # try to continue if sheet missing
                        continue
                if not pdfs:
                    pdfs = [read_excel_as_pandas(p, sheet_name=0)]
                big_pdf = pd.concat(pdfs, ignore_index=True)
            else:
                all_sheets = read_excel_as_pandas(p, sheet_name=None)
                big_pdf = pd.concat(all_sheets.values(), ignore_index=True)
            df = df_pandas_to_polars(big_pdf)
            df = rename_columns_loose(df, SPENDING_MAP)
            df = normalize_nsc_col(df, "NSC_CODE")
            df = ensure_date_column(df, "date")
            dfs["spending"] = try_cast_numeric(df, ["spending_net"])
    # account_base_file
    if "account_base_file" in local_paths:
        p = local_paths["account_base_file"]
        df = read_generic(p)
        if df is None:
            pdf = read_excel_as_pandas(p, sheet_name=0)
            df = df_pandas_to_polars(pdf)
        df = rename_columns_loose(df, ACCOUNT_BASE_MAP)
        # account_base likely maps NSC_id -> 层级, 抖音id->门店名
        df = df.with_columns(pl.col("NSC_CODE").cast(pl.Utf8))
        df = df.select([c for c in df.columns if c in ("NSC_CODE","level","store_name","NSC_Code")])
        dfs["account_base"] = df

    # ---------- Merge step ----------
    # We'll create a base frame by union of all NSC_CODE+date pairs from all frames
    keys = []
    for k,v in dfs.items():
        if "NSC_CODE" in v.columns and "date" in v.columns:
            keys.append(v.select(["NSC_CODE","date"]).unique())
    if not keys:
        raise HTTPException(status_code=400, detail="No frames with NSC_CODE + date found.")
    base = pl.concat(keys).unique().sort(["NSC_CODE","date"])
    # Join each df onto base
    for name, df in dfs.items():
        if name in ("account_base",):  # account_base join by NSC_CODE only
            base = base.join(df, left_on="NSC_CODE", right_on="NSC_CODE", how="left")
        else:
            if "date" in df.columns:
                base = base.join(df, left_on=["NSC_CODE","date"], right_on=["NSC_CODE","date"], how="left")
            else:
                base = base.join(df, left_on="NSC_CODE", right_on="NSC_CODE", how="left")

    # fill nulls for numeric columns
    numeric_cols = [c for c,dtype in zip(base.columns, base.dtypes) if dtype in (pl.Int64, pl.Float64)]
    for c in numeric_cols:
        base = base.with_column(pl.col(c).fill_null(0).alias(c))

    # ---------- Derived metrics ----------
    # convert over25min_live_mins -> hours (effective 25+ hours)
    if "over25_min_live_mins" in base.columns:
        base = base.with_column((pl.col("over25_min_live_mins")/60.0).alias("over25_min_live_hours"))

    # We'll compute daily natural/paid/car_store/area_boost/local leads from dr table if exists
    if "dr" in dfs:
        dr = dfs["dr"]
        # natural leads per date per NSC
        dr_natural = dr.filter(pl.col("leads_type") == "自然").groupby(["NSC_CODE","date"]).agg(pl.count().alias("daily_natural_leads"))
        dr_paid = dr.filter(pl.col("leads_type") == "付费").groupby(["NSC_CODE","date"]).agg(pl.count().alias("daily_paid_leads"))
        # car store paid leads: filter mkt_second_channel_name contains specific substrings
        cs_cond = (pl.col("mkt_second_channel_name").str.contains("抖音车云店", literal=False))
        cs_paid = dr.filter((pl.col("leads_type") == "付费") & (cs_cond)).groupby(["NSC_CODE","date"]).agg(pl.count().alias("daily_car_store_paid_leads"))
        # area boosted: match '抖音车云店_BMW_总部BDT_LS直发'
        area_cond = pl.col("mkt_second_channel_name").str.contains("总部BDT", literal=False)
        area_paid = dr.filter((pl.col("leads_type") == "付费") & (area_cond)).groupby(["NSC_CODE","date"]).agg(pl.count().alias("daily_area_boost_paid_leads"))
        # local leads: send2dealer == reg_dealer => in our normalized df send2dealer == NSC_CODE
        local = dr.filter(pl.col("send2dealer") == pl.col("NSC_CODE")).groupby(["NSC_CODE","date"]).agg(pl.count().alias("daily_local_leads"))

        # Join these daily counts into base
        for df_add in [dr_natural, dr_paid, cs_paid, area_paid, local]:
            base = base.join(df_add, on=["NSC_CODE","date"], how="left")

    # fill remaining nulls with zeros for those newly added columns
    fill_cols = [c for c in base.columns if c.startswith("daily_")]
    for c in fill_cols:
        base = base.with_column(pl.col(c).fill_null(0).alias(c))

    # ---------- Month extraction and identify T / T-1 ----------
    # convert date column (polars date object) to year-month integer YYYYMM
    # ensure date is python date
    if "date" in base.columns:
        # create yearmonth as int
        base = base.with_column(pl.col("date").apply(lambda x: x.strftime("%Y%m") if x is not None else None).alias("ym"))
    else:
        raise HTTPException(status_code=400, detail="No date column found in merged base.")

    # compute global T as max ym in data
    pdf = base.select(["NSC_CODE","date","ym"]).to_pandas()
    pdf["ym"] = pdf["ym"].astype(float)
    pdf = pdf.dropna(subset=["ym"])
    if pdf.empty:
        raise HTTPException(status_code=400, detail="No valid dates found.")
    max_ym = int(pdf["ym"].max())
    # compute previous month as T-1
    ym_dt = datetime(year=int(str(max_ym)[:4]), month=int(str(max_ym)[4:6]), day=1)
    # compute prev month
    if ym_dt.month == 1:
        prev_month_dt = datetime(year=ym_dt.year-1, month=12, day=1)
    else:
        prev_month_dt = datetime(year=ym_dt.year, month=ym_dt.month-1, day=1)
    T = max_ym
    T_minus_1 = int(prev_month_dt.strftime("%Y%m"))

    # ---------- Aggregations per NSC_CODE for T and T-1 ----------
    # define metrics to sum (from doc). We try to use standard column names we created above.
    metric_cols = {
        "anchor_exposure":"anchor_exposure",
        "component_clicks":"component_clicks",
        "short_video_count":"short_video_count",
        "short_video_leads":"short_video_leads",
        "short_video_plays":"short_video_plays",
        "enter_private_count":"enter_private_count",
        "private_open_count":"private_open_count",
        "private_leads_count":"private_leads_count",
        "over25_min_live_hours":"over25_min_live_hours",
        "live_effective_hours":"live_effective_hours",
        "effective_live_sessions":"effective_live_sessions",
        "exposures":"exposures",
        "viewers":"viewers",
        "small_wheel_clicks":"small_wheel_clicks",
        "small_wheel_leads":"small_wheel_leads",
        "live_leads":"live_leads",
        "daily_natural_leads":"daily_natural_leads",
        "daily_paid_leads":"daily_paid_leads",
        "spending_net":"spending_net",
        "daily_car_store_paid_leads":"daily_car_store_paid_leads",
        "daily_area_boost_paid_leads":"daily_area_boost_paid_leads",
        "daily_local_leads":"daily_local_leads"
    }

    results = []
    # Prepare groupby by NSC_CODE and ym
    # convert base.ym to int
    base = base.with_columns(pl.col("ym").cast(pl.Int64).alias("ym_int"))
    # For safety, ensure numeric columns exist (fill 0 when missing)
    for col in metric_cols.values():
        if col not in base.columns:
            base = base.with_columns(pl.lit(0).alias(col))

    # group sums per NSC_CODE and ym
    group_cols = ["NSC_CODE","ym_int"]
    agg_exprs = [pl.col(c).sum().alias(c+"_sum") for c in metric_cols.values()]
    grouped = base.groupby(group_cols).agg(agg_exprs)

    # compute effective days per NSC_CODE per month: count distinct date
    day_counts = base.groupby(["NSC_CODE","ym_int"]).agg(pl.col("date").n_unique().alias("effective_days"))

    # join grouped + day_counts
    grouped = grouped.join(day_counts, on=["NSC_CODE","ym_int"], how="left")
    gpdf = grouped.to_pandas().fillna(0)

    # convert to a dict keyed by NSC_CODE to easily extract T and T-1
    by_nsc = {}
    for _, row in gpdf.iterrows():
        nsc = str(row["NSC_CODE"])
        ym = int(row["ym_int"])
        entry = by_nsc.setdefault(nsc, {"NSC_CODE": nsc})
        entry.setdefault("months", {})[ym] = {**{k: float(row.get(v+"_sum", 0)) for k,v in metric_cols.items()},
                                             "effective_days": int(row.get("effective_days",0))}
    # for each NSC found, build final output object merging account_base info
    # pull account_base info from dfs if exists
    account_map = {}
    if "account_base" in dfs:
        ab_pdf = dfs["account_base"].to_pandas()
        ab_pdf = ab_pdf.drop_duplicates(subset=["NSC_CODE"])
        for _, r in ab_pdf.iterrows():
            account_map[str(r["NSC_CODE"])] = {"level": r.get("level"), "store_name": r.get("store_name")}

    for nsc, info in by_nsc.items():
        months = info.get("months", {})
        T_metrics = months.get(T, {})
        T1_metrics = months.get(T_minus_1, {})
        out = {
            "NSC CODE": nsc,
            "层级": account_map.get(nsc, {}).get("level"),
            "门店名": account_map.get(nsc, {}).get("store_name"),
            "T": T,
            "T-1": T_minus_1,
            "T_累计有效天数": int(T_metrics.get("effective_days",0)),
            "T-1_累计有效天数": int(T1_metrics.get("effective_days",0))
        }
        # add each metric both T and T-1
        for key in metric_cols.keys():
            out[f"{key}_T"] = float(T_metrics.get(key,0.0))
            out[f"{key}_T-1"] = float(T1_metrics.get(key,0.0))
        results.append(out)

    return results

# ---------- FastAPI endpoints ----------
class ProcessRequest(BaseModel):
    video_url: Optional[str] = None
    live_url: Optional[str] = None
    msg_url: Optional[str] = None
    dr1_url: Optional[str] = None
    dr2_url: Optional[str] = None
    base_url: Optional[str] = None
    leads_url: Optional[str] = None
    account_bi_file: Optional[str] = None
    spending_url: Optional[str] = None
    spending_sheets: Optional[str] = None
    save_to_disk: Optional[bool] = False

@app.post("/process-files")
async def process_files(request: Request, payload: ProcessRequest = Body(...), x_api_key: Optional[str] = Header(None)):
    if not auth_ok(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    # create unique temp dir for this request
    run_dir = os.path.join(TMP_ROOT, f"run_{int(time.time()*1000)}")
    os.makedirs(run_dir, exist_ok=True)
# Log raw request body for debugging
    raw_body = await request.body()
    logger.info(f"RAW REQUEST BODY: {raw_body.decode()}")
    
    # Map Coze field names to internal field names
    field_mapping = {
        'video_url': 'video_excel_file',
        'live_url': 'live_bi_file', 
        'msg_url': 'msg_excel_file',
        'dr1_url': 'DR1_file',
        'dr2_url': 'DR2_file',
        'base_url': 'account_base_file',
        'leads_url': 'leads_file',
        'account_bi_file': 'account_bi_file',
        'spending_url': 'Spending_file',
        'spending_sheets': 'spending_sheet_names'
    }
    
    provided = payload.dict()
    local_paths = {}
    logger.info(f"Starting file processing with Coze payload: {list(provided.keys())}")
    
    # Map and filter valid files
    valid_files = {}
    for coze_key, internal_key in field_mapping.items():
        val = provided.get(coze_key)
        logger.info(f"DEBUG: {coze_key} raw value = '{repr(val)}' (type: {type(val)})")
        if val is not None and str(val).strip() != "" and str(val).strip() != "None":
            valid_files[internal_key] = val
            logger.info(f"✅ Mapping {coze_key} -> {internal_key}: {val}")
        else:
            logger.info(f"❌ Skipping {coze_key}: value is '{repr(val)}'")
    
    logger.info(f"Valid files to process: {list(valid_files.keys())}")
    
    if not valid_files:
        shutil.rmtree(run_dir, ignore_errors=True)
        raise HTTPException(status_code=400, detail="No valid file URLs provided. Please provide at least one file URL.")
    
    try:
        for key, val in valid_files.items():
            logger.info(f"Downloading file: {key} = {val}")
            local_paths[key] = download_to_file(val, run_dir)
            logger.info(f"Successfully downloaded {key} to {local_paths[key]}")
    
        logger.info(f"Total files downloaded: {len(local_paths)}")
    except Exception as e:
        # cleanup
        shutil.rmtree(run_dir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=f"Failed to download files: {str(e)}")

    # call core processor
    try:
        logger.info(f"Starting core processing with {len(local_paths)} files: {list(local_paths.keys())}")
        results = process_all_files(local_paths, spending_sheet_names=payload.spending_sheet_names)
        logger.info(f"Processing completed successfully, returning {len(results)} results")
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}", exc_info=True)
        shutil.rmtree(run_dir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

    # optionally save to disk and return a file path
    if payload.save_to_disk:
        out_path = os.path.join(run_dir, "result.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        # return path (or you can return download link if hosting static serving)
        return {"status":"ok","result_path":out_path, "results_preview": results[:3]}
    # else return JSON directly
    # cleanup temp dir (optional: keep for debug)
    shutil.rmtree(run_dir, ignore_errors=True)
    return results

@app.get("/health")
def health():
    return {"status":"ok","time": datetime.utcnow().isoformat()}

@app.post("/debug")
def debug(payload: dict = Body(...), x_api_key: Optional[str] = Header(None)):
    """Debug endpoint to see exact raw payload"""
    if not auth_ok(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return {
        "raw_payload": payload,
        "keys_received": list(payload.keys()),
        "types": {k: str(type(v)) for k, v in payload.items()}
    }

@app.post("/process-and-download")
def process_and_download(payload: ProcessRequest = Body(...), x_api_key: Optional[str] = Header(None)):
    """Process and return as downloadable attachment (Content-Disposition)."""
    if not auth_ok(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    payload.save_to_disk = True
    # reuse process_files to create file
    res = process_files(payload, x_api_key)
    # if run gave path
    path = res.get("result_path")
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=500, detail="Result file not found")
    with open(path, "rb") as f:
        content = f.read()
    # stream back as attachment
    headers = {
        "Content-Disposition": f'attachment; filename="result_{int(time.time())}.json"'
    }
    return Response(content, media_type="application/json", headers=headers)