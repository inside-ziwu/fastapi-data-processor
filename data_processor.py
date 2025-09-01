import os
import shutil
import logging
from typing import Optional, Dict
import polars as pl
import pandas as pd
import re
import json

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# 字段映射（输入部分，中文表头 -> 英文标准字段）
VIDEO_MAP = {
    "主机厂经销商id": "NSC_CODE", "日期": "date",
    "锚点曝光次数": "anchor_exposure", "锚点点击次数": "component_clicks",
    "新发布视频数": "short_video_count", "短视频表单提交商机量": "short_video_leads"
}
LIVE_MAP = {
    "主机厂经销商id列表": "NSC_CODE", "开播日期": "date",
    "超25分钟直播时长(分)": "over25_min_live_mins", "直播有效时长（小时）": "live_effective_hours",
    "超25min直播总场次": "effective_live_sessions", "曝光人数": "exposures", "场观": "viewers",
    "小风车点击次数（不含小雪花）": "small_wheel_clicks"
}
MSG_MAP = {
    "主机厂经销商ID": "NSC_CODE", "日期": "date",
    "进入私信客户数": "enter_private_count",
    "主动咨询客户数": "private_open_count",
    "私信留资客户数": "private_leads_count"
}
ACCOUNT_BI_MAP = {
    "主机厂经销商id列表": "NSC_CODE", "日期": "date", "直播间表单提交商机量": "live_leads", "短-播放量": "short_video_plays"
}
LEADS_MAP = {
    "主机厂经销商id列表": "NSC_CODE", "留资日期": "date", "直播间表单提交商机量(去重)": "small_wheel_leads"
}
DR_MAP = {
    "reg_dealer": "NSC_CODE", "register_time": "date", "leads_type": "leads_type",
    "mkt_second_channel_name": "mkt_second_channel_name", "send2dealer_id": "send2dealer_id"
}
SPENDING_MAP = {"NSC CODE": "NSC_CODE", "Date": "date", "Spending(Net)": "spending_net"}
ACCOUNT_BASE_MAP = {"NSC_id": "NSC_CODE", "第二期层级": "level", "NSC Code": "NSC_Code", "抖音id": "store_name"}

# 输出字段英文化映射（中英文对照表）
FIELD_EN_MAP = {
    "车云店+区域综合CPL": "total_cpl",
    "付费CPL（车云店+区域）": "paid_cpl",
    "本地线索占比": "local_leads_ratio",
    "直播车云店+区域日均消耗": "avg_daily_spending",
    "T月直播车云店+区域日均消耗": "avg_daily_spending_t",
    "T-1月直播车云店+区域日均消耗": "avg_daily_spending_t_minus_1",
    "直播车云店+区域付费线索量日均": "avg_daily_paid_leads",
    "T月直播车云店+区域付费线索量日均": "avg_daily_paid_leads_t",
    "T-1月直播车云店+区域付费线索量日均": "avg_daily_paid_leads_t_minus_1",
    "T月直播付费CPL": "paid_cpl_t",
    "T-1月直播付费CPL": "paid_cpl_t_minus_1",
    "有效（25min以上）时长（h）": "effective_live_hours_25min",
    "T月有效（25min以上）时长（h）": "effective_live_hours_25min_t",
    "T-1月有效（25min以上）时长（h）": "effective_live_hours_25min_t_minus_1",
    "日均有效（25min以上）时长（h）": "avg_daily_effective_live_hours_25min",
    "T月日均有效（25min以上）时长（h）": "avg_daily_effective_live_hours_25min_t",
    "T-1月日均有效（25min以上）时长（h）": "avg_daily_effective_live_hours_25min_t_minus_1",
    "场均曝光人数": "avg_exposures_per_session",
    "T月场均曝光人数": "avg_exposures_per_session_t",
    "T-1月场均曝光人数": "avg_exposures_per_session_t_minus_1",
    "曝光进入率": "exposure_to_viewer_rate",
    "T月曝光进入率": "exposure_to_viewer_rate_t",
    "T-1月曝光进入率": "exposure_to_viewer_rate_t_minus_1",
    "场均场观": "avg_viewers_per_session",
    "T月场均场观": "avg_viewers_per_session_t",
    "T-1月场均场观": "avg_viewers_per_session_t_minus_1",
    "小风车点击率": "small_wheel_click_rate",
    "T月小风车点击率": "small_wheel_click_rate_t",
    "T-1月小风车点击率": "small_wheel_click_rate_t_minus_1",
    "小风车点击留资率": "small_wheel_leads_rate",
    "T月小风车点击留资率": "small_wheel_leads_rate_t",
    "T-1月小风车点击留资率": "small_wheel_leads_rate_t_minus_1",
    "场均小风车留资量": "avg_small_wheel_leads_per_session",
    "T月场均小风车留资量": "avg_small_wheel_leads_per_session_t",
    "T-1月场均小风车留资量": "avg_small_wheel_leads_per_session_t_minus_1",
    "组件点击率": "component_click_rate",
    "T月组件点击率": "component_click_rate_t",
    "T-1月组件点击率": "component_click_rate_t_minus_1",
    "组件留资率": "component_leads_rate",
    "T月组件留资率": "component_leads_rate_t",
    "T-1月组件留资率": "component_leads_rate_t_minus_1",
    "日均进私人数": "avg_daily_private_entry_count",
    "T月日均进私人数": "avg_daily_private_entry_count_t",
    "T-1月日均进私人数": "avg_daily_private_entry_count_t_minus_1",
    "日均私信开口人数": "avg_daily_private_open_count",
    "T月日均私信开口人数": "avg_daily_private_open_count_t",
    "T-1月日均私信开口人数": "avg_daily_private_open_count_t_minus_1",
    "日均咨询留资人数": "avg_daily_private_leads_count",
    "T月日均咨询留资人数": "avg_daily_private_leads_count_t",
    "T-1月日均咨询留资人数": "avg_daily_private_leads_count_t_minus_1",
    "私信咨询率": "private_open_rate",
    "T月私信咨询率": "private_open_rate_t",
    "T-1月私信咨询率": "private_open_rate_t_minus_1",
    "咨询留资率": "private_leads_rate",
    "T月咨询留资率": "private_leads_rate_t",
    "T-1月咨询留资率": "private_leads_rate_t_minus_1",
    "私信转化率": "private_conversion_rate",
    "T月私信转化率": "private_conversion_rate_t",
    "T-1月私信转化率": "private_conversion_rate_t_minus_1"
}

def normalize_symbol(s: str) -> str:
    return (
        s.replace("（", "(")
         .replace("）", ")")
         .replace("，", ",")
         .replace("。", ".")
         .replace("【", "[")
         .replace("】", "]")
         .replace("“", "\"")
         .replace("”", "\"")
         .replace("‘", "'")
         .replace("’", "'")
         .replace("：", ":")
         .replace("；", ";")
         .replace("、", "/")
         .replace("—", "-")
         .replace("－", "-")
         .replace("　", "")
         .replace(" ", "")
    )

def split_chinese_english(s: str):
    parts = re.findall(r'[\u4e00-\u9fff]+|[^\u4e00-\u9fff]+', s)
    return parts

def field_match(src: str, col: str) -> bool:
    src_norm = normalize_symbol(src)
    col_norm = normalize_symbol(col)
    src_parts = split_chinese_english(src_norm)
    col_parts = split_chinese_english(col_norm)
    logger.debug(f"field_match: src='{src}', col='{col}', src_parts={src_parts}, col_parts={col_parts}")
    if len(src_parts) != len(col_parts):
        return False
    for s, c in zip(src_parts, col_parts):
        if s.lower() != c.lower():
            logger.debug(f"片段不匹配: '{s.lower()}' != '{c.lower()}'")
            return False
    return True

def rename_columns_loose(pl_df: pl.DataFrame, mapping: Dict[str, str]) -> pl.DataFrame:
    col_map = {}
    for src, dst in mapping.items():
        for c in pl_df.columns:
            match = field_match(src, c)
            logger.debug(f"字段映射尝试: 源字段='{src}'，目标字段='{c}'，match={match}")
            if match:
                col_map[c] = dst
                break
    logger.debug(f"最终映射关系: {col_map}")
    pl_df = pl_df.rename(col_map)
    logger.debug(f"重命名后列名: {pl_df.columns}")
    return pl_df

def read_csv_polars(path: str) -> pl.DataFrame:
    return pl.read_csv(path, try_parse_dates=False, low_memory=False)

def read_excel_as_pandas(path: str, sheet_name=None) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")

def df_pandas_to_polars(df: pd.DataFrame) -> pl.DataFrame:
    for col in df.columns:
        df[col] = df[col].astype(str)
    return pl.from_pandas(df)

def normalize_nsc_col(df: pl.DataFrame, colname: str = "NSC_CODE") -> pl.DataFrame:
    if colname not in df.columns:
        df = df.with_columns(pl.lit(None).alias(colname))
    df = df.with_columns(pl.col(colname).cast(pl.Utf8))
    df = df.with_columns(
        pl.col(colname)
        .str.replace_all(r"[；，\|\u3001/\\]+", ",")
        .alias(colname)
    )
    df = df.with_columns(pl.col(colname).str.split(",").alias("_nsc_list"))
    df = df.explode("_nsc_list")
    df = df.with_columns(
        pl.col("_nsc_list").str.strip_chars().alias("NSC_CODE_CLEAN")
    )
    if colname in df.columns:
        df = df.drop([colname, "_nsc_list"])
    else:
        df = df.drop(["_nsc_list"])
    df = df.rename({"NSC_CODE_CLEAN": "NSC_CODE"})
    df = df.filter(pl.col("NSC_CODE").is_not_null() & (pl.col("NSC_CODE") != ""))
    return df

def ensure_date_column(pl_df: pl.DataFrame, colname: str = "date") -> pl.DataFrame:
    if colname not in pl_df.columns:
        return pl_df.with_columns(pl.lit(None, dtype=pl.Date).alias(colname))
    formats_to_try = [
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d",
    ]
    parser_expressions = [
        pl.col(colname).str.strptime(pl.Datetime, format=fmt, strict=False)
        for fmt in formats_to_try
    ]
    pl_df = pl_df.with_columns(
        pl.coalesce(parser_expressions)
        .cast(pl.Date)
        .alias(colname)
    )
    return pl_df

def try_cast_numeric(pl_df: pl.DataFrame, cols):
    for c in cols:
        if c in pl_df.columns:
            pl_df = pl_df.with_columns(
                pl.col(c).cast(pl.Float64, strict=False).fill_null(0).fill_nan(0)
            )
    return pl_df

def process_single_table(df, mapping, sum_cols=None):
    orig_cols = list(df.columns)
    logger.debug(f"[process_single_table] 原始列名: {orig_cols}")
    df = rename_columns_loose(df, mapping)
    renamed_cols = list(df.columns)
    logger.debug(f"[process_single_table] 重命名后列名: {renamed_cols}")
    if "NSC_CODE" not in df.columns:
        hit_report = {}
        for src, dst in mapping.items():
            hit = False
            for c in orig_cols:
                if field_match(src, c):
                    hit = True
                    break
            hit_report[src] = {"expected": dst, "matched": hit}
        err_msg = (
            "[process_single_table] 重命名后未发现 NSC_CODE。\n"
            f"  - 原始列名: {orig_cols}\n"
            f"  - 重命名后列名: {renamed_cols}\n"
            f"  - 映射命中报告: {hit_report}\n"
            "请确认主键源字段（例如：主机厂经销商ID）是否在 mapping 中，"
            "且与实际表头中文部分完全一致。"
        )
        logger.error(err_msg)
        raise ValueError(err_msg)
    df = normalize_nsc_col(df, "NSC_CODE")
    after_norm_cols = list(df.columns)
    logger.debug(f"[process_single_table] 标准化 NSC_CODE 后列名: {after_norm_cols}")
    if "NSC_CODE" not in df.columns:
        logger.error("[process_single_table] normalize_nsc_col 之后 NSC_CODE 列丢失。")
        raise ValueError("[process_single_table] normalize_nsc_col 之后 NSC_CODE 列丢失。")
    non_null_cnt = df.filter(pl.col("NSC_CODE").is_not_null() & (pl.col("NSC_CODE") != "")).height
    if non_null_cnt == 0:
        logger.error(f"[process_single_table] NSC_CODE 标准化后为空。非空行计数={non_null_cnt}")
        raise ValueError(f"[process_single_table] NSC_CODE 标准化后为空。非空行计数={non_null_cnt}")
    df = ensure_date_column(df, "date")
    if "date" not in df.columns:
        logger.error("[process_single_table] ensure_date_column 之后未发现 date 列。")
        raise ValueError("[process_single_table] ensure_date_column 之后未发现 date 列。")
    if sum_cols:
        df = try_cast_numeric(df, sum_cols)
    group_cols = ["NSC_CODE", "date"]
    agg_exprs = [pl.col(c).sum().alias(c) for c in sum_cols] if sum_cols else []
    if agg_exprs:
        df = df.group_by(group_cols).agg(agg_exprs)
    else:
        df = df.unique(subset=group_cols)
    logger.debug(f"[process_single_table] 完成。输出列名: {list(df.columns)}, 行数={df.height}")
    return df

def process_dr_table(df):
    logger.debug(f"[DR PROBE] Initial columns: {df.columns}")
    logger.debug(f"[DR PROBE] Initial head:\n{df.head()}")
    df = rename_columns_loose(df, DR_MAP)
    logger.debug(f"[DR PROBE] Columns after rename: {df.columns}")
    df = normalize_nsc_col(df, "NSC_CODE")
    df = ensure_date_column(df, "date")
    
    if "leads_type" in df.columns:
        logger.debug(f"[DR PROBE] Unique 'leads_type' values: {df.get_column('leads_type').unique().to_list()}")
    if "mkt_second_channel_name" in df.columns:
        logger.debug(f"[DR PROBE] Unique 'mkt_second_channel_name' values: {df.get_column('mkt_second_channel_name').unique().to_list()}")

    dr_all = df.group_by("NSC_CODE", "date").agg(
        pl.col("leads_type").filter(pl.col("leads_type") == "自然").count().alias("natural_leads"),
        pl.col("leads_type").filter(pl.col("leads_type") == "广告").count().alias("ad_leads"),
        pl.col("leads_type").filter(
            (pl.col("leads_type") == "广告") &
            (pl.col("mkt_second_channel_name").is_in(["抖音车云店_BMW_本市_LS直发", "抖音车云店_LS直发"]))
        ).count().alias("paid_leads"),
        pl.col("leads_type").filter(
            (pl.col("leads_type") == "广告") &
            (pl.col("mkt_second_channel_name") == "抖音车云店_BMW_总部BDT_LS直发")
        ).count().alias("area_leads"),
        pl.col("send2dealer_id").filter(
            pl.col("send2dealer_id").cast(pl.Utf8) == pl.col("NSC_CODE")
        ).count().alias("local_leads")
    )
    logger.debug(f"[DR PROBE] Final aggregated DR data:\n{dr_all.head()}")
    return dr_all

def process_account_base(all_sheets):
    level_df, store_name_df = None, None

    # Define the exact column names for each sheet
    level_sheet_keys = ["NSC_id", "第二期层级"]
    store_sheet_keys = ["NSC Code", "抖音id"]

    for sheetname, pdf in all_sheets.items():
        df_cols = pdf.columns
        
        # Check if it's the level sheet
        if all(key in df_cols for key in level_sheet_keys):
            logger.info(f"[ACC_BASE] Identified level sheet: {sheetname}")
            level_df = df_pandas_to_polars(pdf).select(
                pl.col("NSC_id").alias("NSC_CODE"),
                pl.col("第二期层级").alias("level")
            )
        
        # Check if it's the store name sheet
        elif all(key in df_cols for key in store_sheet_keys):
            logger.info(f"[ACC_BASE] Identified store_name sheet: {sheetname}")
            store_name_df = df_pandas_to_polars(pdf).select(
                pl.col("NSC Code").alias("NSC_CODE"),
                pl.col("抖音id").alias("store_name")
            )

    if level_df is None and store_name_df is None:
        logger.warning("[ACC_BASE] Could not find valid level or store_name sheets in account_base_file.")
        return None

    # Join the two dataframes
    if level_df is not None and store_name_df is not None:
        merged = level_df.join(store_name_df, on="NSC_CODE", how="outer")
    elif level_df is not None:
        merged = level_df
    else:
        merged = store_name_df
    
    if merged is not None and "NSC_CODE" in merged.columns:
         merged = merged.filter(pl.col("NSC_CODE").is_not_null() & (pl.col("NSC_CODE") != ""))

    logger.debug(f"[ACC_BASE PROBE] Final merged account_base data:\n{merged.head() if merged is not None else 'None'}")
    return merged

def process_all_files(local_paths: Dict[str, str], spending_sheet_names: Optional[str] = None) -> pl.DataFrame:
    logger.info(f"Entering process_all_files with {len(local_paths)} files")
    dfs = {}
    # 1. video_excel_file
    if "video_excel_file" in local_paths:
        p = local_paths["video_excel_file"]
        df = None
        if '.csv' in p.lower() or '.txt' in p.lower():
            df = read_csv_polars(p)
        elif '.xlsx' in p.lower() or '.xls' in p.lower():
            pdf = read_excel_as_pandas(p, sheet_name=0)
            df = df_pandas_to_polars(pdf)
        if df is not None:
            logger.debug(f"[video] columns={df.columns}")
            dfs["video"] = process_single_table(df, VIDEO_MAP, ["anchor_exposure","component_clicks","short_video_count","short_video_leads"])
    # 2. live_bi_file
    if "live_bi_file" in local_paths:
        p = local_paths["live_bi_file"]
        df = None
        if '.csv' in p.lower() or '.txt' in p.lower():
            df = read_csv_polars(p)
        elif '.xlsx' in p.lower() or '.xls' in p.lower():
            pdf = read_excel_as_pandas(p, sheet_name=0)
            df = df_pandas_to_polars(pdf)
        if df is not None:
            logger.debug(f"[live] columns={df.columns}")
            dfs["live"] = process_single_table(df, LIVE_MAP, ["over25_min_live_mins","live_effective_hours","effective_live_sessions","exposures","viewers","small_wheel_clicks"])
    # 3. msg_excel_file
    if "msg_excel_file" in local_paths:
        p = local_paths["msg_excel_file"]
        all_sheets = read_excel_as_pandas(p, sheet_name=None)
        per_sheet_frames = []
        sheet_report = []
        for sheetname, pdf in all_sheets.items():
            orig_cols = list(map(str, pdf.columns))
            logger.debug(f"[MSG调试] sheet={sheetname}, columns={orig_cols}")
            PK_COLUMN = "主机厂经销商ID"
            if PK_COLUMN not in pdf.columns:
                sheet_report.append({
                    "sheet": sheetname,
                    "orig_cols": orig_cols,
                    "pk_found": "未找到必需主键",
                })
                err_msg = (
                    f"MSG sheet 格式无效。sheet='{sheetname}' "
                    f"缺少必需的主键列: '{PK_COLUMN}'。实际列: {orig_cols}"
                )
                logger.error(err_msg)
                raise ValueError(err_msg)
            pdf_local = pdf.copy()
            pdf_local["日期"] = sheetname
            rename_map = {
                PK_COLUMN: "NSC_CODE",
                "日期": "date",
                "进入私信客户数": "enter_private_count",
                "主动咨询客户数": "private_open_count",
                "私信留资客户数": "private_leads_count",
            }
            rename_map_eff = {k: v for k, v in rename_map.items() if k in pdf_local.columns}
            pdf_local = pdf_local.rename(columns=rename_map_eff)
            df = df_pandas_to_polars(pdf_local)
            logger.debug(f"[MSG调试] sheet={sheetname}, polars columns={df.columns}")
            df = normalize_nsc_col(df, "NSC_CODE")
            df = ensure_date_column(df, "date")
            keep = [c for c in ["NSC_CODE","date","enter_private_count","private_open_count","private_leads_count"] if c in df.columns]
            df = df.select(keep)
            per_sheet_frames.append(df)
            sheet_status = {
                "sheet": sheetname,
                "orig_cols": orig_cols,
                "pk_found": PK_COLUMN,
                "kept_cols": keep
            }
            sheet_report.append(sheet_status)
        if not per_sheet_frames:
            logger.error("[MSG] per_sheet_frames 为空，未生成 msg df")
            raise ValueError("[MSG] per_sheet_frames 为空，未生成 msg df")
        df = pl.concat(per_sheet_frames, how="vertical")
        logger.debug(f"[MSG合并后] columns={df.columns}")
        group_cols = ["NSC_CODE", "date"]
        sum_cols = ["enter_private_count","private_open_count","private_leads_count"]
        for col in sum_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False).fill_null(0))
        agg_exprs = [pl.col(c).sum().alias(c) for c in sum_cols if c in df.columns]
        if agg_exprs:
            df = df.group_by(group_cols).agg(agg_exprs)
        else:
            df = df.unique(subset=group_cols)
        logger.debug(f"[MSG聚合后] columns={df.columns}")
        if "NSC_CODE" not in df.columns:
            logger.error(f"[MSG聚合后] 没有 NSC_CODE，实际列: {df.columns}")
            raise ValueError(f"[MSG聚合后] 没有 NSC_CODE，实际列: {df.columns}")
        dfs["msg"] = df
        logger.info(f"[MSG 严格模式报告] {sheet_report}")
    # 4. account_bi_file
    if "account_bi_file" in local_paths:
        p = local_paths["account_bi_file"]
        df = None
        if '.csv' in p.lower() or '.txt' in p.lower():
            df = read_csv_polars(p)
        elif '.xlsx' in p.lower() or '.xls' in p.lower():
            pdf = read_excel_as_pandas(p, sheet_name=0)
            df = df_pandas_to_polars(pdf)
        if df is not None:
            logger.debug(f"[ACC_BI PROBE] Initial columns: {df.columns}")
            logger.debug(f"[ACC_BI PROBE] Initial head:\n{df.head()}")
            dfs["account_bi"] = process_single_table(df, ACCOUNT_BI_MAP, ["live_leads","short_video_plays"])
            logger.debug(f"[ACC_BI PROBE] Processed account_bi data:\n{dfs['account_bi'].head()}")
    # 5. leads_file
    if "leads_file" in local_paths:
        p = local_paths["leads_file"]
        df = None
        if '.csv' in p.lower() or '.txt' in p.lower():
            df = read_csv_polars(p)
        elif '.xlsx' in p.lower() or '.xls' in p.lower():
            pdf = read_excel_as_pandas(p, sheet_name=0)
            df = df_pandas_to_polars(pdf)
        if df is not None:
            logger.debug(f"[leads] columns={df.columns}")
            dfs["leads"] = process_single_table(df, LEADS_MAP, ["small_wheel_leads"])
    # 6. DR1_file, DR2_file
    dr_frames = []
    for key in ["DR1_file", "DR2_file"]:
        if key in local_paths:
            p = local_paths[key]
            if '.csv' in p.lower() or '.txt' in p.lower():
                df = read_csv_polars(p)
                dr_frames.append(df)
            else:
                logger.warning(f"{key} 只支持CSV格式，已跳过: {p}")
    if dr_frames:
        dr_df = pl.concat(dr_frames, how="vertical")
        logger.debug(f"[dr] columns={dr_df.columns}")
        dfs["dr"] = process_dr_table(dr_df)
    # 7. Spending_file
    if "Spending_file" in local_paths:
        p = local_paths["Spending_file"]
        df = None
        if '.csv' in p.lower() or '.txt' in p.lower():
            df = read_csv_polars(p)
        elif '.xlsx' in p.lower() or '.xls' in p.lower():
            sheet_names = None
            if spending_sheet_names:
                normalized_sheet_names = spending_sheet_names.replace("，", ",")
                sheet_names = [s.strip() for s in normalized_sheet_names.split(",") if s.strip()]
            if sheet_names:
                pdfs = []
                for s in sheet_names:
                    try:
                        pdf = read_excel_as_pandas(p, sheet_name=s)
                        pdfs.append(pdf)
                    except Exception as e:
                        logger.warning(f"Spending_file sheet {s} not found: {e}")
                        continue
                if not pdfs:
                    pdfs = [read_excel_as_pandas(p, sheet_name=0)]
                big_pdf = pd.concat(pdfs, ignore_index=True)
            else:
                all_sheets = read_excel_as_pandas(p, sheet_name=None)
                big_pdf = pd.concat(all_sheets.values(), ignore_index=True)
            df = df_pandas_to_polars(big_pdf)
        if df is not None:
            logger.debug(f"[spending] columns={df.columns}")
            dfs["spending"] = process_single_table(df, SPENDING_MAP, ["spending_net"])
    # 8. account_base_file
    account_base = None
    if "account_base_file" in local_paths:
        p = local_paths["account_base_file"]
        all_sheets = read_excel_as_pandas(p, sheet_name=None)
        account_base = process_account_base(all_sheets)
    if not dfs:
        raise ValueError("No dataframes were processed. Check file inputs and names.")
    keys = []
    for k, v in dfs.items():
        logger.debug(f"[keys] {k} columns={v.columns}")
        if "NSC_CODE" not in v.columns:
            logger.error(f"[keys] {k} 没有 NSC_CODE，实际列: {v.columns}")
        if "NSC_CODE" in v.columns and "date" in v.columns:
            keys.append(v.select(["NSC_CODE", "date"]).unique())
    logger.debug(f"[keys] keys count={len(keys)}")
    if not keys:
        raise ValueError("No frames with NSC_CODE + date found.")
    base = pl.concat(keys).unique().sort(["NSC_CODE", "date"])
    logger.debug(f"[最终合并] base.columns={base.columns}")

    # --- JOIN PROBE ---
    logger.debug(f"[JOIN PROBE] Schema of base df: {base.schema}")
    logger.debug(f"[JOIN PROBE] Head of base df:\n{base.head()}")
    if "dr" in dfs:
        logger.debug(f"[JOIN PROBE] Schema of dfs['dr']: {dfs['dr'].schema}")
        logger.debug(f"[JOIN PROBE] Head of dfs['dr']:\n{dfs['dr'].head()}")
    # --- END JOIN PROBE ---

    for name, df in dfs.items():
        logger.debug(f"[join] {name} columns={df.columns}")
        if "date" in df.columns:
            base = base.join(df, on=["NSC_CODE", "date"], how="left")
        else:
            base = base.join(df, on="NSC_CODE", how="left")
        logger.debug(f"[join后] base.columns={base.columns}")
    if account_base is not None:
        base = base.join(account_base, on="NSC_CODE", how="left")
        logger.debug(f"[account_base join后] base.columns={base.columns}")
    # --- FINAL ANALYSIS LOGIC --- #
    logger.info("开始最终分析前的数据清洗...")
    base = base.filter(pl.col("date").is_not_null())
    id_cols_to_fill = ["level", "store_name"]
    for col in id_cols_to_fill:
        if col in base.columns:
            base = base.with_columns(pl.col(col).fill_null("unknown"))
    logger.info("数据清洗完成")
    logger.info("开始进行T/T-1分析...")
    unique_months = base.get_column("date").dt.month().unique().sort(descending=True)
    if len(unique_months) < 2:
        raise ValueError("数据不足两个月，无法进行T/T-1对比分析。")
    month_t = unique_months[0]
    month_t_minus_1 = unique_months[1]
    logger.info(f"T周期月份: {month_t}, T-1周期月份: {month_t_minus_1}")
    base = base.with_columns(
        period=pl.when(pl.col("date").dt.month() == month_t).then(pl.lit("T"))
                 .when(pl.col("date").dt.month() == month_t_minus_1).then(pl.lit("T-1"))
                 .otherwise(pl.lit(None))
    ).filter(pl.col("period").is_not_null())
    t_days = base.filter(pl.col("period") == "T").select(pl.col("date").n_unique()).item() or 1
    t_minus_1_days = base.filter(pl.col("period") == "T-1").select(pl.col("date").n_unique()).item() or 1
    logger.info(f"T周期有效天数: {t_days}, T-1周期有效天数: {t_minus_1_days}")
    agg_cols = [
        'anchor_exposure', 'component_clicks', 'short_video_count', 'short_video_leads',
        'over25_min_live_mins', 'live_effective_hours', 'effective_live_sessions', 'exposures',
        'viewers', 'small_wheel_clicks', 'enter_private_count', 'private_open_count',
        'private_leads_count', 'live_leads', 'short_video_plays', 'small_wheel_leads',
        'natural_leads', 'ad_leads', 'paid_leads', 'area_leads', 'local_leads', 'spending_net'
    ]
    agg_cols_exist = [c for c in agg_cols if c in base.columns]
    id_cols = ["NSC_CODE", "level", "store_name"]
    id_cols_exist = [c for c in id_cols if c in base.columns]
    summary_df = base.pivot(
        index=id_cols_exist,
        columns="period",
        values=agg_cols_exist,
        aggregate_function="sum"
    )
    # Step 3: Calculate Totals
    for col in agg_cols_exist:
        summary_df = summary_df.with_columns(
            (pl.col(f"{col}_T").fill_null(0) + pl.col(f"{col}_T-1").fill_null(0)).alias(f"{col}_total"),
            pl.col(f"{col}_T").alias(f"{col}_t"),
            pl.col(f"{col}_T-1").alias(f"{col}_t_minus_1")
        )
    # Step 4: Calculate Intermediate Derived Metrics (全部英文)
    summary_df = summary_df.with_columns(
        (pl.col("paid_leads_total").fill_null(0) + pl.col("area_leads_total").fill_null(0)).alias("paid_area_leads_total"),
        (pl.col("paid_leads_t").fill_null(0) + pl.col("area_leads_t").fill_null(0)).alias("paid_area_leads_t"),
        (pl.col("paid_leads_t_minus_1").fill_null(0) + pl.col("area_leads_t_minus_1").fill_null(0)).alias("paid_area_leads_t_minus_1"),
        (pl.col("over25_min_live_mins_total") / 60).alias("effective_live_hours_25min"),
        (pl.col("over25_min_live_mins_t") / 60).alias("effective_live_hours_25min_t"),
        (pl.col("over25_min_live_mins_t_minus_1") / 60).alias("effective_live_hours_25min_t_minus_1")
    )
    # Step 5: Calculate Final Ratios (全部英文)
    def safe_div(num_expr, den_expr):
        return pl.when(den_expr != 0).then(num_expr / den_expr).otherwise(None)
    summary_df = summary_df.with_columns(
        safe_div(pl.col("spending_net_total"), (pl.col("natural_leads_total").fill_null(0) + pl.col("ad_leads_total").fill_null(0))).alias("total_cpl"),
        safe_div(pl.col("spending_net_total"), pl.col("paid_area_leads_total")).alias("paid_cpl"),
        safe_div(pl.col("local_leads_total"), (pl.col("natural_leads_total").fill_null(0) + pl.col("ad_leads_total").fill_null(0))).alias("local_leads_ratio"),
        safe_div(pl.col("spending_net_total"), (t_days + t_minus_1_days)).alias("avg_daily_spending"),
        safe_div(pl.col("spending_net_t"), t_days).alias("avg_daily_spending_t"),
        safe_div(pl.col("spending_net_t_minus_1"), t_minus_1_days).alias("avg_daily_spending_t_minus_1"),
        safe_div(pl.col("paid_area_leads_total"), (t_days + t_minus_1_days)).alias("avg_daily_paid_leads"),
        safe_div(pl.col("paid_area_leads_t"), t_days).alias("avg_daily_paid_leads_t"),
        safe_div(pl.col("paid_area_leads_t_minus_1"), t_minus_1_days).alias("avg_daily_paid_leads_t_minus_1"),
        safe_div(pl.col("spending_net_t"), pl.col("paid_area_leads_t")).alias("paid_cpl_t"),
        safe_div(pl.col("spending_net_t_minus_1"), pl.col("paid_area_leads_t_minus_1")).alias("paid_cpl_t_minus_1"),
        safe_div(pl.col("effective_live_hours_25min"), (t_days + t_minus_1_days)).alias("avg_daily_effective_live_hours_25min"),
        safe_div(pl.col("effective_live_hours_25min_t"), t_days).alias("avg_daily_effective_live_hours_25min_t"),
        safe_div(pl.col("effective_live_hours_25min_t_minus_1"), t_minus_1_days).alias("avg_daily_effective_live_hours_25min_t_minus_1"),
        safe_div(pl.col("exposures_total"), pl.col("effective_live_sessions_total")).alias("avg_exposures_per_session"),
        safe_div(pl.col("exposures_t"), pl.col("effective_live_sessions_t")).alias("avg_exposures_per_session_t"),
        safe_div(pl.col("exposures_t_minus_1"), pl.col("effective_live_sessions_t_minus_1")).alias("avg_exposures_per_session_t_minus_1"),
        safe_div(pl.col("viewers_total"), pl.col("exposures_total")).alias("exposure_to_viewer_rate"),
        safe_div(pl.col("viewers_t"), pl.col("exposures_t")).alias("exposure_to_viewer_rate_t"),
        safe_div(pl.col("viewers_t_minus_1"), pl.col("exposures_t_minus_1")).alias("exposure_to_viewer_rate_t_minus_1"),
        safe_div(pl.col("viewers_total"), pl.col("effective_live_sessions_total")).alias("avg_viewers_per_session"),
        safe_div(pl.col("viewers_t"), pl.col("effective_live_sessions_t")).alias("avg_viewers_per_session_t"),
        safe_div(pl.col("viewers_t_minus_1"), pl.col("effective_live_sessions_t_minus_1")).alias("avg_viewers_per_session_t_minus_1"),
        safe_div(pl.col("small_wheel_clicks_total"), pl.col("viewers_total")).alias("small_wheel_click_rate"),
        safe_div(pl.col("small_wheel_clicks_t"), pl.col("viewers_t")).alias("small_wheel_click_rate_t"),
        safe_div(pl.col("small_wheel_clicks_t_minus_1"), pl.col("viewers_t_minus_1")).alias("small_wheel_click_rate_t_minus_1"),
        safe_div(pl.col("small_wheel_leads_total"), pl.col("small_wheel_clicks_total")).alias("small_wheel_leads_rate"),
        safe_div(pl.col("small_wheel_leads_t"), pl.col("small_wheel_clicks_t")).alias("small_wheel_leads_rate_t"),
        safe_div(pl.col("small_wheel_leads_t_minus_1"), pl.col("small_wheel_clicks_t_minus_1")).alias("small_wheel_leads_rate_t_minus_1"),
        safe_div(pl.col("small_wheel_leads_total"), pl.col("effective_live_sessions_total")).alias("avg_small_wheel_leads_per_session"),
        safe_div(pl.col("small_wheel_leads_t"), pl.col("effective_live_sessions_t")).alias("avg_small_wheel_leads_per_session_t"),
        safe_div(pl.col("small_wheel_leads_t_minus_1"), pl.col("effective_live_sessions_t_minus_1")).alias("avg_small_wheel_leads_per_session_t_minus_1"),
        safe_div(pl.col("component_clicks_total"), pl.col("anchor_exposure_total")).alias("component_click_rate"),
        safe_div(pl.col("component_clicks_t"), pl.col("anchor_exposure_t")).alias("component_click_rate_t"),
        safe_div(pl.col("component_clicks_t_minus_1"), pl.col("anchor_exposure_t_minus_1")).alias("component_click_rate_t_minus_1"),
        safe_div(pl.col("short_video_leads_total"), pl.col("anchor_exposure_total")).alias("component_leads_rate"),
        safe_div(pl.col("short_video_leads_t"), pl.col("anchor_exposure_t")).alias("component_leads_rate_t"),
        safe_div(pl.col("short_video_leads_t_minus_1"), pl.col("anchor_exposure_t_minus_1")).alias("component_leads_rate_t_minus_1"),
        safe_div(pl.col("enter_private_count_total"), (t_days + t_minus_1_days)).alias("avg_daily_private_entry_count"),
        safe_div(pl.col("enter_private_count_t"), t_days).alias("avg_daily_private_entry_count_t"),
        safe_div(pl.col("enter_private_count_t_minus_1"), t_minus_1_days).alias("avg_daily_private_entry_count_t_minus_1"),
        safe_div(pl.col("private_open_count_total"), (t_days + t_minus_1_days)).alias("avg_daily_private_open_count"),
        safe_div(pl.col("private_open_count_t"), t_days).alias("avg_daily_private_open_count_t"),
        safe_div(pl.col("private_open_count_t_minus_1"), t_minus_1_days).alias("avg_daily_private_open_count_t_minus_1"),
        safe_div(pl.col("private_leads_count_total"), (t_days + t_minus_1_days)).alias("avg_daily_private_leads_count"),
        safe_div(pl.col("private_leads_count_t"), t_days).alias("avg_daily_private_leads_count_t"),
        safe_div(pl.col("private_leads_count_t_minus_1"), t_minus_1_days).alias("avg_daily_private_leads_count_t_minus_1"),
        safe_div(pl.col("private_open_count_total"), pl.col("enter_private_count_total")).alias("private_open_rate"),
        safe_div(pl.col("private_open_count_t"), pl.col("enter_private_count_t")).alias("private_open_rate_t"),
        safe_div(pl.col("private_open_count_t_minus_1"), pl.col("enter_private_count_t_minus_1")).alias("private_open_rate_t_minus_1"),
        safe_div(pl.col("private_leads_count_total"), pl.col("private_open_count_total")).alias("private_leads_rate"),
        safe_div(pl.col("private_leads_count_t"), pl.col("private_open_count_t")).alias("private_leads_rate_t"),
        safe_div(pl.col("private_leads_count_t_minus_1"), pl.col("private_open_count_t_minus_1")).alias("private_leads_rate_t_minus_1"),
        safe_div(pl.col("private_leads_count_total"), pl.col("enter_private_count_total")).alias("private_conversion_rate"),
        safe_div(pl.col("private_leads_count_t"), pl.col("enter_private_count_t")).alias("private_conversion_rate_t"),
        safe_div(pl.col("private_leads_count_t_minus_1"), pl.col("enter_private_count_t_minus_1")).alias("private_conversion_rate_t_minus_1"),
    )
    # Step 6: Final Selection (全部英文)
    final_columns = [
        "NSC_CODE", "level", "store_name",
        "natural_leads_total", "natural_leads_t", "natural_leads_t_minus_1",
        "ad_leads_total", "ad_leads_t", "ad_leads_t_minus_1",
        "spending_net_total", "spending_net_t", "spending_net_t_minus_1",
        "paid_leads_total", "paid_leads_t", "paid_leads_t_minus_1",
        "area_leads_total", "area_leads_t", "area_leads_t_minus_1",
        "local_leads_total", "local_leads_t", "local_leads_t_minus_1",
        "live_effective_hours_total", "live_effective_hours_t", "live_effective_hours_t_minus_1",
        "effective_live_sessions_total", "effective_live_sessions_t", "effective_live_sessions_t_minus_1",
        "exposures_total", "exposures_t", "exposures_t_minus_1",
        "viewers_total", "viewers_t", "viewers_t_minus_1",
        "small_wheel_clicks_total", "small_wheel_clicks_t", "small_wheel_clicks_t_minus_1",
        "small_wheel_leads_total", "small_wheel_leads_t", "small_wheel_leads_t_minus_1",
        "live_leads_total", "live_leads_t", "live_leads_t_minus_1",
        "anchor_exposure_total", "anchor_exposure_t", "anchor_exposure_t_minus_1",
        "component_clicks_total", "component_clicks_t", "component_clicks_t_minus_1",
        "short_video_leads_total", "short_video_leads_t", "short_video_leads_t_minus_1",
        "short_video_count_total", "short_video_count_t", "short_video_count_t_minus_1",
        "short_video_plays_total", "short_video_plays_t", "short_video_plays_t_minus_1",
        "enter_private_count_total", "enter_private_count_t", "enter_private_count_t_minus_1",
        "private_open_count_total", "private_open_count_t", "private_open_count_t_minus_1",
        "private_leads_count_total", "private_leads_count_t", "private_leads_count_t_minus_1",
        "total_cpl", "paid_cpl", "local_leads_ratio",
        "avg_daily_spending", "avg_daily_spending_t", "avg_daily_spending_t_minus_1",
        "avg_daily_paid_leads", "avg_daily_paid_leads_t", "avg_daily_paid_leads_t_minus_1",
        "paid_cpl_t", "paid_cpl_t_minus_1",
        "avg_daily_effective_live_hours_25min", "avg_daily_effective_live_hours_25min_t", "avg_daily_effective_live_hours_25min_t_minus_1",
        "avg_exposures_per_session", "avg_exposures_per_session_t", "avg_exposures_per_session_t_minus_1",
        "exposure_to_viewer_rate", "exposure_to_viewer_rate_t", "exposure_to_viewer_rate_t_minus_1",
        "avg_viewers_per_session", "avg_viewers_per_session_t", "avg_viewers_per_session_t_minus_1",
        "small_wheel_click_rate", "small_wheel_click_rate_t", "small_wheel_click_rate_t_minus_1",
        "small_wheel_leads_rate", "small_wheel_leads_rate_t", "small_wheel_leads_rate_t_minus_1",
        "avg_small_wheel_leads_per_session", "avg_small_wheel_leads_per_session_t", "avg_small_wheel_leads_per_session_t_minus_1",
        "component_click_rate", "component_click_rate_t", "component_click_rate_t_minus_1",
        "component_leads_rate", "component_leads_rate_t", "component_leads_rate_t_minus_1",
        "avg_daily_private_entry_count", "avg_daily_private_entry_count_t", "avg_daily_private_entry_count_t_minus_1",
        "avg_daily_private_open_count", "avg_daily_private_open_count_t", "avg_daily_private_open_count_t_minus_1",
        "avg_daily_private_leads_count", "avg_daily_private_leads_count_t", "avg_daily_private_leads_count_t_minus_1",
        "private_open_rate", "private_open_rate_t", "private_open_rate_t_minus_1",
        "private_leads_rate", "private_leads_rate_t", "private_leads_rate_t_minus_1",
        "private_conversion_rate", "private_conversion_rate_t", "private_conversion_rate_t_minus_1"
    ]
    final_columns_exist = [c for c in final_columns if c in summary_df.columns]
    final_df = summary_df.select(final_columns_exist)
    for col_name in final_df.columns:
        if final_df[col_name].dtype in [pl.Float32, pl.Float64]:
            final_df = final_df.with_columns(
                pl.when(pl.col(col_name).is_infinite())
                .then(None)
                .otherwise(pl.col(col_name))
                .fill_nan(None)
                .alias(col_name)
            )
    # 输出中英文对照表
    print("字段中英文对照表如下：")
    print(json.dumps(FIELD_EN_MAP, ensure_ascii=False, indent=2))
    return final_df
