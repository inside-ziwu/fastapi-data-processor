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
    "主机厂经销商ID": "NSC_CODE", 
    "日期": "date",
    "进入私信客户数": "enter_private_count",
    "主动咨询客户数": "private_open_count",
    "私信留资客户数": "private_leads_count"
}
ACCOUNT_BI_MAP = {
    "主机厂经销商id列表": "NSC_CODE", 
    "日期": "date", 
    "直播间表单提交商机量": "live_leads", 
    "短-播放量": "short_video_plays"
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
    "主机厂经销商ID": "NSC_CODE",
    "层级": "level", 
    "门店名": "store_name",
    "自然线索总量": "natural_leads_total",
    "T月自然线索量": "natural_leads_t",
    "T-1月自然线索量": "natural_leads_t_minus_1",
    "广告线索总量": "ad_leads_total",
    "T月广告线索量": "ad_leads_t",
    "T-1月广告线索量": "ad_leads_t_minus_1",
    "总消耗": "spending_net_total",
    "T月消耗": "spending_net_t",
    "T-1月消耗": "spending_net_t_minus_1",
    "付费线索总量": "paid_leads_total",
    "T月付费线索量": "paid_leads_t",
    "T-1月付费线索量": "paid_leads_t_minus_1",
    "区域线索总量": "area_leads_total",
    "T月区域线索量": "area_leads_t",
    "T-1月区域线索量": "area_leads_t_minus_1",
    "本地线索总量": "local_leads_total",
    "T月本地线索量": "local_leads_t",
    "T-1月本地线索量": "local_leads_t_minus_1",
    "有效直播时长总量(小时)": "live_effective_hours_total",
    "T月有效直播时长(小时)": "live_effective_hours_t",
    "T-1月有效直播时长(小时)": "live_effective_hours_t_minus_1",
    "有效直播场次总量": "effective_live_sessions_total",
    "T月有效直播场次": "effective_live_sessions_t",
    "T-1月有效直播场次": "effective_live_sessions_t_minus_1",
    "总曝光人数": "exposures_total",
    "T月曝光人数": "exposures_t",
    "T-1月曝光人数": "exposures_t_minus_1",
    "总场观": "viewers_total",
    "T月场观": "viewers_t",
    "T-1月场观": "viewers_t_minus_1",
    "小风车点击总量": "small_wheel_clicks_total",
    "T月小风车点击": "small_wheel_clicks_t",
    "T-1月小风车点击": "small_wheel_clicks_t_minus_1",
    "小风车留资总量": "small_wheel_leads_total",
    "T月小风车留资": "small_wheel_leads_t",
    "T-1月小风车留资": "small_wheel_leads_t_minus_1",
    "直播线索总量": "live_leads_total",
    "T月直播线索": "live_leads_t",
    "T-1月直播线索": "live_leads_t_minus_1",
    "锚点曝光总量": "anchor_exposure_total",
    "T月锚点曝光": "anchor_exposure_t",
    "T-1月锚点曝光": "anchor_exposure_t_minus_1",
    "组件点击总量": "component_clicks_total",
    "T月组件点击": "component_clicks_t",
    "T-1月组件点击": "component_clicks_t_minus_1",
    "短视频留资总量": "short_video_leads_total",
    "T月短视频留资": "short_video_leads_t",
    "T-1月短视频留资": "short_video_leads_t_minus_1",
    "短视频发布总量": "short_video_count_total",
    "T月短视频发布": "short_video_count_t",
    "T-1月短视频发布": "short_video_count_t_minus_1",
    "短视频播放总量": "short_video_plays_total",
    "T月短视频播放": "short_video_plays_t",
    "T-1月短视频播放": "short_video_plays_t_minus_1",
    "进私总量": "enter_private_count_total",
    "T月进私": "enter_private_count_t",
    "T-1月进私": "enter_private_count_t_minus_1",
    "私信开口总量": "private_open_count_total",
    "T月私信开口": "private_open_count_t",
    "T-1月私信开口": "private_open_count_t_minus_1",
    "私信留资总量": "private_leads_count_total",
    "T月私信留资": "private_leads_count_t",
    "T-1月私信留资": "private_leads_count_t_minus_1",
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

def read_excel_polars(path: str, sheet_name=None):
    # 回退到pandas读取，因为fastexcel不可用
    if sheet_name is None:
        # 处理多sheet情况 - 返回字典保持sheet名称
        all_sheets = pd.read_excel(path, sheet_name=None, engine="openpyxl")
        result = {}
        for sheet_name, df in all_sheets.items():
            for col in df.columns:
                df[col] = df[col].astype(str)
            result[sheet_name] = pl.from_pandas(df)
        return result
    else:
        pdf = pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
        for col in pdf.columns:
            pdf[col] = pdf[col].astype(str)
        return pl.from_pandas(pdf)


def normalize_nsc_col(df: pl.DataFrame, colname: str = "NSC_CODE") -> pl.DataFrame:
    if colname not in df.columns:
        df = df.with_columns(pl.lit(None).alias(colname))
    
    df = df.with_columns(pl.col(colname).cast(pl.Utf8))
    
    # 修复：保留原始值，不要过度清洗
    df = df.with_columns(
        pl.when(pl.col(colname).str.contains(r"[；，\|\u3001/\\]"))
        .then(pl.col(colname).str.replace_all(r"[；，\|\u3001/\\]+", ","))
        .otherwise(pl.col(colname))
        .alias(colname)
    )
    
    # 单条NSC数据不拆分，保留原始值
    mask = pl.col(colname).str.contains(",")
    multi_df = df.filter(mask).with_columns(pl.col(colname).str.split(",").alias("_nsc_list")).explode("_nsc_list")
    single_df = df.filter(~mask).with_columns(pl.col(colname).alias("_nsc_list"))
    
    df = pl.concat([multi_df, single_df])
    df = df.with_columns(pl.col("_nsc_list").str.strip_chars().alias("NSC_CODE_CLEAN"))
    
    if colname in df.columns:
        df = df.drop([colname, "_nsc_list"])
    else:
        df = df.drop(["_nsc_list"])
    df = df.rename({"NSC_CODE_CLEAN": "NSC_CODE"})
    original_count = df.height
    df = df.filter(pl.col("NSC_CODE").is_not_null() & (pl.col("NSC_CODE") != ""))
    filtered_count = df.height
    logger.debug(f"[NSC_CODE过滤] 原始行数: {original_count}, 过滤后: {filtered_count}")
    if filtered_count == 0:
        logger.warning(f"[NSC_CODE过滤] 过滤后无数据，保留部分原始数据: {df.head()}")
    return df

def ensure_date_column(pl_df: pl.DataFrame, colname: str = "date") -> pl.DataFrame:
    if colname not in pl_df.columns:
        return pl_df.with_columns(pl.lit(None, dtype=pl.Date).alias(colname))
    
    # 如果已经是date/datetime类型，直接转换
    if pl_df[colname].dtype in [pl.Date, pl.Datetime]:
        return pl_df.with_columns(pl.col(colname).cast(pl.Date).alias(colname))
    
    # 如果是字符串类型，尝试解析
    logger.debug(f"[日期调试] 字符串值示例: {pl_df[colname].head(3)}")
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
    logger.debug(f"[日期调试] 转换后: {pl_df[colname].head(3)}")
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

    # 处理dict或DataFrame的情况
    if isinstance(all_sheets, dict):
        # 多sheet情况（dict格式）
        sheets_data = all_sheets.items()
    else:
        # 单sheet情况（DataFrame格式）
        sheets_data = [("sheet1", all_sheets)]

    for sheetname, df_sheet in sheets_data:
        # 处理DataFrame类型：可能是pandas或polars
        if hasattr(df_sheet, 'dtypes') and str(type(df_sheet)).find('pandas') != -1:
            # pandas DataFrame
            df_sheet = pl.from_pandas(df_sheet)
        elif not hasattr(df_sheet, 'schema'):
            # 已经是polars DataFrame或需要转换
            try:
                df_sheet = pl.DataFrame(df_sheet)
            except:
                pass  # 已经是polars DataFrame
        df_cols = list(df_sheet.columns) if hasattr(df_sheet, 'columns') else []
        
        # Use loose matching for column names
        level_df_sheet = rename_columns_loose(df_sheet, {"NSC_id": "NSC_CODE", "第二期层级": "level"})
        if "NSC_CODE" in level_df_sheet.columns and "level" in level_df_sheet.columns:
            logger.info(f"[ACC_BASE] Identified level sheet: {sheetname}")
            level_df = level_df_sheet.select(["NSC_CODE", "level"])
        
        store_name_df_sheet = rename_columns_loose(df_sheet, {"NSC Code": "NSC_CODE", "抖音id": "store_name"})
        if "NSC_CODE" in store_name_df_sheet.columns and "store_name" in store_name_df_sheet.columns:
            logger.info(f"[ACC_BASE] Identified store_name sheet: {sheetname}")
            store_name_df = store_name_df_sheet.select(["NSC_CODE", "store_name"])

    if level_df is None and store_name_df is None:
        logger.warning("[ACC_BASE] Could not find valid level or store_name sheets in account_base_file.")
        return None

    # Join the two dataframes
    if level_df is not None and store_name_df is not None:
        # Select only the necessary columns from the right df to avoid redundant _right columns
        store_name_to_join = store_name_df.select(["NSC_CODE", "store_name"])
        merged = level_df.join(store_name_to_join, on="NSC_CODE", how="outer")
    elif level_df is not None:
        merged = level_df.with_columns(pl.lit(None).alias("store_name"))
    else:
        merged = store_name_df.with_columns(pl.lit(None).alias("level"))
    
    if merged is not None and "NSC_CODE" in merged.columns:
         merged = merged.filter(pl.col("NSC_CODE").is_not_null() & (pl.col("NSC_CODE") != ""))

    logger.debug(f"[ACC_BASE PROBE] Final merged account_base data:\n{merged.head() if merged is not None else 'None'}")
    return merged

def process_all_files(local_paths: Dict[str, str], spending_sheet_names: Optional[str] = None, dimension: str = "NSC_CODE") -> pl.DataFrame:
    logger.info(f"Entering process_all_files with {len(local_paths)} files")
    dfs = {}
    # 1. video_excel_file
    if "video_excel_file" in local_paths:
        p = local_paths["video_excel_file"]
        df = None
        if '.csv' in p.lower() or '.txt' in p.lower():
            df = read_csv_polars(p)
        elif '.xlsx' in p.lower() or '.xls' in p.lower():
            df = read_excel_polars(p, sheet_name=0)
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
            df = read_excel_polars(p, sheet_name=0)
        if df is not None:
            logger.debug(f"[live] columns={df.columns}")
            dfs["live"] = process_single_table(df, LIVE_MAP, ["over25_min_live_mins","live_effective_hours","effective_live_sessions","exposures","viewers","small_wheel_clicks"])
    # 3. msg_excel_file
    if "msg_excel_file" in local_paths:
        p = local_paths["msg_excel_file"]
        df_sheets = read_excel_polars(p, sheet_name=None)
        per_sheet_frames = []
        sheet_report = []
        
        # 处理dict或DataFrame的情况
        if isinstance(df_sheets, dict):
            sheets_data = df_sheets.items()
        else:
            # 单sheet情况
            sheets_data = [("sheet1", df_sheets)]
            
        for sheetname, df in sheets_data:
            orig_cols = list(map(str, df.columns))
            logger.debug(f"[MSG调试] sheet={sheetname}, columns={orig_cols}")
            PK_COLUMN = "主机厂经销商ID"
            if PK_COLUMN not in df.columns:
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
            # MSG文件处理：每个sheet的title就是日期，强制添加date列
            date_str = str(sheetname)
            try:
                from datetime import datetime
                
                # 处理不同日期格式
                if "-" in date_str:
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
                elif len(date_str) == 8 and date_str.isdigit():
                    date_obj = datetime.strptime(date_str, "%Y%m%d").date()
                elif len(date_str) == 10 and date_str.count("/") == 2:
                    date_obj = datetime.strptime(date_str, "%Y/%m/%d").date()
                else:
                    date_obj = datetime.strptime(date_str, "%Y%m%d").date()  # 默认尝试
                    
                # 强制添加date列
                df = df.with_columns(pl.lit(date_obj).alias("date"))
                logger.debug(f"[MSG日期] 使用sheet标题作为日期: {date_obj}")
            except ValueError as e:
                logger.error(f"[MSG日期] sheet标题 '{date_str}' 不是有效日期格式: {e}")
                # 作为字符串让ensure_date_column处理
                df = df.with_columns(pl.lit(date_str).alias("date"))
            
            # 然后进行列映射
            df = rename_columns_loose(df, MSG_MAP)
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
        logger.info(f"[MSG合并完成] 总行数: {df.height}, 总列数: {len(df.columns)}")
        logger.info(f"[MSG合并后] 列名: {df.columns}")
        if df.height > 0:
            logger.info(f"[MSG数据预览]\n{df.head(5)}")
            # 强制转换字符串为数值类型
            str_to_num_cols = ["enter_private_count","private_open_count","private_leads_count"]
            for col in str_to_num_cols:
                if col in df.columns:
                    # 先转换为字符串，再转换为数值，处理空值
                    df = df.with_columns(
                        pl.col(col)
                        .cast(pl.Utf8)  # 确保是字符串
                        .str.strip_chars()  # 去除空格
                        .str.replace_all("[^0-9.]", "")  # 只保留数字和小数点
                        .cast(pl.Float64, strict=False)  # 转换为浮点数
                        .fill_null(0)  # 空值填0
                        .fill_nan(0)   # NaN填0
                        .alias(col)
                    )
            
            # 显示数值统计
            numeric_cols = ["enter_private_count","private_open_count","private_leads_count"]
            for col in numeric_cols:
                if col in df.columns:
                    total = df[col].sum()
                    non_null = df[col].is_not_null().sum()
                    logger.info(f"[MSG统计] {col}: 总和={total}, 非空值={non_null}, 空值={(df.height - non_null)}, 类型={df[col].dtype}")
        group_cols = ["NSC_CODE", "date"]
        sum_cols = ["enter_private_count","private_open_count","private_leads_count"]
        for col in sum_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False).fill_null(0).fill_nan(0))
        agg_exprs = [pl.col(c).sum().alias(c) for c in sum_cols if c in df.columns]
        if agg_exprs:
            df = df.group_by(group_cols).agg(agg_exprs)
        else:
            df = df.unique(subset=group_cols)
        logger.info(f"[MSG聚合完成] 聚合后行数: {df.height}, 聚合后列数: {len(df.columns)}")
        logger.info(f"[MSG聚合后] 列名: {df.columns}")
        if df.height > 0:
            logger.info(f"[MSG聚合数据预览]\n{df.head(5)}")
            # 显示聚合后统计 - 确保数值类型
            for col in sum_cols:
                if col in df.columns:
                    # 再次确保数值类型
                    df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False).fill_null(0).fill_nan(0).alias(col))
                    total = df[col].sum()
                    logger.info(f"[MSG聚合统计] {col}: 总和={total}")
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
            df = read_excel_polars(p, sheet_name=0)
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
            df = read_excel_polars(p, sheet_name=0)
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
                processed_dfs = []
                for s in sheet_names:
                    try:
                        df_sheet = read_excel_polars(p, sheet_name=s)
                        processed = process_single_table(df_sheet, SPENDING_MAP, ["spending_net"])
                        processed_dfs.append(processed)
                    except Exception as e:
                        logger.warning(f"Spending_file sheet {s} processing failed: {e}")
                        continue
                if processed_dfs:
                    df = pl.concat(processed_dfs, how="vertical")
                else:
                    df_sheet = read_excel_polars(p, sheet_name=0)
                    df = process_single_table(df_sheet, SPENDING_MAP, ["spending_net"])
            else:
                all_sheets = read_excel_polars(p, sheet_name=None)
                processed_dfs = []
                for sheet_name, df_sheet in all_sheets.items():
                    try:
                        processed = process_single_table(df_sheet, SPENDING_MAP, ["spending_net"])
                        processed_dfs.append(processed)
                    except Exception as e:
                        logger.warning(f"Spending_file sheet {sheet_name} processing failed: {e}")
                        continue
                if processed_dfs:
                    df = pl.concat(processed_dfs, how="vertical")
                else:
                    df_sheet = read_excel_polars(p, sheet_name=0)
                    df = process_single_table(df_sheet, SPENDING_MAP, ["spending_net"])
        if df is not None:
            logger.debug(f"[spending] columns={df.columns}")
            dfs["spending"] = process_single_table(df, SPENDING_MAP, ["spending_net"])
    # 8. account_base_file
    account_base = None
    if "account_base_file" in local_paths:
        p = local_paths["account_base_file"]
        logger.info(f"Processing account_base_file: {p}")
        df_base = read_excel_polars(p, sheet_name=None)
        account_base = process_account_base(df_base)
        if account_base is not None:
            logger.info(f"Account base processed successfully: {account_base.shape[0]} records")
        else:
            logger.warning("Account base processing returned None")
    if not dfs:
        raise ValueError("No dataframes were processed. Check file inputs and names.")
    logger.info("Building base keys using pure Python to bypass Polars concat bug.")
    
    # 1. Use a Python set for automatic uniqueness
    unique_keys_set = set()

    # 2. Iterate through all processed dataframes
    for k, v in dfs.items():
        try:
            logger.debug(f"Extracting keys from '{k}', columns: {v.columns}")
            if "NSC_CODE" in v.columns and "date" in v.columns:
                logger.debug(f"Processing {k} with {v.height} rows")
                # 严格过滤：只保留有值的NSC_CODE和有效date
                try:
                    filtered_df = v.select(["NSC_CODE", "date"]).filter(
                        pl.col("NSC_CODE").is_not_null() & 
                        (pl.col("NSC_CODE") != "") & 
                        (pl.col("NSC_CODE") != "--") &
                        (pl.col("NSC_CODE").str.strip_chars() != "") &
                        pl.col("date").is_not_null()
                    )
                    keys_from_df = filtered_df.to_dicts()
                    logger.debug(f"Extracted {len(keys_from_df)} valid keys from {k}")
                    for row in keys_from_df:
                        unique_keys_set.add((row['NSC_CODE'], row['date']))
                except Exception as e:
                    logger.error(f"Error filtering {k}: {str(e)}")
                    # 如果没有过滤成功，至少添加原始数据
                    keys_from_df = v.select(["NSC_CODE", "date"]).drop_nulls().to_dicts()
                    for row in keys_from_df:
                        unique_keys_set.add((row['NSC_CODE'], row['date']))
            else:
                logger.warning(f"Skipping {k}: missing NSC_CODE or date columns")
        except Exception as e:
            logger.error(f"Error processing dataframe {k}: {str(e)}")
            logger.error(f"DataFrame info: columns={list(v.columns) if hasattr(v, 'columns') else 'unknown'}, type={type(v)}")

    if not unique_keys_set:
        raise ValueError("No valid (NSC_CODE, date) key pairs found in any file.")

    logger.info(f"Found {len(unique_keys_set)} unique keys.")
    
    # 数据验证日志
    logger.info("开始验证各数据源：")
    for name, df in dfs.items():
        logger.info(f"[数据源] {name}: 行数={df.height}, 列={len(df.columns)}")
        # 检查数值列总和
        numeric_cols = [col for col in df.columns if col in [
            'natural_leads', 'ad_leads', 'paid_leads', 'area_leads', 'local_leads', 
            'spending_net', 'live_leads', 'small_wheel_leads', 'private_leads_count',
            'enter_private_count', 'private_open_count', 'short_video_leads'
        ]]
        for col in numeric_cols:
            if col in df.columns:
                total = df[col].sum()
                null_count = df[col].is_null().sum()
                logger.info(f"[数据验证] {name}.{col}: 总和={total}, 空值={null_count}")

    # 根据维度优化聚合策略
    if dimension == "level":
        # level维度：直接按level聚合，避免NSC_CODE膨胀
        logger.info("level维度：直接按层级聚合")
        
        # level维度时，先按level分组，避免NSC笛卡尔积
        level_base = base.select(["level", "date"] + agg_cols_exist).filter(
            pl.col("level").is_not_null() & (pl.col("level") != "") & (pl.col("level") != "unknown")
        ).group_by("level", "date").agg([
            pl.col(c).sum() for c in agg_cols_exist if c in base.columns
        ])
        base = level_base
        
    else:
        # NSC_CODE维度：保持原有逻辑
        logger.info("NSC_CODE维度：保持原有逻辑")
        # 4. Create the base DataFrame from the clean Python set and sort it
        base = pl.DataFrame(
            list(unique_keys_set),
            schema={'NSC_CODE': pl.Utf8, 'date': pl.Date},
            orient="row"
        ).sort(["NSC_CODE", "date"])

        for name, df in dfs.items():
            if "date" in df.columns:
                base = base.join(df, on=["NSC_CODE", "date"], how="left")
            else:
                base = base.join(df, on="NSC_CODE", how="left")
        if account_base is not None:
            base = base.join(account_base, on="NSC_CODE", how="left")
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
    # 根据dimension参数确定聚合维度
    valid_dimensions = ["NSC_CODE", "层级"]
    if dimension not in valid_dimensions:
        logger.warning(f"未知维度: {dimension}，使用默认NSC_CODE")
        dimension = "NSC_CODE"
    
    logger.info(f"使用聚合维度: {dimension}")
    logger.info(f"实际group_by_cols: {group_by_cols}")
    logger.info(f"base列: {base.columns[:5]}")
    
    # 动态构建聚合维度列表 - 使用实际字段名
    field_mapping = {
        "NSC_CODE": "NSC_CODE",
        "level": "层级"  # 关键：level维度使用中文字段名
    }
    
    if dimension == "NSC_CODE":
        # NSC_CODE维度时，保留level和store_name作为额外信息
        group_by_cols = ["NSC_CODE", "level", "store_name"]
    elif dimension == "level":
        # level维度时，仅按层级聚合，不包含NSC_CODE和store_name
        group_by_cols = ["层级"]
    
    # 过滤掉不存在的列
    group_by_cols = [c for c in group_by_cols if c in base.columns]
    
    summary_df = base.pivot(
        index=group_by_cols,
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
        # 处理Python整数和Polars表达式的混合情况
        num_expr = pl.lit(num_expr) if not hasattr(num_expr, 'is_not_null') else num_expr
        den_expr = pl.lit(den_expr) if not hasattr(den_expr, 'is_not_null') else den_expr
        
        return pl.when((den_expr != 0) & den_expr.is_not_null() & num_expr.is_not_null()) \
                .then(num_expr / den_expr) \
                .otherwise(None)
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
    # 根据维度动态选择最终字段
    final_columns = []
    if dimension == "NSC_CODE":
        final_columns.extend(["NSC_CODE", "level", "store_name"])
    elif dimension == "level":
        final_columns.append("层级")
    
    final_columns.extend([
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
    ])
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
    # 创建反向映射（英文->中文）
    EN_TO_CN_MAP = {v: k for k, v in FIELD_EN_MAP.items()}
    
    # 创建变量类型映射
    TYPE_MAPPING = {}
    for col in final_df.columns:
        if col in EN_TO_CN_MAP:
            cn_name = EN_TO_CN_MAP[col]
            dtype = str(final_df[col].dtype)
            if "float" in dtype:
                type_desc = "数值型"
            elif "int" in dtype:
                type_desc = "整数型"
            elif "str" in dtype or "utf8" in dtype:
                type_desc = "文本型"
            elif "date" in dtype:
                type_desc = "日期型"
            else:
                type_desc = "其他"
            TYPE_MAPPING[cn_name] = type_desc
    
    # 输出中英文对照表和类型说明
    print("字段中英文对照表如下：")
    print(json.dumps(FIELD_EN_MAP, ensure_ascii=False, indent=2))
    
    return final_df, EN_TO_CN_MAP, TYPE_MAPPING
