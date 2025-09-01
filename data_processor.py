import os
import shutil
import logging
from typing import Optional, Dict
import polars as pl
import pandas as pd
import re

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

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
    # Normalize the entire strings first to handle spaces and symbol variations globally.
    src_norm = normalize_symbol(src)
    col_norm = normalize_symbol(col)

    # After normalization, split them into parts.
    src_parts = split_chinese_english(src_norm)
    col_parts = split_chinese_english(col_norm)

    logger.debug(f"field_match: src='{src}', col='{col}', src_parts={src_parts}, col_parts={col_parts}")

    if len(src_parts) != len(col_parts):
        return False

    for s, c in zip(src_parts, col_parts):
        # For non-Chinese parts, the comparison should be case-insensitive.
        # For Chinese parts, it remains a direct comparison.
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

    # List of formats to try, from most to least common in this project
    formats_to_try = [
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d",
    ]

    # Coalesce allows us to try parsing with one format, and if it fails (produces null),
    # try the next format in the list.
    parser_expressions = [
        pl.col(colname).str.strptime(pl.Datetime, format=fmt, strict=False)
        for fmt in formats_to_try
    ]

    # We also include the original string as a fallback in case all parsing fails
    # and to preserve original values that are not dates.
    pl_df = pl_df.with_columns(
        pl.coalesce(parser_expressions)
        .cast(pl.Date)
        .alias(colname)
    )
    return pl_df

def try_cast_numeric(pl_df: pl.DataFrame, cols):
    for c in cols:
        if c in pl_df.columns:
            pl_df = pl_df.with_columns(pl.col(c).cast(pl.Float64).fill_null(0))
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
    df = rename_columns_loose(df, DR_MAP)
    df = normalize_nsc_col(df, "NSC_CODE")
    df = ensure_date_column(df, "date")

    # A single group_by with conditional aggregations is far more efficient
    # and robust than creating 5 separate dataframes and joining them.
    dr_all = df.group_by("NSC_CODE", "date").agg(
        pl.col("leads_type").filter(pl.col("leads_type") == "自然").count().alias("自然线索"),
        pl.col("leads_type").filter(pl.col("leads_type") == "广告").count().alias("广告线索"),
        pl.col("leads_type").filter(
            (pl.col("leads_type") == "广告") &
            (pl.col("mkt_second_channel_name").is_in(["抖音车云店_BMW_本市_LS直发", "抖音车云店_LS直发"]))
        ).count().alias("车云店付费线索"),
        pl.col("leads_type").filter(
            (pl.col("leads_type") == "广告") &
            (pl.col("mkt_second_channel_name") == "抖音车云店_BMW_总部BDT_LS直发")
        ).count().alias("区域付费线索"),
        pl.col("send2dealer_id").filter(
            pl.col("send2dealer_id").cast(pl.Utf8) == pl.col("NSC_CODE")
        ).count().alias("本地线索量")
    )
    return dr_all

def process_account_base(all_sheets):
    level_df, store_name_df = None, None
    for sheetname, pdf in all_sheets.items():
        df = df_pandas_to_polars(pdf)
        df = rename_columns_loose(df, ACCOUNT_BASE_MAP)
        if "level" in df.columns and "NSC_CODE" in df.columns:
            level_df = df.select(["NSC_CODE", "level"])
        if "store_name" in df.columns and "NSC_CODE" in df.columns:
            store_name_df = df.select(["NSC_CODE", "store_name"])
    if level_df is not None or store_name_df is not None:
        if level_df is None:
            merged = store_name_df.with_columns(pl.lit(None).alias("level"))
        elif store_name_df is None:
            merged = level_df.with_columns(pl.lit(None).alias("store_name"))
        else:
            merged = level_df.join(store_name_df, on="NSC_CODE", how="outer")
        return merged
    return None

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

        # Cast columns to be summed to numeric to avoid type errors
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
            logger.debug(f"[account_bi] columns={df.columns}")
            dfs["account_bi"] = process_single_table(df, ACCOUNT_BI_MAP, ["live_leads","short_video_plays"])

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
    # CRITICAL FIX: Filter out rows with null dates and fill nulls in pivot index keys.
    logger.info("开始最终分析前的数据清洗...")
    base = base.filter(pl.col("date").is_not_null())
    
    id_cols_to_fill = ["level", "store_name"]
    for col in id_cols_to_fill:
        if col in base.columns:
            base = base.with_columns(pl.col(col).fill_null("未知"))
    logger.info("数据清洗完成")
    logger.info("开始进行T/T-1分析...")
    
    # Step 1: Data Preparation
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

    # Step 2: Pivot Data
    agg_cols = [
        'anchor_exposure', 'component_clicks', 'short_video_count', 'short_video_leads',
        'over25_min_live_mins', 'live_effective_hours', 'effective_live_sessions', 'exposures',
        'viewers', 'small_wheel_clicks', 'enter_private_count', 'private_open_count',
        'private_leads_count', 'live_leads', 'short_video_plays', 'small_wheel_leads',
        '自然线索', '广告线索', '车云店付费线索', '区域付费线索', '本地线索量', 'spending_net'
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
            (pl.col(f"{col}_T").fill_null(0) + pl.col(f"{col}_T-1").fill_null(0)).alias(f"{col}_Total")
        )

    # Step 4: Calculate Intermediate Derived Metrics
    summary_df = summary_df.with_columns(
        (pl.col("车云店付费线索_Total").fill_null(0) + pl.col("区域付费线索_Total").fill_null(0)).alias("直播车云店+区域付费线索量"),
        (pl.col("车云店付费线索_T").fill_null(0) + pl.col("区域付费线索_T").fill_null(0)).alias("T月直播车云店+区域付费线索量"),
        (pl.col("车云店付费线索_T-1").fill_null(0) + pl.col("区域付费线索_T-1").fill_null(0)).alias("T-1月直播车云店+区域付费线索量"),
        (pl.col("over25_min_live_mins_Total") / 60).alias("有效（25min以上）时长（h）"),
        (pl.col("over25_min_live_mins_T") / 60).alias("T月有效（25min以上）时长（h）"),
        (pl.col("over25_min_live_mins_T-1") / 60).alias("T-1月有效（25min以上）时长（h）")
    )

    # Step 5: Calculate Final Ratios
    def safe_div(num_expr, den_expr):
        return pl.when(den_expr != 0).then(num_expr / den_expr).otherwise(None)

    summary_df = summary_df.with_columns(
        safe_div(pl.col("spending_net_Total"), (pl.col("自然线索_Total").fill_null(0) + pl.col("广告线索_Total").fill_null(0))).alias("车云店+区域综合CPL"),
        safe_div(pl.col("spending_net_Total"), pl.col("直播车云店+区域付费线索量")).alias("付费CPL（车云店+区域）"),
        safe_div(pl.col("本地线索量_Total"), (pl.col("自然线索_Total").fill_null(0) + pl.col("广告线索_Total").fill_null(0))).alias("本地线索占比"),
        safe_div(pl.col("spending_net_Total"), (t_days + t_minus_1_days)).alias("直播车云店+区域日均消耗"),
        safe_div(pl.col("spending_net_T"), t_days).alias("T月直播车云店+区域日均消耗"),
        safe_div(pl.col("spending_net_T-1"), t_minus_1_days).alias("T-1月直播车云店+区域日均消耗"),
        safe_div(pl.col("直播车云店+区域付费线索量"), (t_days + t_minus_1_days)).alias("直播车云店+区域付费线索量日均"),
        safe_div(pl.col("T月直播车云店+区域付费线索量"), t_days).alias("T月直播车云店+区域付费线索量日均"),
        safe_div(pl.col("T-1月直播车云店+区域付费线索量"), t_minus_1_days).alias("T-1月直播车云店+区域付费线索量日均"),
        safe_div(pl.col("spending_net_T"), pl.col("T月直播车云店+区域付费线索量")).alias("T月直播付费CPL"),
        safe_div(pl.col("spending_net_T-1"), pl.col("T-1月直播车云店+区域付费线索量")).alias("T-1月直播付费CPL"),
        safe_div(pl.col("有效（25min以上）时长（h）"), (t_days + t_minus_1_days)).alias("日均有效（25min以上）时长（h）"),
        safe_div(pl.col("T月有效（25min以上）时长（h）"), t_days).alias("T月日均有效（25min以上）时长（h）"),
        safe_div(pl.col("T-1月有效（25min以上）时长（h）"), t_minus_1_days).alias("T-1月日均有效（25min以上）时长（h）"),
        safe_div(pl.col("exposures_Total"), pl.col("effective_live_sessions_Total")).alias("场均曝光人数"),
        safe_div(pl.col("exposures_T"), pl.col("effective_live_sessions_T")).alias("T月场均曝光人数"),
        safe_div(pl.col("exposures_T-1"), pl.col("effective_live_sessions_T-1")).alias("T-1月场均曝光人数"),
        safe_div(pl.col("viewers_Total"), pl.col("exposures_Total")).alias("曝光进入率"),
        safe_div(pl.col("viewers_T"), pl.col("exposures_T")).alias("T月曝光进入率"),
        safe_div(pl.col("viewers_T-1"), pl.col("exposures_T-1")).alias("T-1月曝光进入率"),
        safe_div(pl.col("viewers_Total"), pl.col("effective_live_sessions_Total")).alias("场均场观"),
        safe_div(pl.col("viewers_T"), pl.col("effective_live_sessions_T")).alias("T月场均场观"),
        safe_div(pl.col("viewers_T-1"), pl.col("effective_live_sessions_T-1")).alias("T-1月场均场观"),
        safe_div(pl.col("small_wheel_clicks_Total"), pl.col("viewers_Total")).alias("小风车点击率"),
        safe_div(pl.col("small_wheel_clicks_T"), pl.col("viewers_T")).alias("T月小风车点击率"),
        safe_div(pl.col("small_wheel_clicks_T-1"), pl.col("viewers_T-1")).alias("T-1月小风车点击率"),
        safe_div(pl.col("small_wheel_leads_Total"), pl.col("small_wheel_clicks_Total")).alias("小风车点击留资率"),
        safe_div(pl.col("small_wheel_leads_T"), pl.col("small_wheel_clicks_T")).alias("T月小风车点击留资率"),
        safe_div(pl.col("small_wheel_leads_T-1"), pl.col("small_wheel_clicks_T-1")).alias("T-1月小风车点击留资率"),
        safe_div(pl.col("small_wheel_leads_Total"), pl.col("effective_live_sessions_Total")).alias("场均小风车留资量"),
        safe_div(pl.col("small_wheel_leads_T"), pl.col("effective_live_sessions_T")).alias("T月场均小风车留资量"),
        safe_div(pl.col("small_wheel_leads_T-1"), pl.col("effective_live_sessions_T-1")).alias("T-1月场均小风车留资量"),
        safe_div(pl.col("component_clicks_Total"), pl.col("anchor_exposure_Total")).alias("组件点击率"),
        safe_div(pl.col("component_clicks_T"), pl.col("anchor_exposure_T")).alias("T月组件点击率"),
        safe_div(pl.col("component_clicks_T-1"), pl.col("anchor_exposure_T-1")).alias("T-1月组件点击率"),
        safe_div(pl.col("short_video_leads_Total"), pl.col("anchor_exposure_Total")).alias("组件留资率"),
        safe_div(pl.col("short_video_leads_T"), pl.col("anchor_exposure_T")).alias("T月组件留资率"),
        safe_div(pl.col("short_video_leads_T-1"), pl.col("anchor_exposure_T-1")).alias("T-1月组件留资率"),
        safe_div(pl.col("enter_private_count_Total"), (t_days + t_minus_1_days)).alias("日均进私人数"),
        safe_div(pl.col("enter_private_count_T"), t_days).alias("T月日均进私人数"),
        safe_div(pl.col("enter_private_count_T-1"), t_minus_1_days).alias("T-1月日均进私人数"),
        safe_div(pl.col("private_open_count_Total"), (t_days + t_minus_1_days)).alias("日均私信开口人数"),
        safe_div(pl.col("private_open_count_T"), t_days).alias("T月日均私信开口人数"),
        safe_div(pl.col("private_open_count_T-1"), t_minus_1_days).alias("T-1月日均私信开口人数"),
        safe_div(pl.col("private_leads_count_Total"), (t_days + t_minus_1_days)).alias("日均咨询留资人数"),
        safe_div(pl.col("private_leads_count_T"), t_days).alias("T月日均咨询留资人数"),
        safe_div(pl.col("private_leads_count_T-1"), t_minus_1_days).alias("T-1月日均咨询留资人数"),
        safe_div(pl.col("private_open_count_Total"), pl.col("enter_private_count_Total")).alias("私信咨询率"),
        safe_div(pl.col("private_open_count_T"), pl.col("enter_private_count_T")).alias("T月私信咨询率"),
        safe_div(pl.col("private_open_count_T-1"), pl.col("enter_private_count_T-1")).alias("T-1月私信咨询率"),
        safe_div(pl.col("private_leads_count_Total"), pl.col("private_open_count_Total")).alias("咨询留资率"),
        safe_div(pl.col("private_leads_count_T"), pl.col("private_open_count_T")).alias("T月咨询留资率"),
        safe_div(pl.col("private_leads_count_T-1"), pl.col("private_open_count_T-1")).alias("T-1月咨询留资率"),
        safe_div(pl.col("private_leads_count_Total"), pl.col("enter_private_count_Total")).alias("私信转化率"),
        safe_div(pl.col("private_leads_count_T"), pl.col("enter_private_count_T")).alias("T月私信转化率"),
        safe_div(pl.col("private_leads_count_T-1"), pl.col("enter_private_count_T-1")).alias("T-1月私信转化率"),
    )

    # Step 6: Final Selection
    final_columns = [
        "NSC_CODE", "level", "store_name",
        "dr_natural_Total", "dr_natural_T", "dr_natural_T-1",
        "dr_ad_Total", "dr_ad_T", "dr_ad_T-1",
        "spending_net_Total", "spending_net_T", "spending_net_T-1",
        "dr_cyd_Total", "dr_cyd_T", "dr_cyd_T-1",
        "dr_area_Total", "dr_area_T", "dr_area_T-1",
        "dr_local_Total", "dr_local_T", "dr_local_T-1",
        "live_effective_hours_Total", "live_effective_hours_T", "live_effective_hours_T-1",
        "effective_live_sessions_Total", "effective_live_sessions_T", "effective_live_sessions_T-1",
        "exposures_Total", "exposures_T", "exposures_T-1",
        "viewers_Total", "viewers_T", "viewers_T-1",
        "small_wheel_clicks_Total", "small_wheel_clicks_T", "small_wheel_clicks_T-1",
        "small_wheel_leads_Total", "small_wheel_leads_T", "small_wheel_leads_T-1",
        "live_leads_Total", "live_leads_T", "live_leads_T-1",
        "anchor_exposure_Total", "anchor_exposure_T", "anchor_exposure_T-1",
        "component_clicks_Total", "component_clicks_T", "component_clicks_T-1",
        "short_video_leads_Total", "short_video_leads_T", "short_video_leads_T-1",
        "short_video_count_Total", "short_video_count_T", "short_video_count_T-1",
        "short_video_plays_Total", "short_video_plays_T", "short_video_plays_T-1",
        "enter_private_count_Total", "enter_private_count_T", "enter_private_count_T-1",
        "private_open_count_Total", "private_open_count_T", "private_open_count_T-1",
        "private_leads_count_Total", "private_leads_count_T", "private_leads_count_T-1",
        "车云店+区域综合CPL", "付费CPL（车云店+区域）", "本地线索占比",
        "直播车云店+区域日均消耗", "T月直播车云店+区域日均消耗", "T-1月直播车云店+区域日均消耗",
        "直播车云店+区域付费线索量日均", "T月直播车云店+区域付费线索量日均", "T-1月直播车云店+区域付费线索量日均",
        "T月直播付费CPL", "T-1月直播付费CPL",
        "日均有效（25min以上）时长（h）", "T月日均有效（25min以上）时长（h）", "T-1月日均有效（25min以上）时长（h）",
        "场均曝光人数", "T月场均曝光人数", "T-1月场均曝光人数",
        "曝光进入率", "T月曝光进入率", "T-1月曝光进入率",
        "场均场观", "T月场均场观", "T-1月场均场观",
        "小风车点击率", "T月小风车点击率", "T-1月小风车点击率",
        "小风车点击留资率", "T月小风车点击留资率", "T-1月小风车点击留资率",
        "场均小风车留资量", "T月场均小风车留资量", "T-1月场均小风车留资量",
        "组件点击率", "T月组件点击率", "T-1月组件点击率",
        "组件留资率", "T月组件留资率", "T-1月组件留资率",
        "日均进私人数", "T月日均进私人数", "T-1月日均进私人数",
        "日均私信开口人数", "T月日均私信开口人数", "T-1月日均私信开口人数",
        "日均咨询留资人数", "T月日均咨询留资人数", "T-1月日均咨询留资人数",
        "私信咨询率", "T月私信咨询率", "T-1月私信咨询率",
        "咨询留资率", "T月咨询留资率", "T-1月咨询留资率",
        "私信转化率", "T月私信转化率", "T-1月私信转化率"
    ]
    
    final_columns_exist = [c for c in final_columns if c in summary_df.columns]
    final_df = summary_df.select(final_columns_exist)

    # Final cleaning step before returning
    for col_name in final_df.columns:
        if final_df[col_name].dtype in [pl.Float32, pl.Float64]:
            final_df = final_df.with_columns(
                pl.when(pl.col(col_name).is_infinite())
                .then(None)
                .otherwise(pl.col(col_name))
                .fill_nan(None)
                .alias(col_name)
            )
    return final_df
