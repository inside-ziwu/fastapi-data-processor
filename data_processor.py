import os
import shutil
import logging
from typing import Optional, Dict
import polars as pl
import pandas as pd
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
    "主机厂经销商ID": "NSC_CODE", "日期": "date", "进入私信客户数": "enter_private_count",
    "主动咨询客户数": "private_open_count", "私信留资客户数": "private_leads_count"
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
    src_parts = split_chinese_english(src)
    col_parts = split_chinese_english(col)
    print(f"field_match: src='{src}', col='{col}', src_parts={src_parts}, col_parts={col_parts}")
    if len(src_parts) != len(col_parts):
        return False
    for s, c in zip(src_parts, col_parts):
        if re.search(r'[\u4e00-\u9fff]', s):  # 中文片段
            if s != c:
                print(f"中文不匹配: '{s}' != '{c}'")
                return False
        else:  # 英文/符号片段
            if normalize_symbol(s).lower() != normalize_symbol(c).lower():
                print(f"英文/符号不匹配: '{normalize_symbol(s).lower()}' != '{normalize_symbol(c).lower()}'")
                return False
    return True

def rename_columns_loose(pl_df: pl.DataFrame, mapping: Dict[str, str]) -> pl.DataFrame:
    col_map = {}
    for src, dst in mapping.items():
        for c in pl_df.columns:
            match = field_match(src, c)
            print(f"字段映射尝试: 源字段='{src}'，目标字段='{c}'，match={match}")
            if match:
                col_map[c] = dst
                break
    if col_map:
        print("最终映射关系:", col_map)
        pl_df = pl_df.rename(col_map)
    else:
        print("没有任何字段被映射")
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
        pl.col("_nsc_list").str.strip_chars().alias("NSC_CODE")
    )
    df = df.drop(colname).drop("_nsc_list")
    df = df.filter(pl.col("NSC_CODE").is_not_null() & (pl.col("NSC_CODE") != ""))
    return df

def ensure_date_column(pl_df: pl.DataFrame, colname: str = "date") -> pl.DataFrame:
    if colname not in pl_df.columns:
        pl_df = pl_df.with_columns(pl.lit(None).alias(colname))
    pl_df = pl_df.with_columns(
        pl.col(colname).str.slice(0, 10).str.strptime(pl.Date, strict=False)
    )
    return pl_df

def try_cast_numeric(pl_df: pl.DataFrame, cols):
    for c in cols:
        if c in pl_df.columns:
            pl_df = pl_df.with_columns(pl.col(c).cast(pl.Float64).fill_null(0))
    return pl_df

def process_single_table(df, mapping, sum_cols=None):
    df = rename_columns_loose(df, mapping)
    df = normalize_nsc_col(df, "NSC_CODE")
    df = ensure_date_column(df, "date")
    if sum_cols:
        df = try_cast_numeric(df, sum_cols)
    group_cols = ["NSC_CODE", "date"]
    agg_exprs = [pl.col(c).sum().alias(c) for c in sum_cols] if sum_cols else []
    if agg_exprs:
        df = df.groupby(group_cols).agg(agg_exprs)
    else:
        df = df.unique(subset=group_cols)
    return df

def process_dr_table(df):
    df = rename_columns_loose(df, DR_MAP)
    df = normalize_nsc_col(df, "NSC_CODE")
    df = ensure_date_column(df, "date")
    dr_natural = df.filter(pl.col("leads_type") == "自然").groupby(["NSC_CODE", "date"]).count().rename({"count": "自然线索"})
    dr_ad = df.filter(pl.col("leads_type") == "广告").groupby(["NSC_CODE", "date"]).count().rename({"count": "广告线索"})
    dr_cyd = df.filter(
        (pl.col("leads_type") == "广告") &
        (pl.col("mkt_second_channel_name").is_in(["抖音车云店_BMW_本市_LS直发", "抖音车云店_LS直发"]))
    ).groupby(["NSC_CODE", "date"]).count().rename({"count": "车云店付费线索"})
    dr_area = df.filter(
        (pl.col("leads_type") == "广告") &
        (pl.col("mkt_second_channel_name") == "抖音车云店_BMW_总部BDT_LS直发")
    ).groupby(["NSC_CODE", "date"]).count().rename({"count": "区域付费线索"})
    dr_local = df.filter(pl.col("send2dealer_id") == pl.col("reg_dealer")).groupby(["NSC_CODE", "date"]).count().rename({"count": "本地线索量"})
    dr_all = dr_natural.join(dr_ad, on=["NSC_CODE", "date"], how="outer") \
                       .join(dr_cyd, on=["NSC_CODE", "date"], how="outer") \
                       .join(dr_area, on=["NSC_CODE", "date"], how="outer") \
                       .join(dr_local, on=["NSC_CODE", "date"], how="outer")
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
        if p.lower().endswith(('.csv', '.txt')):
            df = read_csv_polars(p)
        elif p.lower().endswith(('.xlsx', '.xls')):
            pdf = read_excel_as_pandas(p, sheet_name=0)
            df = df_pandas_to_polars(pdf)
        if df is not None:
            dfs["video"] = process_single_table(df, VIDEO_MAP, ["anchor_exposure","component_clicks","short_video_count","short_video_leads"])

    # 2. live_bi_file
    if "live_bi_file" in local_paths:
        p = local_paths["live_bi_file"]
        df = None
        if p.lower().endswith(('.csv', '.txt')):
            df = read_csv_polars(p)
        elif p.lower().endswith(('.xlsx', '.xls')):
            pdf = read_excel_as_pandas(p, sheet_name=0)
            df = df_pandas_to_polars(pdf)
        if df is not None:
            dfs["live"] = process_single_table(df, LIVE_MAP, ["over25_min_live_mins","live_effective_hours","effective_live_sessions","exposures","viewers","small_wheel_clicks"])

    # 3. msg_excel_file
    if "msg_excel_file" in local_paths:
        p = local_paths["msg_excel_file"]
        all_sheets = read_excel_as_pandas(p, sheet_name=None)

        # 仅允许的主键列名（中文完全一致；英文/符号不涉及这里）
        PRIMARY_KEYS = ["主机厂经销商ID"]  # 如需允许“经销商ID”，明确加进去：["主机厂经销商ID", "经销商ID"]

        per_sheet_frames = []
        sheet_report = []  # 调试报告

        for sheetname, pdf in all_sheets.items():
            # 记录原始列名
            orig_cols = list(map(str, pdf.columns))

            # 精准寻找主键列（中文完全一致）
            pk_matched = None
            for pk in PRIMARY_KEYS:
                if pk in pdf.columns:
                    pk_matched = pk
                    break

            sheet_status = {
                "sheet": sheetname,
                "orig_cols": orig_cols,
                "pk_found": pk_matched or "",
            }

            if pk_matched is None:
                # 严格模式：遇到缺主键的 sheet，直接报错，定位问题
                sheet_report.append(sheet_status)
                err_msg = (
                    f"MSG sheet 缺少主键列。sheet='{sheetname}' "
                    f"需要列之一={PRIMARY_KEYS}，实际列={orig_cols}"
                )
                raise ValueError(err_msg)

            # 复制并构造“日期=sheetname”
            pdf_local = pdf.copy()
            pdf_local["日期"] = sheetname

            # 仅对这一张表做最小 rename：把 pk_matched -> NSC_CODE；把度量中文列精确映射
            rename_map = {
                pk_matched: "NSC_CODE",
                "日期": "date",
                "进入私信客户数": "enter_private_count",
                "主动咨询客户数": "private_open_count",
                "私信留资客户数": "private_leads_count",
            }
            # 只改存在的列
            rename_map_eff = {k: v for k, v in rename_map.items() if k in pdf_local.columns}
            pdf_local = pdf_local.rename(columns=rename_map_eff)

            # 转 polars 并做 NSC 与 date 标准化
            df = df_pandas_to_polars(pdf_local)
            # 此时必须已经有 NSC_CODE，否则直接断言失败
            assert "NSC_CODE" in df.columns, f"[严格] NSC_CODE 不存在。sheet='{sheetname}', cols={df.columns}"

            df = normalize_nsc_col(df, "NSC_CODE")
            df = ensure_date_column(df, "date")

            # 只保留关心列（其余列不进合并，避免后续被误导）
            keep = [c for c in ["NSC_CODE","date","enter_private_count","private_open_count","private_leads_count"] if c in df.columns]
            df = df.select(keep)

            per_sheet_frames.append(df)
            sheet_status["kept_cols"] = keep
            sheet_report.append(sheet_status)

        # 合并各 sheet（同一主键标准化后再合并，避免列名不一致问题）
        if per_sheet_frames:
            df = pl.concat(per_sheet_frames, how="vertical")
            # 聚合到 NSC_CODE+date
            df = process_single_table(df, mapping={}, sum_cols=["enter_private_count","private_open_count","private_leads_count"])
            dfs["msg"] = df

        # 打印一份可追溯报告（出现在日志里）
        logger.info(f"[MSG 严格模式报告] {sheet_report}")


    # 4. account_bi_file
    if "account_bi_file" in local_paths:
        p = local_paths["account_bi_file"]
        df = None
        if p.lower().endswith(('.csv', '.txt')):
            df = read_csv_polars(p)
        elif p.lower().endswith(('.xlsx', '.xls')):
            pdf = read_excel_as_pandas(p, sheet_name=0)
            df = df_pandas_to_polars(pdf)
        if df is not None:
            dfs["account_bi"] = process_single_table(df, ACCOUNT_BI_MAP, ["live_leads","short_video_plays"])

    # 5. leads_file
    if "leads_file" in local_paths:
        p = local_paths["leads_file"]
        df = None
        if p.lower().endswith(('.csv', '.txt')):
            df = read_csv_polars(p)
        elif p.lower().endswith(('.xlsx', '.xls')):
            pdf = read_excel_as_pandas(p, sheet_name=0)
            df = df_pandas_to_polars(pdf)
        if df is not None:
            dfs["leads"] = process_single_table(df, LEADS_MAP, ["small_wheel_leads"])

    # 6. DR1_file, DR2_file
    dr_frames = []
    for key in ["DR1_file", "DR2_file"]:
        if key in local_paths:
            p = local_paths[key]
            if p.lower().endswith(('.csv', '.txt')):
                df = read_csv_polars(p)
                dr_frames.append(df)
            else:
                logger.warning(f"{key} 只支持CSV格式，已跳过: {p}")
    if dr_frames:
        dr_df = pl.concat(dr_frames, how="vertical")
        dfs["dr"] = process_dr_table(dr_df)

    # 7. Spending_file
    if "Spending_file" in local_paths:
        p = local_paths["Spending_file"]
        df = None
        if p.lower().endswith(('.csv', '.txt')):
            df = read_csv_polars(p)
        elif p.lower().endswith(('.xlsx', '.xls')):
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
            dfs["spending"] = process_single_table(df, SPENDING_MAP, ["spending_net"])

    # 8. account_base_file
    account_base = None
    if "account_base_file" in local_paths:
        p = local_paths["account_base_file"]
        all_sheets = read_excel_as_pandas(p, sheet_name=None)
        account_base = process_account_base(all_sheets)

    keys = []
    for k, v in dfs.items():
        if "NSC_CODE" in v.columns and "date" in v.columns:
            keys.append(v.select(["NSC_CODE", "date"]).unique())
    if not keys:
        raise ValueError("No frames with NSC_CODE + date found.")
    base = pl.concat(keys).unique().sort(["NSC_CODE", "date"])

    for name, df in dfs.items():
        if "date" in df.columns:
            base = base.join(df, on=["NSC_CODE", "date"], how="left")
        else:
            base = base.join(df, on="NSC_CODE", how="left")

    if account_base is not None:
        base = base.join(account_base, on="NSC_CODE", how="left")

    base = base.with_columns([
        pl.col("date").dt.month().cast(pl.Utf8).str.zfill(2).alias("月份"),
        pl.col("date").dt.day().cast(pl.Utf8).str.zfill(2).alias("日期")
    ])

    return base
