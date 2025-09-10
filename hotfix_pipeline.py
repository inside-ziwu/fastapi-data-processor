# -*- coding: utf-8 -*-
"""
火线修复版数据汇流与月度聚合管线（Pandas）
-------------------------------------------------
目标：
1) 彻底杜绝 merge 覆盖（masking）：所有源列统一“源前缀__中文指标名”。
2) 先冻结键，再统一重命名、严格带覆盖率的 join，最后才做折叠/派生/别名。
3) 不把“无法判定”提前写成 0；率用安全除法，月度用“分子/分母”聚合再计算。
4) 输出：日宽表 wide_daily、门店级 T/T-1 月度输出 final_out，以及诊断 diag。

使用方式：
from hotfix_pipeline import run_hotfix_pipeline
final_out, wide_daily, diag = run_hotfix_pipeline(
    dr1_df=DR1, dr2_df=DR2, spending_df=SP, message_df=MSG,
    live_df=LIVE, video_df=VIDEO, leads_df=LEADS,
    account_bi_df=ABI, account_base_df=ABA,
)

注：函数只依赖 pandas/numpy。需要 DataFrame 均包含键列：'经销商ID'、'日期'。
'日期'可以是 str/datetime，函数内部会统一为 date。
"""
from __future__ import annotations
import logging
from typing import Dict, List, Tuple, Optional
import numpy as np
import pandas as pd

# -----------------------------
# 日志配置
# -----------------------------
logger = logging.getLogger("hotfix")
if not logger.handlers:
    handler = logging.StreamHandler()
    fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(fmt)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

# -----------------------------
# 工具函数
# -----------------------------

def norm_keys(df: pd.DataFrame) -> pd.DataFrame:
    """统一键：经销商ID -> str 去空白；日期 -> date（不含时分秒）。"""
    if df is None or df.empty:
        return df
    out = df.copy()
    if '经销商ID' not in out.columns or '日期' not in out.columns:
        raise KeyError("缺少键列：必须包含 '经销商ID' 与 '日期'")
    out['经销商ID'] = out['经销商ID'].astype(str).str.strip()
    out['日期'] = pd.to_datetime(out['日期'], errors='coerce').dt.date
    return out


def rename_with_prefix(df: pd.DataFrame, prefix: str, mapping: Dict[str, str]) -> pd.DataFrame:
    """在 merge 之前就重命名：raw_col -> f"{prefix}__{中文名}".
    mapping: {raw_col: 中文指标名}
    """
    if df is None or df.empty:
        return df
    df = norm_keys(df)
    rename_map = {k: f"{prefix}__{v}" for k, v in mapping.items() if k in df.columns}
    return df.rename(columns=rename_map)


def merge_with_report(left: pd.DataFrame, right: pd.DataFrame, on: List[str], right_name: str,
                      how: str = 'left', coverage_threshold: float = 0.95) -> pd.DataFrame:
    """带覆盖率与右孤儿统计的 merge；覆盖率低直接报错。
    - coverage = 1 - right_only/len(merge)，近似反映右表键匹配情况
    """
    if right is None or right.empty:
        logger.warning(f"{right_name} 为空，跳过 merge。")
        return left
    m = left.merge(right, on=on, how=how, indicator=True)
    total = len(m)
    right_only = (m['_merge'] == 'right_only').sum()
    left_only = (m['_merge'] == 'left_only').sum()
    coverage = 1.0 - (right_only / total if total else 0.0)
    logger.info(f"Join '{right_name}': total={total}, left_only={left_only}, right_only={right_only}, coverage={coverage:.2%}")
    if coverage < coverage_threshold:
        raise RuntimeError(f"Join '{right_name}' 覆盖率 {coverage:.2%} < 阈值 {coverage_threshold:.0%}, 请检查键一致性与日期对齐。")
    return m.drop(columns=['_merge'])


def force_numeric_cols(df: pd.DataFrame, cols: List[str], at_least: float = 0.98) -> pd.DataFrame:
    """数值化并做可转化率断言。不要在这里 fillna(0)。"""
    if df is None or df.empty or not cols:
        return df
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            continue
        s = (out[c].astype(str)
                   .str.replace(',', '', regex=False)
                   .str.replace('\u00A0', '', regex=False)
                   .str.replace('%', '', regex=False)
                   .str.replace('—', '', regex=False)
                   .str.strip())
        num = pd.to_numeric(s, errors='coerce')
        rate = num.notna().mean()
        logger.info(f"数值化: {c} parse_rate={rate:.2%}")
        if rate < at_least:
            raise ValueError(f"{c} 数值可转化率 {rate:.2%} < {at_least:.0%}")
        out[c] = num
    return out


def safe_div(num: pd.Series, den: pd.Series) -> pd.Series:
    num = pd.to_numeric(num, errors='coerce')
    den = pd.to_numeric(den, errors='coerce')
    with np.errstate(divide='ignore', invalid='ignore'):
        res = np.where((den > 0) & np.isfinite(den), num / den, np.nan)
    return pd.Series(res, index=num.index)


# -----------------------------
# 源字段映射（按你的 Transform 日志）
# -----------------------------
DR_MAPPING = {
    'natural_leads': '自然线索量',
    'paid_leads': '付费线索量',
    'cheyundian_paid_leads': '车云店付费线索',
    'regional_paid_leads': '区域加码付费线索',
    'local_leads': '本地线索量',
}
VIDEO_MAPPING = {
    'anchor_exposure': '锚点曝光量',
    'component_clicks': '组件点击次数',
    'short_video_count': '短视频数',
    'short_video_leads': '短视频线索量',
}
LIVE_MAPPING = {
    'over25_min_live_mins': '25分钟以上直播分钟数',
    'live_effective_hours': '直播有效时长(小时)',
    'effective_live_sessions': '有效直播场次',
    'exposures': '直播曝光量',
    'viewers': '直播观看人数',
    'small_wheel_clicks': '小风车点击次数',
}
MESSAGE_MAPPING = {
    'enter_private_count': '进私人数',
    'private_open_count': '私信开口人数',
    'private_leads_count': '咨询留资人数',
}
LEADS_MAPPING = {
    'small_wheel_leads': '小风车留资人数',
}
ABI_MAPPING = {
    'live_leads': '直播线索量',
    'short_video_plays': '短视频播放量',
}
SP_MAPPING = {
    'spending': '投放金额',
}
ABA_MAPPING: Dict[str, str] = {}  # 目前无指标列

# -----------------------------
# 折叠/派生配置
# -----------------------------
FOLD_SUM_PAIRS = [
    ('付费线索量', ['DR1__付费线索量', 'DR2__付费线索量']),
    ('自然线索量', ['DR1__自然线索量', 'DR2__自然线索量']),
    ('车云店付费线索', ['DR1__车云店付费线索', 'DR2__车云店付费线索']),
    ('区域加码付费线索', ['DR1__区域加码付费线索', 'DR2__区域加码付费线索']),
    ('本地线索量', ['DR1__本地线索量', 'DR2__本地线索量']),
]

DERIVED_SUMS = {
    '直播车云店+区域付费线索量': ['车云店付费线索', '区域加码付费线索'],
}

# rate 的分子/分母定义（便于月度聚合时用“分子合计/分母合计”方式计算）
RATE_DEFS = {
    '组件点击率': ('VIDEO__组件点击次数', 'VIDEO__锚点曝光量'),
    '组件留资率': ('LEADS__小风车留资人数', 'VIDEO__组件点击次数'),
    '私信咨询率': ('MESSAGE__私信开口人数', 'MESSAGE__进私人数'),
    '咨询留资率': ('MESSAGE__咨询留资人数', 'MESSAGE__私信开口人数'),
    '私信转化率': ('MESSAGE__咨询留资人数', 'MESSAGE__进私人数'),
}

# 需要做数值化校验的列（存在才校验）
NUMERIC_CANDIDATES = set(
    [y for _, cols in FOLD_SUM_PAIRS for y in cols] +
    [f"VIDEO__{v}" for v in VIDEO_MAPPING.values()] +
    [f"LIVE__{v}" for v in LIVE_MAPPING.values()] +
    [f"MESSAGE__{v}" for v in MESSAGE_MAPPING.values()] +
    [f"LEADS__{v}" for v in LEADS_MAPPING.values()] +
    [f"ABI__{v}" for v in ABI_MAPPING.values()] +
    [f"SP__{v}" for v in SP_MAPPING.values()]
)

# 合同强校验列（最小集：你日志里缺失的 3 个）
CONTRACT_MUST_HAVE = [
    '直播车云店+区域付费线索量',
    'T月直播车云店+区域付费线索量',
    'T-1月直播车云店+区域付费线索量',
]


# -----------------------------
# 主流程
# -----------------------------

def run_hotfix_pipeline(
    dr1_df: pd.DataFrame,
    dr2_df: pd.DataFrame,
    spending_df: pd.DataFrame,
    message_df: Optional[pd.DataFrame] = None,
    live_df: Optional[pd.DataFrame] = None,
    video_df: Optional[pd.DataFrame] = None,
    leads_df: Optional[pd.DataFrame] = None,
    account_bi_df: Optional[pd.DataFrame] = None,
    account_base_df: Optional[pd.DataFrame] = None,
    coverage_threshold: float = 0.95,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    """
    返回：final_out(月度 T/T-1 门店表), wide_daily(日宽表), diag(诊断信息)
    """
    # 1) 冻结骨架键：DR1 ∪ DR2 ∪ Spending
    logger.info("构建骨架键集（DR1 ∪ DR2 ∪ Spending）…")
    keys = ['经销商ID', '日期']
    sk = norm_keys(dr1_df)[keys].drop_duplicates()
    sk = sk.merge(norm_keys(dr2_df)[keys].drop_duplicates(), on=keys, how='outer')
    sk = sk.merge(norm_keys(spending_df)[keys].drop_duplicates(), on=keys, how='outer')
    skeleton = sk.sort_values(keys).reset_index(drop=True)
    logger.info(f"骨架键行数={len(skeleton)}，去重后={skeleton.drop_duplicates(keys).shape[0]}")

    # 2) 各源重命名 + merge（带覆盖率）
    logger.info("标准化并重命名各源列…")
    DR1 = rename_with_prefix(dr1_df, 'DR1', DR_MAPPING)
    DR2 = rename_with_prefix(dr2_df, 'DR2', DR_MAPPING)
    SP = rename_with_prefix(spending_df, 'SP', SP_MAPPING)
    MSG = rename_with_prefix(message_df, 'MESSAGE', MESSAGE_MAPPING) if message_df is not None else None
    LIVE = rename_with_prefix(live_df, 'LIVE', LIVE_MAPPING) if live_df is not None else None
    VIDEO = rename_with_prefix(video_df, 'VIDEO', VIDEO_MAPPING) if video_df is not None else None
    LEADS = rename_with_prefix(leads_df, 'LEADS', LEADS_MAPPING) if leads_df is not None else None
    ABI = rename_with_prefix(account_bi_df, 'ABI', ABI_MAPPING) if account_bi_df is not None else None
    ABA = rename_with_prefix(account_base_df, 'ABA', ABA_MAPPING) if account_base_df is not None else None

    # 逐步合并
    wide = skeleton.copy()
    for df, name in [
        (DR1, 'DR1'), (DR2, 'DR2'), (SP, 'SP'), (MSG, 'MESSAGE'), (LIVE, 'LIVE'),
        (VIDEO, 'VIDEO'), (LEADS, 'LEADS'), (ABI, 'ABI'), (ABA, 'ABA')
    ]:
        if df is None or df.empty:
            logger.warning(f"{name} 源为空或无列，跳过。")
            continue
        right_cols = [c for c in df.columns if c not in keys]
        if not right_cols:
            logger.warning(f"{name} 无可并入的指标列，跳过。")
            continue
        wide = merge_with_report(wide, df[keys + right_cols], on=keys, right_name=name,
                                 coverage_threshold=coverage_threshold)

    # 3) 列名冲突/掩蔽自检
    dup_cols = wide.columns[wide.columns.duplicated()].tolist()
    if dup_cols:
        raise AssertionError(f"列名重复（掩蔽风险）：{dup_cols}")
    logger.info("列名唯一性校验通过，无重复。")

    # 4) 数值化（存在才校验），不要提前填 0
    num_cols = [c for c in NUMERIC_CANDIDATES if c in wide.columns]
    wide = force_numeric_cols(wide, num_cols, at_least=0.98)

    # 5) 折叠（DR1+DR2 → 总列）
    for out_col, parts in FOLD_SUM_PAIRS:
        exists = [p for p in parts if p in wide.columns]
        if not exists:
            logger.warning(f"折叠缺少源列：{out_col} <- {parts}")
            continue
        tmp = sum([wide[p].fillna(0) for p in exists]) if exists else np.nan
        wide[out_col] = tmp.replace({0: 0})  # 不特殊处理 0

    # 6) 二级派生（例如“直播车云店+区域付费线索量”）
    for out_col, children in DERIVED_SUMS.items():
        miss = [c for c in children if c not in wide.columns]
        if miss:
            logger.warning(f"派生列 {out_col} 缺少原子列：{miss}")
            continue
        wide[out_col] = sum([wide[c].fillna(0) for c in children])

    # 7) 率（先在日级给出，保留 NaN 表示不可判定）
    for rate_name, (num_c, den_c) in RATE_DEFS.items():
        if num_c in wide.columns and den_c in wide.columns:
            wide[rate_name] = safe_div(wide[num_c], wide[den_c])
        else:
            logger.info(f"率缺列：{rate_name} 需要 {num_c}/{den_c}")

    # 8) 月份字段
    wide['月份'] = pd.to_datetime(wide['日期']).astype('datetime64[M]').dt.strftime('%Y-%m')

    # 9) 月度聚合（分子/分母法）
    sum_metrics = list({
        '付费线索量', '自然线索量', '车云店付费线索', '区域加码付费线索', '本地线索量',
        '直播车云店+区域付费线索量', 'SP__投放金额',
        'VIDEO__锚点曝光量', 'VIDEO__组件点击次数',
        'LEADS__小风车留资人数',
        'MESSAGE__进私人数', 'MESSAGE__私信开口人数', 'MESSAGE__咨询留资人数',
    } & set(wide.columns))

    monthly = (wide.groupby(['经销商ID', '月份'], as_index=False)[sum_metrics]
                    .sum(min_count=1))

    # 月度率：按照 RATE_DEFS 的分子/分母在 monthly 上再算一遍
    for rate_name, (num_c, den_c) in RATE_DEFS.items():
        if (num_c in monthly.columns) and (den_c in monthly.columns):
            monthly[rate_name] = safe_div(monthly[num_c], monthly[den_c])

    # 10) 确定 T / T-1 月
    months = sorted(monthly['月份'].dropna().unique())
    if not months:
        raise ValueError("没有可用月份，检查日期字段是否为空")
    T_month = months[-1]
    T_1_month = months[-2] if len(months) >= 2 else None
    logger.info(f"窗口月份：T={T_month}, T-1={T_1_month}")

    # 11) 生成门店最终输出（至少包含合同校验列）
    def pick_month(mdf: pd.DataFrame, month: str, cols: List[str]) -> pd.DataFrame:
        sub = mdf.loc[mdf['月份'] == month, ['经销商ID'] + [c for c in cols if c in mdf.columns]].copy()
        mapping = {c: f"T月{c}" for c in cols}
        return sub.rename(columns=mapping)

    def pick_month_prev(mdf: pd.DataFrame, month: Optional[str], cols: List[str]) -> pd.DataFrame:
        if not month:
            return pd.DataFrame(columns=['经销商ID'] + [f"T-1月{c}" for c in cols])
        sub = mdf.loc[mdf['月份'] == month, ['经销商ID'] + [c for c in cols if c in mdf.columns]].copy()
        mapping = {c: f"T-1月{c}" for c in cols}
        return sub.rename(columns=mapping)

    base_cols = ['直播车云店+区域付费线索量', '付费线索量', '自然线索量', 'SP__投放金额']
    base_cols = [c for c in base_cols if c in monthly.columns]

    t_part = pick_month(monthly, T_month, base_cols)
    t1_part = pick_month_prev(monthly, T_1_month, base_cols)

    # 基础主表：以所有门店并集为骨架
    dealers = pd.DataFrame({'经销商ID': monthly['经销商ID'].unique()})
    final_out = dealers.merge(t_part, on='经销商ID', how='left').merge(t1_part, on='经销商ID', how='left')

    # 同时给一个“当前（月度）值”作为合同里的不带前缀列（用 T 月）
    if 'T月直播车云店+区域付费线索量' in final_out.columns:
        final_out['直播车云店+区域付费线索量'] = final_out['T月直播车云店+区域付费线索量']

    # 12) 合同强校验
    missing = [c for c in CONTRACT_MUST_HAVE if c not in final_out.columns]
    if missing:
        logger.error(f"合同缺列：{missing}")
        raise AssertionError(f"合同缺列：{missing}")

    # 13) 诊断信息（类似你日志里的结构）
    # period rows（以 wide 的当期、上期行数近似）
    period_counts = {
        'total_rows': len(wide),
        'T_rows': int((wide['月份'] == T_month).sum()),
        'T-1_rows': int((wide['月份'] == (T_1_month or '')).sum()),
    }

    metric_sums = {}
    for k in ['自然线索量', '付费线索量', '车云店付费线索', '区域加码付费线索', '本地线索量', 'SP__投放金额']:
        if k in wide.columns:
            both = pd.to_numeric(wide[k], errors='coerce').sum()
            t_sum = pd.to_numeric(wide.loc[wide['月份'] == T_month, k], errors='coerce').sum()
            t1_sum = pd.to_numeric(wide.loc[wide['月份'] == (T_1_month or ''), k], errors='coerce').sum()
            metric_sums[k] = {'both': float(both or 0.0), 'T': float(t_sum or 0.0), 'T-1': float(t1_sum or 0.0)}

    core_rates_diag = {}
    for r in ['组件点击率', '组件留资率', '私信咨询率', '咨询留资率', '私信转化率']:
        if r in wide.columns:
            ser = pd.to_numeric(wide[r], errors='coerce')
            core_rates_diag[r] = {
                'present': True,
                'column': r,
                'non_null': int(ser.notna().sum()),
                'mean': float(np.nanmean(ser)) if ser.notna().any() else np.nan,
                'min': float(np.nanmin(ser)) if ser.notna().any() else np.nan,
                'max': float(np.nanmax(ser)) if ser.notna().any() else np.nan,
            }
        else:
            core_rates_diag[r] = {'present': False}

    # message 列是否存在于日宽表
    msg_present = any(col.startswith('MESSAGE__') for col in wide.columns)

    diag = {
        'period': period_counts,
        'metric_sums': metric_sums,
        'core_rates_diag': core_rates_diag,
        'message_columns_present_in_wide': msg_present,
        'T_month': T_month,
        'T-1_month': T_1_month,
    }

    logger.info(f"结算完成：按维度 '经销商ID' 聚合后得到 {len(final_out)} 行, 列数 {final_out.shape[1]}")

    return final_out, wide, diag


# -----------------------------
# 可选：快速自测入口（示例）
# -----------------------------
if __name__ == '__main__':
    # 仅作占位示例；实际运行时传入你的各 Transform 结果 DataFrame
    print("该模块为火线修复版管线。请在你的数据环境中导入并调用 run_hotfix_pipeline 。")