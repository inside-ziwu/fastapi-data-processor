import polars as pl
import os
from src.analysis.settlement import compute_settlement_cn

# --- 1. 设置环境变量以模拟我们的测试配置 ---
os.environ["LEVEL_NORMALIZE_BY_NSC"] = "on"
os.environ["LEVEL_SUM_EXCLUDE_EMPTY_NSC"] = "on"

# --- 2. 构造一份有代表性的样本数据 ---
sample_data = {
    "层级": ["L1", "L1", "L1", "L2", "L2", "L2"],
    "经销商ID": ["NSC001", "NSC002", None, "NSC003", "NSC003", ""],
    "门店名称": ["A店", "B店", "未知店铺", "C店", "C店", "未知店铺"],
    "自然线索量": [10, 20, 5, 15, 25, 8], # L1 Sum=30 (NSC001/2), L2 Sum=40 (NSC003)
    "付费线索量": [100, 150, 50, 200, 250, 80], # L1 Sum=300 (NSC001/2), L2 Sum=450 (NSC003)
    "T月付费线索量": [60, 90, 30, 120, 150, 40],
    "T-1月付费线索量": [40, 60, 20, 80, 100, 40],
    "有效天数": [30, 30, 30, 31, 31, 31], # L1 max=30, L2 max=31
    "T月有效天数": [30, 30, 30, 31, 31, 31],
    "T-1月有效天数": [30, 30, 30, 31, 31, 31],
    # 为了计算CPL，添加金额
    "车云店+区域投放总金额": [1000, 1500, 500, 2000, 2500, 800],
    "T月车云店+区域投放总金额": [600, 900, 300, 1200, 1500, 400],
    "T-1月车云店+区域投放总金额": [400, 600, 200, 800, 1000, 400],
    # 为了计算总付费线索
    "车云店付费线索": [50, 75, 25, 100, 125, 40],
    "区域加码付费线索": [50, 75, 25, 100, 125, 40],
    "T月车云店付费线索": [30, 45, 15, 60, 75, 20],
    "T月区域加码付费线索": [30, 45, 15, 60, 75, 20],
    "T-1月车云店付费线索": [20, 30, 10, 40, 50, 20],
    "T-1月区域加码付费线索": [20, 30, 10, 40, 50, 20],
}
df = pl.DataFrame(sample_data)

print("--- 原始输入数据 ---")
print(df)

# --- 3. 执行计算 ---
try:
    # 按“层级”维度聚合
    print("\n" + "="*30)
    print("--- 按“层级”维度聚合的结果 ---")
    print("="*30)
    level_result = compute_settlement_cn(df, dimension="层级", expose_debug_cols=True)
    print(level_result)

    # 按“经销商ID”维度聚合
    print("\n" + "="*30)
    print("--- 按“经销商ID”维度聚合的结果 ---")
    print("="*30)
    dealer_result = compute_settlement_cn(df, dimension="经销商ID", expose_debug_cols=True)
    print(dealer_result)

except Exception as e:
    print(f"\n计算出错: {e}")
