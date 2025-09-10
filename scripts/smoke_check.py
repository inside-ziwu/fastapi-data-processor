import polars as pl

KEYS = ["经销商ID","层级","门店名称"]
METRICS = [
    "自然线索量","付费线索量","本地线索量",
    "直播车云店+区域付费线索量",
    "车云店+区域投放总金额",
    "小风车点击次数","小风车留资量",
    "组件点击次数","组件留资人数（获取线索量）",
    "场观","曝光人数","进私人数","私信开口人数","咨询留资人数",
]

def ok(x): return int(pl.col(x).is_not_null().sum()), float(pl.col(x).sum())

def run(df: pl.DataFrame):
    print(f"rows={df.height}, cols={df.width}")
    for m in METRICS:
        if m in df.columns:
            nz = df.select(pl.col(m).is_not_null().sum()).item()
            s  = df.select(pl.col(m).fill_null(0).sum()).item()
            print(f"{m: <20}  non_null={nz:>6}  sum={s}")
        else:
            print(f"{m: <20}  MISSING")

if __name__ == "__main__":
    # 你可以把 settlement 前的“宽表”保存到 parquet，再读取
    df = pl.read_parquet("wide_table_before_settlement.parquet")
    run(df)
