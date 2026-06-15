#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
aggregate_kline_grid_simple
等比网格法 K线聚合（简化算法）
Author: Joyful_Flame_Finance
"""

import math

import numpy as np
import pandas as pd
from tqdm import tqdm

# ==========================
# 配置
# ==========================
INTERVAL = "1m"
DB_FILE = f"kline_{INTERVAL}.db"
SRC_TABLE = f"kline_{INTERVAL}"
DST_TABLE = f"kline_{INTERVAL}_grid"
THRESHOLD = 0.01

LOG_BASE = math.log(1 + THRESHOLD)


# ==========================
# 网格工具函数
# ==========================
def calc_grid(price: float) -> float:
    return math.log(price) / LOG_BASE


def calc_direction_and_grid(open_g, close_g):
    """
    返回 (direction, grid_id or None)
    """
    if math.floor(open_g) == math.floor(close_g):
        return 0, None

    if close_g > open_g:
        return 1, math.floor(close_g)
    else:
        return -1, math.floor(close_g) + 1


# ==========================
# 数据库初始化
# ==========================

# ==========================
# 读取 symbol 列表
# ==========================
symbols = pd.read_csv("/tmp/btc.csv")["symbol"].tolist()
symbols = set(symbols)
print(f"Loaded {len(symbols)} new symbols")

# ==========================
# 主聚合逻辑
# ==========================
# for symbol in tqdm(symbols, desc="Aggregating"):
df = pd.read_csv("/tmp/btc.csv")

if df.empty:
    print("empty")
    exit()

# ===== 1. 向量化计算网格 =====
# 替换原本的 df["open"].apply(...)，改用 numpy 数组运算，速度极快
df["open_grid"] = np.log(df["open"]) / LOG_BASE
df["close_grid"] = np.log(df["close"]) / LOG_BASE

floor_open = np.floor(df["open_grid"])
floor_close = np.floor(df["close_grid"])

# ===== 2. 向量化寻找穿越点 =====
cond_cross = floor_open != floor_close
cond_up = cond_cross & (df["close_grid"] > df["open_grid"])
cond_down = cond_cross & (df["close_grid"] <= df["open_grid"])

# 初始化 raw_grid 为 NaN（代表没有发生穿越，在同一网格内横盘）
df["raw_grid"] = np.nan

# 记录发生穿越的那一刻的网格 ID (完全还原原代码的数学逻辑)
df.loc[cond_up, "raw_grid"] = np.floor(df.loc[cond_up, "close_grid"])
df.loc[cond_down, "raw_grid"] = np.floor(df.loc[cond_down, "close_grid"]) + 1
# ===== 3. 核心技巧：向前填充 (ffill) 吸收震荡 =====
# 这一步先算出每根 K 线实际对应的网格（包含未突破时的向前填充）
df["grid_id_actual"] = df["raw_grid"].ffill().bfill()

# 【核心修复】：将网格 ID 整体向后平移 1 行！
# 这样发生穿越的那一根 K 线（触发点）就会被留在先前的老组里，而下一根 K 线才会正式开启新组。
df["grid_id"] = df["grid_id_actual"].shift(1).bfill().astype(int)

# 如果这个品种的历史走势犹如死水，没有任何一次穿越，直接跳过
result = df["grid_id"].isna().all()
if result is False:
    print("empty")
    exit()

# ===== 4. 划分连续分组 (Group ID) =====
# 比较当前的 grid_id 和上一行的 grid_id，只要发生变化，组号就 +1
df["group_id"] = (df["grid_id"] != df["grid_id"].shift()).cumsum()

# ===== 5. 分组聚合 (GroupBy) =====
agg_funcs = {
    "symbol": "first",
    "start_time": "first",
    "close_time": "last",
    "open": "first",
    "high": "max",
    "low": "min",
    "close": "last",
    "volume": "sum",
    "quote_asset_volume": "sum",
    "trades": "sum",
    "taker_buy_base_asset_volume": "sum",
    "taker_buy_quote_asset_volume": "sum",
    "grid_id": "first",
}

# 一次性聚合出所有网格 K 线
gdf = df.groupby("group_id").agg(agg_funcs)

# 补充 count 字段 (即每组包含了多少根 1 分钟 K 线)
gdf["count"] = df.groupby("group_id").size()  # type: ignore

# 类型还原 (防止 numpy 浮点运算影响整型字段)
gdf = gdf.astype(
    {
        "start_time": int,
        "close_time": int,
        "trades": int,
        "count": int,
        "grid_id": int,
    }
)

# 【注意】严格还原你原代码的逻辑：
# 你原代码中的 `buffer` 只有在跨入新网格时才结算，如果历史数据走完了，但最后一个网格还没破位，
# 原代码会直接丢弃这个未走完的 buffer。为了与你的原结果保持100%一致，我们要砍掉最后一行未完成的组。
if len(gdf) > 0:
    gdf = gdf.iloc[:-1]

# ==========================
# 写入数据库
# ==========================
if not gdf.empty:
    gdf.to_csv("/tmp/grid_agg.csv", index=False)
print("Grid aggregation (simplified) completed!")
