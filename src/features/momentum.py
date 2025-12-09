# src/features/momentum.py
import pandas as pd


def add_momentum_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["SYMBOL", "DATE"]).copy()

    px = df["adj_close"]

    df["mom_3d"]  = px.groupby(df["SYMBOL"]).pct_change(3)
    df["mom_5d"]  = px.groupby(df["SYMBOL"]).pct_change(5)
    df["mom_10d"] = px.groupby(df["SYMBOL"]).pct_change(10)

    # ✅ Normalize per SYMBOL (critical)
    for c in ["mom_3d", "mom_5d", "mom_10d"]:
        mean = df.groupby("SYMBOL")[c].transform("mean")
        std  = df.groupby("SYMBOL")[c].transform("std")
        df[c] = (df[c] - mean) / std

    return df
