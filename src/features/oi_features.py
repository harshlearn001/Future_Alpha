# src/features/oi_features.py
from __future__ import annotations
import pandas as pd

def add_oi_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    df: continuous futures or daily series with OPEN_INT column.
    """
    df = df.copy()
    if "OPEN_INT" not in df.columns and "open_int" in df.columns:
        df["OPEN_INT"] = df["open_int"]

    df["oi_chg_1d"] = df["OPEN_INT"].pct_change()
    df["oi_5d_avg"] = df["OPEN_INT"].rolling(5).mean()
    df["oi_10d_avg"] = df["OPEN_INT"].rolling(10).mean()
    df["oi_breakout"] = df["OPEN_INT"] / df["oi_10d_avg"]
    return df
