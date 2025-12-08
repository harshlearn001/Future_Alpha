# src/features/momentum.py
from __future__ import annotations
import pandas as pd

def add_momentum_features(df: pd.DataFrame, price_col: str = "close") -> pd.DataFrame:
    """
    Input: continuous futures history per symbol
    Output: df with momentum columns
    """
    df = df.copy()
    df["mom_3d"] = df[price_col].pct_change(3)
    df["mom_5d"] = df[price_col].pct_change(5)
    df["mom_10d"] = df[price_col].pct_change(10)
    return df
