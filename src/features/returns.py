# src/features/returns.py
from __future__ import annotations
import pandas as pd

def add_log_returns(df: pd.DataFrame, price_col: str = "close", prefix: str = "ret") -> pd.DataFrame:
    df = df.copy()
    df[f"{prefix}_1d"] = (df[price_col].pct_change()).fillna(0.0)
    return df

def add_rolling_returns(df: pd.DataFrame, price_col: str = "close", windows=(3, 5, 10), prefix: str = "ret") -> pd.DataFrame:
    df = df.copy()
    for w in windows:
        df[f"{prefix}_{w}d"] = df[price_col].pct_change(w)
    return df
