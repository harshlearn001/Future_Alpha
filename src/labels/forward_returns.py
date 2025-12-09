# src/labels/forward_returns.py
from __future__ import annotations
import pandas as pd


def build_forward_returns(df: pd.DataFrame, horizon: int = 1) -> pd.DataFrame:
    """
    Builds ML targets:
    1) next_ret       : clipped forward return
    2) direction      : 1 if positive, 0 if negative
    """

    df = df.sort_values(["SYMBOL", "DATE"]).copy()

    daily_ret = (
        df.groupby("SYMBOL")["adj_close"]
          .pct_change()
    )

    next_ret = (
        daily_ret
        .groupby(df["SYMBOL"])
        .shift(-horizon)
        .clip(-0.05, 0.10)
    )

    out = pd.DataFrame(
        {
            "next_ret": next_ret,
            "direction": (next_ret > 0).astype(int)
        }
    )

    return out
