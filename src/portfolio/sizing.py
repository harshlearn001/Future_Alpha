from __future__ import annotations
import pandas as pd


def vol_target_weights(
    df: pd.DataFrame,
    vol_col: str = "vol_20d",
    target_portfolio_vol: float = 0.18,   # 18% annual
    max_weight: float = 0.20
) -> pd.DataFrame:
    """
    Volatility-parity position sizing
    """

    df = df.copy()

    df = df.dropna(subset=[vol_col])
    if df.empty:
        return df

    # inverse volatility
    df["inv_vol"] = 1.0 / df[vol_col]
    df["raw_weight"] = df["inv_vol"] / df["inv_vol"].sum()

    # portfolio vol estimate
    portfolio_vol = (df["raw_weight"] * df[vol_col]).sum()
    scale = target_portfolio_vol / portfolio_vol if portfolio_vol > 0 else 1.0

    df["weight"] = df["raw_weight"] * scale

    # cap single name risk
    df["weight"] = df["weight"].clip(upper=max_weight)

    # renormalize
    df["weight"] = df["weight"] / df["weight"].sum()

    return df
