from __future__ import annotations
import pandas as pd


def detect_market_regime(
    index_df: pd.DataFrame,
    price_col: str = "adj_close"
) -> pd.Series:
    """
    Simple, robust market regime filter.

    True  = Risk ON  (trade allowed)
    False = Risk OFF (stay flat)

    Logic:
    Price > long-term EMA
    """

    df = index_df.copy()

    # Long-term trend filter (CTA-style)
    df["ema_100"] = df[price_col].ewm(span=100, adjust=False).mean()

    df["RISK_ON"] = df[price_col] > df["ema_100"]

    return df.set_index("DATE")["RISK_ON"]
