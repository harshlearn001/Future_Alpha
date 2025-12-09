# src/features/volatility.py
import pandas as pd


def add_volatility_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds rolling volatility features.
    """

    df = df.sort_values(["SYMBOL", "DATE"]).copy()

    ret = df.groupby("SYMBOL")["adj_close"].pct_change()

    df["vol_5d"]  = ret.rolling(5).std()
    df["vol_10d"] = ret.rolling(10).std()
    df["vol_20d"] = ret.rolling(20).std()

    # Volatility regime
    df["vol_regime"] = (
        df["vol_10d"] / df["vol_20d"]
    )

    return df
