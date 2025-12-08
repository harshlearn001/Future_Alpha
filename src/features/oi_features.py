from __future__ import annotations
import pandas as pd


def add_oi_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Open Interest features for continuous futures.
    Compatible with:
      - 'oi'  (continuous futures)
      - 'OPEN_INT' (daily FO, if ever used)

    Output:
      oi_chg_1d
      oi_5d_avg
      oi_10d_avg
      oi_breakout
    """

    df = df.copy()

    # -------------------------
    # ✅ Normalize OI column
    # -------------------------
    if "oi" in df.columns:
        df["OPEN_INT"] = df["oi"]
    elif "OPEN_INT" in df.columns:
        pass
    else:
        # No OI → neutral signal
        df["OPEN_INT"] = 0.0

    # Ensure numeric
    df["OPEN_INT"] = pd.to_numeric(df["OPEN_INT"], errors="coerce").fillna(0.0)

    # -------------------------
    # OI features
    # -------------------------
    df["oi_chg_1d"] = df["OPEN_INT"].pct_change()

    df["oi_5d_avg"] = df["OPEN_INT"].rolling(5).mean()
    df["oi_10d_avg"] = df["OPEN_INT"].rolling(10).mean()

    # Breakout logic
    df["oi_breakout"] = (
        df["OPEN_INT"] / df["oi_5d_avg"]
    ).replace([float("inf"), -float("inf")], 1.0).fillna(1.0)

    return df
