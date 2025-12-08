# src/signals/rules.py
from __future__ import annotations
import pandas as pd

from src.config.settings import SIGNAL_SETTINGS

def build_cross_sectional_score(df_last: pd.DataFrame) -> pd.DataFrame:
    """
    df_last: one row per symbol (latest date) with columns:
        SYMBOL, close, mom_3d, mom_5d, mom_10d, oi_breakout, TRDVAL / VOLUME
    Returns: df with SCORE and rank, sorted descending.
    """
    df = df_last.copy()

    # Simple composite score
    # Adjust weights later as per testing
    df["score_mom"] = (
        0.4 * df["mom_3d"].fillna(0)
        + 0.3 * df["mom_5d"].fillna(0)
        + 0.3 * df["mom_10d"].fillna(0)
    )
    df["score_oi"] = df["oi_breakout"].fillna(1.0)

    df["SCORE"] = df["score_mom"] * df["score_oi"]

    # Optional liquidity filter
    if "TRDVAL" in df.columns:
        df = df[df["TRDVAL"] >= SIGNAL_SETTINGS.min_liquidity].copy()

    df = df.sort_values("SCORE", ascending=False)
    df["RANK"] = range(1, len(df) + 1)

    # Keep top N
    top_n = SIGNAL_SETTINGS.top_n
    return df.head(top_n)
