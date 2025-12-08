# src/signals/rules.py
from __future__ import annotations

import pandas as pd
from src.config.settings import SIGNAL_SETTINGS


def build_cross_sectional_score(df_last: pd.DataFrame) -> pd.DataFrame:
    """
    Build cross-sectional score for latest snapshot.

    Required columns:
        SYMBOL
        mom_3d, mom_5d, mom_10d
        oi_breakout
        TRDVAL (optional)

    Returns:
        DataFrame with SCORE and RANK
    """

    df = df_last.copy()

    # -------------------------
    # 1️⃣ Sanitize inputs (CRITICAL)
    # -------------------------
    for col in ["mom_3d", "mom_5d", "mom_10d"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    if "oi_breakout" in df.columns:
        df["oi_breakout"] = pd.to_numeric(df["oi_breakout"], errors="coerce").fillna(1.0)
    else:
        df["oi_breakout"] = 1.0  # neutral if missing

    # -------------------------
    # 2️⃣ Score components
    # -------------------------
    df["score_mom"] = (
        0.4 * df["mom_3d"]
        + 0.3 * df["mom_5d"]
        + 0.3 * df["mom_10d"]
    )

    df["score_oi"] = df["oi_breakout"]

    # -------------------------
    # 3️⃣ Final SCORE
    # -------------------------
    df["SCORE"] = df["score_mom"] * df["score_oi"]

    # Drop rows where score is invalid
    df = df[df["SCORE"].notna()]

    # -------------------------
    # 4️⃣ Liquidity filter (AFTER scoring)
    # -------------------------
    if "TRDVAL" in df.columns:
        min_liq = SIGNAL_SETTINGS.min_liquidity
        df = df[df["TRDVAL"] >= min_liq]

    # -------------------------
    # 5️⃣ Ranking
    # -------------------------
    df = df.sort_values("SCORE", ascending=False).reset_index(drop=True)
    df["RANK"] = df.index + 1

    # -------------------------
    # 6️⃣ Top-N cutoff
    # -------------------------
    top_n = SIGNAL_SETTINGS.top_n
    return df.head(top_n)


def explain_score(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    """
    Explain top-ranked symbols using computed score components.
    Works ONLY on output of build_cross_sectional_score.
    """

    if df is None or df.empty:
        return pd.DataFrame()

    explain_cols = [
        "SYMBOL",
        "mom_3d",
        "mom_5d",
        "mom_10d",
        "oi_breakout",
        "score_mom",
        "score_oi",
        "SCORE",
        "RANK",
    ]

    # Keep only columns that exist (defensive)
    explain_cols = [c for c in explain_cols if c in df.columns]

    explain_df = (
        df
        .sort_values("RANK")
        .head(top_n)
        .loc[:, explain_cols]
        .reset_index(drop=True)
    )

    return explain_df
