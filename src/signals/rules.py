from __future__ import annotations
import pandas as pd

from src.config.settings import SIGNAL_SETTINGS


def build_cross_sectional_score(df_last: pd.DataFrame) -> pd.DataFrame:
    """
    Build cross-sectional score for latest snapshot
    """

    df = df_last.copy()

    # -------------------------
    # 1️⃣ Sanitize inputs
    # -------------------------
    for col in ["mom_3d", "mom_5d", "mom_10d"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        else:
            df[col] = 0.0

    df["oi_breakout"] = pd.to_numeric(
        df.get("oi_breakout", 1.0),
        errors="coerce"
    ).fillna(1.0)

    # -------------------------
    # 2️⃣ Cross-sectional z-scores (KEY CHANGE)
    # -------------------------
    for col in ["mom_3d", "mom_5d", "mom_10d"]:
        std = df[col].std()
        if std == 0:
            df[col + "_z"] = 0.0
        else:
            df[col + "_z"] = (df[col] - df[col].mean()) / std

    df["score_mom"] = (
        0.4 * df["mom_3d_z"]
        + 0.3 * df["mom_5d_z"]
        + 0.3 * df["mom_10d_z"]
    )

    df["score_oi"] = df["oi_breakout"]

    # -------------------------
    # 3️⃣ Base SCORE
    # -------------------------
    df["SCORE"] = df["score_mom"] * df["score_oi"]

    # -------------------------
    # 4️⃣ Trend alignment boost (soft, not a filter)
    # -------------------------
    ema50 = df["adj_close"].ewm(span=50, adjust=False).mean()
    df["trend_boost"] = (df["adj_close"] > ema50).astype(int)

    df["SCORE"] *= (1.0 + 0.30 * df["trend_boost"])

    # -------------------------
    # 5️⃣ Liquidity filter
    # -------------------------
    if "TRDVAL" in df.columns:
        df = df[df["TRDVAL"] >= SIGNAL_SETTINGS.min_liquidity]

    # -------------------------
    # 6️⃣ Ranking
    # -------------------------
    df = df.sort_values("SCORE", ascending=False).reset_index(drop=True)
    df["RANK"] = df.index + 1

    return df.head(SIGNAL_SETTINGS.top_n)


def explain_score(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    cols = [
        "SYMBOL",
        "mom_3d",
        "mom_5d",
        "mom_10d",
        "score_mom",
        "score_oi",
        "trend_boost",
        "SCORE",
        "RANK",
    ]

    cols = [c for c in cols if c in df.columns]

    return (
        df.sort_values("RANK")
          .head(top_n)
          .loc[:, cols]
          .reset_index(drop=True)
    )
