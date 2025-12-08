from __future__ import annotations

"""
Future_Alpha - Daily Ranking Pipeline
"""

print("Future Alpha Ready")

import pandas as pd

from src.config.paths import ensure_dirs, PROCESSED_DIR
from src.data.universe import get_active_symbols_list
from src.data.loader import load_clean_daily, load_symbol_history
from src.features.momentum import add_momentum_features
from src.features.oi_features import add_oi_features
from src.signals.rules import build_cross_sectional_score
from src.utils.logging import setup_logger


# -------------------------------------------------
# Helpers
# -------------------------------------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().upper() for c in df.columns]
    return df


def detect_expiry_column(df: pd.DataFrame) -> str:
    for col in ["EXPIRY", "EXPIRY_DT", "EXP_DATE", "EXPIRY_DATE"]:
        if col in df.columns:
            return col
    raise KeyError(f"No expiry column found: {list(df.columns)}")


def detect_close_column(df: pd.DataFrame) -> str:
    for col in ["CLOSE", "SETTLE_PR", "SETTLE_PRICE"]:
        if col in df.columns:
            return col
    raise KeyError(f"No close column found: {list(df.columns)}")


def detect_oi_column(df: pd.DataFrame) -> str:
    for col in ["OPEN_INT", "OPENINTEREST", "OI"]:
        if col in df.columns:
            return col
    raise KeyError(f"No OI column found: {list(df.columns)}")


# -------------------------------------------------
# MAIN
# -------------------------------------------------
def main():
    ensure_dirs()
    logger = setup_logger()

    # =================================================
    # 1️⃣ LOAD DAILY CLEAN FO
    # =================================================
    daily_df = load_clean_daily()
    daily_df = normalize_columns(daily_df)

    logger.info(f"Loaded clean daily FO with {len(daily_df)} rows")

    if "SYMBOL" not in daily_df.columns:
        raise KeyError("SYMBOL column missing in daily FO")

    expiry_col = detect_expiry_column(daily_df)
    close_col = detect_close_column(daily_df)
    oi_col = detect_oi_column(daily_df)

    daily_df[expiry_col] = pd.to_datetime(daily_df[expiry_col])

    # =================================================
    # 2️⃣ PICK NEAREST EXPIRY (CURRENT CONTRACT)
    # =================================================
    idx = daily_df.groupby("SYMBOL")[expiry_col].idxmax()
    daily_today = daily_df.loc[idx].reset_index(drop=True)

    daily_today = daily_today.rename(
        columns={
            close_col: "CLOSE",
            oi_col: "OPEN_INT",
        }
    )

    # Liquidity
    if "TRDVAL" not in daily_today.columns:
        daily_today["TRDVAL"] = (
            daily_today["VAL_INLAKH"]
            if "VAL_INLAKH" in daily_today.columns
            else 0.0
        )

    # =================================================
    # 3️⃣ BUILD FEATURES FROM CONTINUOUS FUTURES
    # =================================================
    symbols = get_active_symbols_list()
    feature_rows = []

    for sym in symbols:
        try:
            hist = load_symbol_history(sym)
        except FileNotFoundError:
            continue

        # normalize continuous data
        hist.columns = [c.lower() for c in hist.columns]

        # --- Momentum (adj_close ONLY) ---
        if "adj_close" not in hist.columns:
            continue

        hist = add_momentum_features(hist, price_col="adj_close")

        # --- OI features (oi → OPEN_INT normalized internally)
        hist = add_oi_features(hist)

        last = hist.iloc[-1].copy()
        last["SYMBOL"] = sym

        feature_rows.append(last)

    if not feature_rows:
        raise RuntimeError("No features generated from continuous futures")

    feat_df = pd.DataFrame(feature_rows)

    # =================================================
    # 4️⃣ MERGE DAILY + FEATURES
    # =================================================
    merged = daily_today.merge(
        feat_df,
        on="SYMBOL",
        how="inner",
    )

    logger.info(f"Universe after merge: {len(merged)} symbols")

    # =================================================
    # 5️⃣ BUILD RANKINGS
    # =================================================
    ranked = build_cross_sectional_score(merged)

    # =================================================
    # 6️⃣ TOP 5 EXPLANATION
    # =================================================
    top5 = ranked.sort_values("SCORE", ascending=False).head(5).copy()

    cols = [
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

    cols = [c for c in cols if c in top5.columns]

    print("\n📊 TOP 5 RANKING EXPLANATION:")
    print(top5[cols].to_string(index=False))

    explain_path = PROCESSED_DIR / "daily_ranking_explain_top5.csv"
    top5[cols].to_csv(explain_path, index=False)
    logger.info(f"✅ Ranking explanation saved: {explain_path}")

    # =================================================
    # 7️⃣ SAVE FULL RANKING
    # =================================================
    out_path = PROCESSED_DIR / "daily_ranking_latest.csv"
    ranked.to_csv(out_path, index=False)

    logger.info(f"✅ Daily ranking saved: {out_path}")


if __name__ == "__main__":
    main()
