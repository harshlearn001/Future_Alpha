from __future__ import annotations

"""
Future_Alpha - Daily Ranking + Position Sizing Pipeline
"""

import sys
import pandas as pd
from datetime import datetime

from src.config.paths import ensure_dirs, PROCESSED_DIR
from src.data.universe import get_active_symbols_list
from src.data.loader import load_clean_daily, load_symbol_history
from src.features.momentum import add_momentum_features
from src.features.oi_features import add_oi_features
from src.signals.rules import build_cross_sectional_score
from src.portfolio.sizing import vol_target_weights
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

    logger.info("🚀 Future_Alpha Daily Ranking Pipeline Started")

    # =================================================
    # 1️⃣ LOAD DAILY CLEAN FO
    # =================================================
    daily_df = load_clean_daily()
    daily_df = normalize_columns(daily_df)

    if daily_df.empty:
        raise RuntimeError("Daily clean FO file is empty")

    expiry_col = detect_expiry_column(daily_df)
    close_col = detect_close_column(daily_df)
    oi_col = detect_oi_column(daily_df)

    daily_df[expiry_col] = pd.to_datetime(daily_df[expiry_col])
    daily_df["DATE"] = pd.to_datetime(daily_df["DATE"])

    trade_date = daily_df["DATE"].max().date()
    logger.info(f"Trade date detected: {trade_date}")

    # =================================================
    # 2️⃣ SELECT CURRENT (FRONT) CONTRACT ✅
    # =================================================
    today = pd.Timestamp(trade_date)

    daily_df = daily_df[daily_df[expiry_col] >= today]

    idx = (
        daily_df
        .sort_values(expiry_col)
        .groupby("SYMBOL", as_index=False)
        .head(1)
        .index
    )

    daily_today = daily_df.loc[idx].copy().reset_index(drop=True)

    daily_today = daily_today.rename(
        columns={
            close_col: "CLOSE",
            oi_col: "OPEN_INT",
        }
    )

    if "TRDVAL" not in daily_today.columns:
        daily_today["TRDVAL"] = (
            daily_today["VAL_INLAKH"]
            if "VAL_INLAKH" in daily_today.columns
            else 0.0
        )

    logger.info(f"Front contract universe: {len(daily_today)} symbols")

    # =================================================
    # 3️⃣ CONTINUOUS FEATURES
    # =================================================
    rows = []

    for sym in get_active_symbols_list():
        try:
            hist = load_symbol_history(sym)
        except FileNotFoundError:
            continue

        hist.columns = [c.lower() for c in hist.columns]

        if "adj_close" not in hist.columns:
            continue

        hist = add_momentum_features(hist, price_col="adj_close")
        hist = add_oi_features(hist)

        last = hist.iloc[-1].copy()
        last["SYMBOL"] = sym
        rows.append(last)

    feat_df = pd.DataFrame(rows)

    required_feats = {"vol_20d"}
    feat_df = feat_df.dropna(subset=required_feats)

    merged = daily_today.merge(feat_df, on="SYMBOL", how="inner")

    if merged.empty:
        raise RuntimeError("No symbols left after feature merge")

    logger.info(f"Universe after feature merge: {len(merged)} symbols")

    # =================================================
    # 4️⃣ RANKINGS
    # =================================================
    ranked = build_cross_sectional_score(merged)
    ranked.insert(0, "DATE", trade_date)

    # =================================================
    # 5️⃣ POSITION SIZING
    # =================================================
    positions = vol_target_weights(
        ranked,
        vol_col="vol_20d",
        target_portfolio_vol=0.18,
        max_weight=0.20,
    )

    cols = ["DATE", "SYMBOL", "RANK", "SCORE", "vol_20d", "weight"]
    cols = [c for c in cols if c in positions.columns]

    print("\n📊 FINAL POSITION SIZES")
    print(positions[cols].to_string(index=False))

    pos_path = PROCESSED_DIR / "daily_positions_latest.csv"
    positions[cols].to_csv(pos_path, index=False)

    logger.info(f"✅ Daily positions saved → {pos_path}")

    # =================================================
    # 6️⃣ SAVE RANKING
    # =================================================
    rank_path = PROCESSED_DIR / "daily_ranking_latest.csv"
    ranked.to_csv(rank_path, index=False)

    logger.info(f"✅ Ranking saved → {rank_path}")
    logger.info("🎯 PIPELINE COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print(f"❌ PIPELINE FAILED: {e}")
        sys.exit(1)
