from __future__ import annotations

import pandas as pd
from pathlib import Path

from src.config.paths import PROCESSED_DIR
from src.signals.rules import build_cross_sectional_score
from src.utils.logging import setup_logger


# ✅ SINGLE SOURCE OF TRUTH
SYMBOLS_DIR = Path(r"H:\Future_Alpha\data\master\symbols")


def main():
    logger = setup_logger()
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    all_rows = []

    logger.info("🚀 Building daily rankings from SYMBOL MASTER")

    # -------------------------------------------------
    # Load per-symbol master CSVs (latest data)
    # -------------------------------------------------
    for path in SYMBOLS_DIR.glob("*.csv"):
        symbol = path.stem

        
        df = pd.read_csv(path, parse_dates=["date", "expiry"])
        df = df.sort_values("date")

# ✅ normalize price columns
        if "adj_close" not in df.columns:
            df["adj_close"] = df["close"]

        # -------------------------------------------------
        # Features
        # -------------------------------------------------
        df["mom_3d"] = df["close"].pct_change(3)
        df["mom_5d"] = df["close"].pct_change(5)
        df["mom_10d"] = df["close"].pct_change(10)

        # OI features
        df["oi_chg_1d"] = df["oi"].pct_change(1)
        df["oi_5d_avg"] = df["oi"].rolling(5).mean()
        df["oi_breakout"] = df["oi"] / df["oi_5d_avg"]

        df["SYMBOL"] = symbol

        all_rows.append(df)

    if not all_rows:
        raise RuntimeError("❌ No symbol data loaded")

    # -------------------------------------------------
    # Combine universe
    # -------------------------------------------------
    full = pd.concat(all_rows, ignore_index=True)

    full = full.dropna(
        subset=["mom_10d", "oi_breakout"]
    )

    logger.info(
        f"📊 Universe: {full['SYMBOL'].nunique()} symbols | "
        f"{full['date'].nunique()} dates"
    )

    # -------------------------------------------------
    # Cross-sectional ranking (DATE-wise)
    # -------------------------------------------------
    rankings = []

    for date, grp in full.groupby("date"):
        ranked = build_cross_sectional_score(grp)
        ranked["DATE"] = date
        rankings.append(ranked)

    final = pd.concat(rankings, ignore_index=True)

    # -------------------------------------------------
    # Outputs
    # -------------------------------------------------
    hist_out = PROCESSED_DIR / "daily_ranking_history.csv"
    final.to_csv(hist_out, index=False)

    latest_date = final["DATE"].max()
    latest = final[final["DATE"] == latest_date]

    latest_out = PROCESSED_DIR / "daily_ranking_latest.csv"
    latest.to_csv(latest_out, index=False)

    logger.info(f"✅ Rankings written")
    logger.info(f"📅 Latest date: {latest_date}")
    logger.info(f"📁 {hist_out}")
    logger.info(f"📁 {latest_out}")


if __name__ == "__main__":
    main()
