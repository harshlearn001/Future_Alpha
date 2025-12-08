from __future__ import annotations

import pandas as pd
from pathlib import Path

from src.config.paths import CLEANED_HIST_DIR, PROCESSED_DIR
from src.signals.rules import build_cross_sectional_score
from src.utils.logging import setup_logger


def main():
    logger = setup_logger()
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    all_rows = []

    # Loop through all continuous futures
    for path in CLEANED_HIST_DIR.glob("*_CONT.csv"):
        symbol = path.stem.replace("_CONT", "")
        df = pd.read_csv(path, parse_dates=["date"])

        df = df.sort_values("date")

        # Build rolling features (reuse logic)
        df["mom_3d"] = df["adj_close"].pct_change(3)
        df["mom_5d"] = df["adj_close"].pct_change(5)
        df["mom_10d"] = df["adj_close"].pct_change(10)

        # OI
        df["oi_chg_1d"] = df["oi"].pct_change(1)
        df["oi_5d_avg"] = df["oi"].rolling(5).mean()
        df["oi_breakout"] = df["oi"] / df["oi_5d_avg"]

        df["SYMBOL"] = symbol
        all_rows.append(df)

    full = pd.concat(all_rows, ignore_index=True)
    full = full.dropna(subset=["mom_10d", "oi_breakout"])

    rankings = []

    for date, grp in full.groupby("date"):
        ranked = build_cross_sectional_score(grp)
        ranked["DATE"] = date
        rankings.append(ranked)

    final = pd.concat(rankings, ignore_index=True)

    out = PROCESSED_DIR / "daily_ranking_history.csv"
    final.to_csv(out, index=False)

    logger.info(f"✅ Historical daily rankings saved: {out}")


if __name__ == "__main__":
    main()
