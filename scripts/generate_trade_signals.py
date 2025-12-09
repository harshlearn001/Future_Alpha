from __future__ import annotations

import pandas as pd

RANK_FILE = "data/processed/daily_ranking_latest.csv"


def main():
    # -------------------------------------------------
    # Load latest rankings
    # -------------------------------------------------
    df = pd.read_csv(RANK_FILE, parse_dates=["DATE"])

    if df.empty:
        raise ValueError("daily_ranking_latest.csv is empty")

    # ✅ Last traded date (from data, single source of truth)
    signal_date = df["DATE"].max().date()
    date_str = signal_date.strftime("%d-%m-%Y")

    # -------------------------------------------------
    # BUY: Top 5 only
    # -------------------------------------------------
    buy = (
        df.sort_values("RANK")
          .head(5)[["SYMBOL", "RANK", "SCORE"]]
          .copy()
    )

    buy.insert(0, "SIGNAL_DATE", signal_date)
    buy["SIGNAL"] = "BUY"

    # -------------------------------------------------
    # Save
    # -------------------------------------------------
    out_file = f"data/processed/trade_signals_for_{date_str}.csv"
    buy.to_csv(out_file, index=False)

    print("\n✅ BUY SIGNALS GENERATED")
    print(buy)
    print(f"\n📅 Signal Date : {signal_date}")
    print(f"📁 Saved to    : {out_file}")


if __name__ == "__main__":
    main()
