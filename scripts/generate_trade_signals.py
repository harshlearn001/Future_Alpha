import pandas as pd

# -------------------------------------------------
# Load rankings
# -------------------------------------------------
df = pd.read_csv("data/processed/daily_ranking_latest.csv", parse_dates=["DATE"])

# ✅ Determine signal date from data (single source of truth)
signal_date = df["DATE"].max()

# -------------------------------------------------
# BUY: Top 5
# -------------------------------------------------
buy = (
    df.sort_values("RANK")
      .head(5)
      [["SYMBOL", "RANK", "SCORE"]]
)
buy["SIGNAL"] = "BUY"

buy_symbols = set(buy["SYMBOL"])

# -------------------------------------------------
# SELL / AVOID: lowest score NOT in BUY
# -------------------------------------------------
sell = (
    df[~df["SYMBOL"].isin(buy_symbols)]
      .sort_values("SCORE")
      .head(5)
      [["SYMBOL", "RANK", "SCORE"]]
)
sell["SIGNAL"] = "SELL"

# -------------------------------------------------
# Combine + add DATE
# -------------------------------------------------
signals = pd.concat([buy, sell], ignore_index=True)

signals.insert(0, "SIGNAL_DATE", signal_date.strftime("%Y-%m-%d"))

out_path = "data/processed/trade_signals_tomorrow.csv"
signals.to_csv(out_path, index=False)

print("\n✅ TRADE SIGNALS GENERATED")
print(signals)
print(f"\n📅 Signal Date  : {signal_date.date()}")
print(f"📁 Saved to    : {out_path}")
