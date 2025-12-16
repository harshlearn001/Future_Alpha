import pandas as pd
from datetime import datetime

RANK_FILE = r"H:\Future_Alpha\data\processed\daily_ranking_latest.csv"
ML_FILE   = r"H:\Future_Alpha\data\processed\daily_ranking_latest_ml.csv"
OUT_FILE  = r"H:\Future_Alpha\data\processed\confluence_trades.csv"

TOP_ML   = 30   # ML strength filter
TOP_RANK = 30   # Price ranking filter

print("\n🚀 Building ML + Ranking Confluence Trades\n")

# Load files
rank = pd.read_csv(RANK_FILE)
ml   = pd.read_csv(ML_FILE)

# Normalize symbols
rank["SYMBOL"] = rank["SYMBOL"].str.upper().str.strip()
ml["SYMBOL"]   = ml["SYMBOL"].str.upper().str.strip()

# Rename columns cleanly
ml = ml.rename(columns={
    "SYMBOL": "symbol",
    "ml_score": "ml_score",
    "RANK": "ml_rank"
})

rank = rank.rename(columns={
    "SYMBOL": "symbol",
    "RANK": "rank_rank"
})

# -----------------------------
# 1️⃣ FILTER TOP ML SIGNALS
# -----------------------------
ml = ml.sort_values("ml_score", ascending=False).head(TOP_ML)
print(f"✔ Top ML signals selected: {len(ml)}")

# -----------------------------
# 2️⃣ MERGE WITH DAILY RANKING
# -----------------------------
merged = ml.merge(
    rank[["symbol", "rank_rank", "SCORE", "trend_boost"]],
    on="symbol",
    how="inner"
)

print(f"✔ Symbols after merge: {len(merged)}")

# -----------------------------
# 3️⃣ FILTER TOP PRICE RANKS
# -----------------------------
merged = merged[merged["rank_rank"] <= TOP_RANK]
print(f"✔ Symbols within top {TOP_RANK} ranks: {len(merged)}")

# -----------------------------
# 4️⃣ FINAL OUTPUT
# -----------------------------
merged["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

final = merged[[
    "symbol",
    "ml_score",
    "rank_rank",
    "SCORE",
    "trend_boost",
    "generated_at"
]].sort_values(
    ["ml_score", "SCORE"],
    ascending=False
)

print("\n✅ FINAL CONFLUENCE TRADE SHEET")
print(final)

final.to_csv(OUT_FILE, index=False)
print(f"\n📁 Saved to: {OUT_FILE}")
print("✔ Done.\n")
