from __future__ import annotations

import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

# =====================================================
# PATHS
# =====================================================
ROOT = Path(__file__).resolve().parents[1]

RANK_FILE = ROOT / "data" / "processed" / "daily_ranking_latest.csv"
ML_FILE   = ROOT / "data" / "processed" / "daily_ranking_latest_ml.csv"
OUT_FILE  = ROOT / "data" / "processed" / "confluence_trades.csv"

TOP_RANK = 30
TOP_ML   = 30

print("\nSTEP 5 | GENERATE CONFLUENCE TRADES")
print("-" * 60)

def main() -> int:
    # -------------------------------------------------
    # Load ranking (mandatory)
    # -------------------------------------------------
    if not RANK_FILE.exists():
        print("Ranking file not found:", RANK_FILE)
        return 1

    rank = pd.read_csv(RANK_FILE)

    if rank.empty:
        print("Ranking file is empty")
        return 1

    rank.columns = [c.upper().strip() for c in rank.columns]

    if "SYMBOL" not in rank or "RANK" not in rank or "SCORE" not in rank:
        print("Ranking file missing required columns")
        return 1

    rank = rank.rename(columns={
        "SYMBOL": "symbol",
        "RANK": "rank_rank",
        "SCORE": "rank_score"
    })

    # -------------------------------------------------
    # Try ML (optional)
    # -------------------------------------------------
    use_ml = ML_FILE.exists()

    if use_ml:
        ml = pd.read_csv(ML_FILE)
        ml.columns = [c.upper().strip() for c in ml.columns]

        if "SYMBOL" in ml and "ML_SCORE" in ml:
            ml = ml.rename(columns={
                "SYMBOL": "symbol",
                "ML_SCORE": "ml_score"
            })
            ml = ml.sort_values("ml_score", ascending=False).head(TOP_ML)
        else:
            use_ml = False

    # -------------------------------------------------
    # Merge logic
    # -------------------------------------------------
    if use_ml:
        merged = ml.merge(
            rank[["symbol", "rank_rank", "rank_score", "TREND_BOOST"]],
            on="symbol",
            how="inner"
        )
    else:
        merged = rank.copy()
        merged["ml_score"] = 0.0

    merged = merged[merged["rank_rank"] <= TOP_RANK]

    if merged.empty:
        print("No symbols after filters")
        return 0   # ❗ pipeline-safe

    merged["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    final = merged.sort_values(
        ["ml_score", "rank_score"],
        ascending=False
    )[
        ["symbol", "ml_score", "rank_rank", "rank_score", "TREND_BOOST", "generated_at"]
    ]

    final.to_csv(OUT_FILE, index=False)

    print("Confluence trades generated")
    print(final.head(10))
    print("Saved to:", OUT_FILE)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print("Step 5 failed:", e)
        sys.exit(1)
