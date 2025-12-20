from __future__ import annotations

import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

# =====================================================
# PATHS (PROJECT ROOT FIXED)
# =====================================================
ROOT = Path(__file__).resolve().parents[2]

RANK_FILE = ROOT / "data" / "processed" / "daily_ranking_latest.csv"
ML_FILE   = ROOT / "data" / "processed" / "daily_ranking_latest_ml.csv"

# ✅ NEW CLEAN OUTPUT LOCATION
OUT_DIR = ROOT / "data" / "signal" / "confluence"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TOP_RANK = 30
TOP_ML   = 30

print("\nSTEP 5 | GENERATE CONFLUENCE TRADES")
print("-" * 60)

def main() -> int:
    # -------------------------------------------------
    # Load ranking (mandatory)
    # -------------------------------------------------
    if not RANK_FILE.exists():
        print(" Ranking file not found:", RANK_FILE)
        return 1

    rank = pd.read_csv(RANK_FILE)

    if rank.empty:
        print(" Ranking file is empty")
        return 1

    rank.columns = [c.upper().strip() for c in rank.columns]

    required = {"SYMBOL", "RANK", "SCORE"}
    if not required.issubset(rank.columns):
        print(" Ranking file missing required columns:", rank.columns.tolist())
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

        if {"SYMBOL", "ML_SCORE"}.issubset(ml.columns):
            ml = ml.rename(columns={
                "SYMBOL": "symbol",
                "ML_SCORE": "ml_score"
            })
            ml = ml.sort_values("ml_score", ascending=False).head(TOP_ML)
        else:
            print("⚠ML file missing required columns — skipping ML")
            use_ml = False

    # -------------------------------------------------
    # Merge logic
    # -------------------------------------------------
    if use_ml:
        merged = ml.merge(
            rank[["symbol", "rank_rank", "rank_score"]],
            on="symbol",
            how="inner"
        )
    else:
        merged = rank.copy()
        merged["ml_score"] = 0.0

    merged = merged[merged["rank_rank"] <= TOP_RANK]

    if merged.empty:
        print(" No symbols after filters")
        return 0

    # -------------------------------------------------
    # DATE HANDLING
    # -------------------------------------------------
    trade_date_ddmmyyyy = datetime.now().strftime("%d%m%Y")
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    merged["trade_date_ddmmyyyy"] = trade_date_ddmmyyyy
    merged["generated_at"] = generated_at

    # -------------------------------------------------
    # Final ordering
    # -------------------------------------------------
    final = merged.sort_values(
        ["ml_score", "rank_score"],
        ascending=False
    )[[
        "trade_date_ddmmyyyy",
        "symbol",
        "ml_score",
        "rank_rank",
        "rank_score",
        "generated_at",
    ]]

    # -------------------------------------------------
    # OUTPUT (AUTO-MOVED PIPELINE)
    # -------------------------------------------------
    OUT_FILE = OUT_DIR / f"confluence_trades_{trade_date_ddmmyyyy}.csv"
    final.to_csv(OUT_FILE, index=False)

    print(" Confluence trades generated")
    print(final.head(10))
    print(" Saved to:", OUT_FILE)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(" Step 5 failed:", e)
        sys.exit(1)
