from __future__ import annotations

import pandas as pd
from pathlib import Path
import sys

# =====================================================
# PATHS
# =====================================================
ROOT = Path(__file__).resolve().parents[2]

MASTER_DIR = ROOT / "data" / "master" / "symbols"
OUT_DIR    = ROOT / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_LATEST = OUT_DIR / "daily_ranking_latest.csv"
OUT_HIST   = OUT_DIR / "daily_ranking_history.csv"

print("\nSTEP 4 | BUILD FULL DAILY RANKINGS")
print("-" * 60)

# =====================================================
# HELPERS
# =====================================================
def detect_column(cols, candidates):
    for c in candidates:
        if c in cols:
            return c
    return None


def main() -> int:
    rows = []
    skipped = []

    # =====================================================
    # LOAD MASTER SYMBOL FILES
    # =====================================================
    for file in MASTER_DIR.glob("*.csv"):
        symbol = file.stem.upper().strip()

        try:
            df = pd.read_csv(file)
        except Exception:
            skipped.append((symbol, "read_error"))
            continue

        if df.empty:
            skipped.append((symbol, "empty_file"))
            continue

        # ---- normalize columns
        df.columns = [c.upper().strip() for c in df.columns]

        # 🔥 HARD FIX: DROP DUPLICATE COLUMNS (CRITICAL)
        df = df.loc[:, ~df.columns.duplicated()]

        date_col = detect_column(df.columns, ["DATE", "TRADE_DATE", "TIMESTAMP"])
        close_col = detect_column(df.columns, ["CLOSE", "CLOSE_PRICE", "CLOSE_FUT"])

        if not date_col or not close_col:
            skipped.append((symbol, "missing_date_or_close"))
            continue

        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col, close_col])

        if len(df) < 6:
            skipped.append((symbol, "insufficient_history"))
            continue

        df = df.sort_values(date_col)

        try:
            latest = df.iloc[-1]
            ret_1d = df[close_col].pct_change().iloc[-1]
            ret_5d = df[close_col].pct_change(5).iloc[-1]
            vol_10 = df[close_col].pct_change().rolling(10).std().iloc[-1]
        except Exception:
            skipped.append((symbol, "calc_error"))
            continue

        rows.append({
            "DATE": latest[date_col],
            "SYMBOL": symbol,
            "CLOSE": float(latest[close_col]),
            "RET_1D": ret_1d,
            "RET_5D": ret_5d,
            "VOL_10D": vol_10,
        })

    # =====================================================
    # VALIDATION
    # =====================================================
    if not rows:
        print(" No symbol data loaded")
        return 1

    ranked = pd.DataFrame(rows)

    # SECOND SAFETY NET (ABSOLUTE)
    ranked = ranked.loc[:, ~ranked.columns.duplicated()]

    ranked["DATE"] = pd.to_datetime(ranked["DATE"], errors="coerce")

    # Deduplicate symbols (latest only)
    ranked = (
        ranked.sort_values("DATE")
              .drop_duplicates(subset=["SYMBOL"], keep="last")
              .reset_index(drop=True)
    )

    ranked = ranked.dropna(subset=["RET_1D", "RET_5D"])

    # =====================================================
    # SCORING
    # =====================================================
    ranked["RANK_RET_5D"] = ranked["RET_5D"].rank(pct=True)
    ranked["RANK_RET_1D"] = ranked["RET_1D"].rank(pct=True)

    ranked["SCORE"] = ranked["RANK_RET_5D"] + ranked["RANK_RET_1D"]
    ranked["TREND_BOOST"] = (ranked["RET_5D"] > 0).astype(int)

    ranked = ranked.sort_values("SCORE", ascending=False).reset_index(drop=True)
    ranked["RANK"] = ranked.index + 1

    # =====================================================
    # SAVE
    # =====================================================
    ranked.to_csv(OUT_LATEST, index=False)

    if OUT_HIST.exists():
        hist = pd.read_csv(OUT_HIST)
        hist = hist.loc[:, ~hist.columns.duplicated()]
        hist = pd.concat([hist, ranked], ignore_index=True)
    else:
        hist = ranked.copy()

    hist.to_csv(OUT_HIST, index=False)

    # =====================================================
    # SUMMARY
    # =====================================================
    print("Daily ranking built successfully")
    print(f" Symbols ranked : {len(ranked)}")
    print(f" Latest saved   : {OUT_LATEST}")
    print(f" History saved  : {OUT_HIST}")

    print("\nTop 10 Preview:")
    print(ranked[["SYMBOL", "RANK", "SCORE"]].head(10))

    if skipped:
        print(f"\nSkipped symbols: {len(skipped)}")
        print(pd.DataFrame(skipped, columns=["SYMBOL", "REASON"]).head(10))

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"Ranking failed: {e}")
        sys.exit(1)
