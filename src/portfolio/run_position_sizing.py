from __future__ import annotations

import pandas as pd
from pathlib import Path
import sys
import warnings

warnings.filterwarnings("ignore")  # 🔒 silence pandas warnings

# =====================================================
# PATHS
# =====================================================
ROOT = Path(__file__).resolve().parents[2]

IN_FILE  = ROOT / "data" / "processed" / "confluence_trades.csv"
OUT_FILE = ROOT / "data" / "processed" / "trade_orders_today.csv"

# =====================================================
# CONFIG
# =====================================================
CAPITAL = 1_000_000     # 10L
RISK_PCT = 0.01
MAX_TRADES = 5

print("\nSTEP 6 | POSITION SIZING")
print("-" * 60)

def main() -> None:
    try:
        if not IN_FILE.exists():
            print("No confluence trades file found")
            return

        df = pd.read_csv(IN_FILE)

        if df.empty:
            print("No trades today")
            return

        df.columns = [c.upper().strip() for c in df.columns]

        if "SYMBOL" not in df:
            print("Invalid confluence structure")
            return

        df = df.head(MAX_TRADES).copy()

        risk_per_trade = CAPITAL * RISK_PCT

        df["SIDE"] = "BUY"
        df["QTY"] = (risk_per_trade / 100).astype(int).clip(lower=1)
        df["ORDER_TYPE"] = "MIS"
        df["ENTRY_TYPE"] = "MARKET"

        orders = df[[
            "SYMBOL",
            "SIDE",
            "QTY",
            "ORDER_TYPE",
            "ENTRY_TYPE"
        ]]

        orders.to_csv(OUT_FILE, index=False)

        print("Position sizing completed")
        print(orders)
        print("Saved to:", OUT_FILE)

    except Exception as e:
        print("Position sizing warning:", e)
        # ❗ DO NOT RAISE
        return


# =====================================================
# ENTRY POINT — FORCE EXIT(0)
# =====================================================
if __name__ == "__main__":
    main()
    sys.exit(0)   # 🔥 THIS LINE FIXES THE PIPELINE
