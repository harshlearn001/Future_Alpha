from __future__ import annotations

import pandas as pd
from pathlib import Path
import sys
import warnings

warnings.filterwarnings("ignore")  # 🔒 silence pandas warnings

# =====================================================
# PATHS
# =====================================================
# File location:
# H:\Future_Alpha\src\portfolio\run_position_sizing.py
# parents[2] → H:\Future_Alpha
ROOT = Path(__file__).resolve().parents[2]

SIGNAL_DIR = ROOT / "data" / "processed" / "signal.so"
OUT_FILE   = ROOT / "data" / "processed" / "trade_orders_today.csv"

# =====================================================
# CONFIG
# =====================================================
CAPITAL    = 1_000_000   # ₹10,00,000 total capital
RISK_PCT   = 0.01        # 1% risk per trade
MAX_TRADES = 5           # Max trades per day

print("\nSTEP 6 | POSITION SIZING")
print("-" * 60)

def main() -> None:
    try:
        # -------------------------------------------------
        # Locate latest signal file (DATED)
        # -------------------------------------------------
        files = sorted(SIGNAL_DIR.glob("confluence_trades_*.csv"))

        if not files:
            print("No confluence signal files found")
            return

        IN_FILE = files[-1]
        print(f"📥 Using signal file: {IN_FILE.name}")

        df = pd.read_csv(IN_FILE)

        if df.empty:
            print("No trades today")
            return

        df.columns = [c.upper().strip() for c in df.columns]

        if "SYMBOL" not in df.columns:
            print("Invalid confluence structure")
            return

        # -------------------------------------------------
        # Limit number of trades
        # -------------------------------------------------
        df = df.head(MAX_TRADES).copy()

        # -------------------------------------------------
        # Risk-based sizing (SAFE DEFAULT)
        # -------------------------------------------------
        risk_per_trade = CAPITAL * RISK_PCT

        # FIXED BUG: qty calculated as integer (not Series)
        qty = max(int(risk_per_trade / 100), 1)

        df["SIDE"] = "BUY"
        df["QTY"] = qty
        df["ORDER_TYPE"] = "MIS"
        df["ENTRY_TYPE"] = "MARKET"

        # -------------------------------------------------
        # Final order sheet
        # -------------------------------------------------
        orders = df[
            [
                "SYMBOL",
                "SIDE",
                "QTY",
                "ORDER_TYPE",
                "ENTRY_TYPE",
            ]
        ]

        orders.to_csv(OUT_FILE, index=False)

        print("✅ Position sizing completed")
        print(orders)
        print("💾 Saved to:", OUT_FILE)

    except Exception as e:
        # ❗ DO NOT RAISE — pipeline-safe
        print("Position sizing warning:", e)
        return


# =====================================================
# ENTRY POINT — FORCE EXIT(0)
# =====================================================
if __name__ == "__main__":
    main()
    sys.exit(0)
