from __future__ import annotations

import pandas as pd
from pathlib import Path
import sys
import warnings
from datetime import datetime

warnings.filterwarnings("ignore")  # silence pandas warnings

# =====================================================
# PATHS
# =====================================================
# File location:
# H:\Future_Alpha\src\portfolio\run_position_sizing.py
# parents[2] → H:\Future_Alpha
ROOT = Path(__file__).resolve().parents[2]

# 🔹 INPUT: final signals only
SIGNAL_DIR = ROOT / "data" / "signal" / "confluence"

# 🔹 OUTPUT: broker-ready orders
ORDER_DIR = ROOT / "data" / "signal" / "orders"
ORDER_DIR.mkdir(parents=True, exist_ok=True)

# =====================================================
# CONFIG
# =====================================================
CAPITAL    = 1_000_000    # ₹10,00,000 total capital
RISK_PCT   = 0.01         # 1% risk per trade
MAX_TRADES = 5            # Max trades per day

print("\nSTEP 6 | POSITION SIZING")
print("-" * 60)

def main() -> None:
    try:
        # -------------------------------------------------
        # Locate latest confluence signal file
        # -------------------------------------------------
        files = sorted(SIGNAL_DIR.glob("confluence_trades_*.csv"))

        if not files:
            print(" No confluence signal files found")
            return

        IN_FILE = files[-1]
        print(f" Using signal file: {IN_FILE.name}")

        df = pd.read_csv(IN_FILE)

        if df.empty:
            print(" No trades today")
            return

        df.columns = [c.upper().strip() for c in df.columns]

        if "SYMBOL" not in df.columns:
            print(" Invalid confluence file structure")
            return

        # -------------------------------------------------
        # Extract trade date (DDMMYYYY)
        # -------------------------------------------------
        if "TRADE_DATE_DDMMYYYY" in df.columns:
            trade_date = str(df["TRADE_DATE_DDMMYYYY"].iloc[0])
        else:
            trade_date = datetime.now().strftime("%d%m%Y")

        # -------------------------------------------------
        # Limit number of trades
        # -------------------------------------------------
        df = df.head(MAX_TRADES).copy()

        # -------------------------------------------------
        # Risk-based sizing (safe default)
        # -------------------------------------------------
        risk_per_trade = CAPITAL * RISK_PCT
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
        ].copy()

        # -------------------------------------------------
        # OUTPUT FILES
        # -------------------------------------------------
        dated_file = ORDER_DIR / f"trade_orders_{trade_date}.csv"
        today_file = ORDER_DIR / "trade_orders_today.csv"

        orders.to_csv(dated_file, index=False)
        orders.to_csv(today_file, index=False)

        print(" Position sizing completed")
        print(orders)
        print(f" Saved dated  : {dated_file}")
        print(f" Saved today : {today_file}")

    except Exception as e:
        #  Pipeline-safe: never crash daily run
        print("Position sizing warning:", e)
        return


# =====================================================
# ENTRY POINT — FORCE CLEAN EXIT
# =====================================================
if __name__ == "__main__":
    main()
    sys.exit(0)
