#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Future_Alpha | STEP 6
POSITION SIZING (BROKER READY)

- Uses confluence filename date (DDMMYYYY)
- No system date dependency
- Pipeline safe
"""

import sys
from pathlib import Path
import pandas as pd
import warnings

warnings.filterwarnings("ignore")

# =====================================================
# PATHS
# =====================================================
ROOT = Path(__file__).resolve().parents[2]
SIGNAL_DIR = ROOT / "data" / "signal" / "confluence"
ORDER_DIR = ROOT / "data" / "signal" / "orders"
ORDER_DIR.mkdir(parents=True, exist_ok=True)

# =====================================================
# CONFIG
# =====================================================
CAPITAL    = 1_000_000    # 10L
RISK_PCT   = 0.01         # 1% per trade
MAX_TRADES = 5

print("\nSTEP 6 | POSITION SIZING")
print("-" * 60)


def main() -> None:
    try:
        files = sorted(SIGNAL_DIR.glob("confluence_trades_*.csv"))

        if not files:
            print("No confluence signal files found")
            sys.exit(0)

        in_file = files[-1]
        print("Using signal file:", in_file.name)

        # ----------------------------------------------
        # EXTRACT DATE FROM FILENAME (STRICT)
        # ----------------------------------------------
        # confluence_trades_04012026.csv → 04012026
        trade_date = in_file.stem.split("_")[-1]

        if not trade_date.isdigit() or len(trade_date) != 8:
            print("Invalid date in confluence filename")
            sys.exit(0)

        df = pd.read_csv(in_file)

        if df.empty:
            print("No trades today")
            sys.exit(0)

        df.columns = [c.upper().strip() for c in df.columns]

        if "SYMBOL" not in df.columns:
            print("Invalid confluence file structure")
            sys.exit(0)

        # ----------------------------------------------
        # LIMIT TRADES
        # ----------------------------------------------
        df = df.head(MAX_TRADES).copy()

        # ----------------------------------------------
        # RISK BASED QTY (SAFE DEFAULT)
        # ----------------------------------------------
        risk_per_trade = CAPITAL * RISK_PCT
        qty = max(int(risk_per_trade / 100), 1)

        df["SIDE"] = "BUY"
        df["QTY"] = qty
        df["ORDER_TYPE"] = "MIS"
        df["ENTRY_TYPE"] = "MARKET"

        orders = df[
            ["SYMBOL", "SIDE", "QTY", "ORDER_TYPE", "ENTRY_TYPE"]
        ].copy()

        # ----------------------------------------------
        # OUTPUT FILES (DDMMYYYY ONLY)
        # ----------------------------------------------
        dated_file = ORDER_DIR / f"trade_orders_{trade_date}.csv"
        today_file = ORDER_DIR / "trade_orders_today.csv"

        orders.to_csv(dated_file, index=False)
        orders.to_csv(today_file, index=False)

        print("Position sizing completed")
        print("Saved dated :", dated_file)
        print("Saved today:", today_file)

        sys.exit(0)

    except Exception as e:
        print("Position sizing warning:", e)
        sys.exit(0)


# =====================================================
# ENTRY POINT
# =====================================================
if __name__ == "__main__":
    main()
