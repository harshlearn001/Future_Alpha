#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Future_Alpha | STEP 3
APPEND cleaned_daily -> data/master/symbols

Processes ALL daily_clean_*.csv (date ordered)
Per-symbol append
Strict duplicate protection (date + expiry)
Idempotent & production safe
"""

from pathlib import Path
import pandas as pd
import re

# ==================================================
# PATHS
# ==================================================
ROOT = Path(__file__).resolve().parents[1]

CLEAN_DAILY_DIR = ROOT / "data" / "cleaned" / "cleaned_daily"
SYMBOLS_DIR = ROOT / "data" / "master" / "symbols"
SYMBOLS_DIR.mkdir(parents=True, exist_ok=True)

BASE_COLS = ["date", "open", "high", "low", "close", "volume", "oi", "expiry"]

# ==================================================
# HELPERS
# ==================================================
def extract_date_from_name(path: Path):
    m = re.search(r"daily_clean_(\d{2})(\d{2})(\d{4})\.csv", path.name)
    if not m:
        return None
    dd, mm, yyyy = m.groups()
    return pd.to_datetime(f"{yyyy}-{mm}-{dd}")


def get_daily_files():
    files = list(CLEAN_DAILY_DIR.glob("daily_clean_*.csv"))
    if not files:
        print("No cleaned daily files found")
        return []

    dated = [(extract_date_from_name(f), f) for f in files]
    dated.sort(key=lambda x: (x[0] if x[0] is not None else pd.Timestamp.min))
    return [f for _, f in dated]


def normalize_old_master(old: pd.DataFrame) -> pd.DataFrame:
    old.columns = [c.lower().strip() for c in old.columns]
    old = old.loc[:, ~old.columns.duplicated()]

    for col in BASE_COLS:
        if col not in old.columns:
            old[col] = pd.NA

    old["date"] = pd.to_datetime(old["date"], errors="coerce")
    old["expiry"] = pd.to_datetime(old["expiry"], errors="coerce")

    return old[BASE_COLS]


# ==================================================
# MAIN
# ==================================================
def main():
    print("\nSTEP 3 | APPENDING cleaned_daily TO master/symbols")
    print("-" * 60)

    daily_files = get_daily_files()
    if not daily_files:
        return

    print(f"Daily files found: {len(daily_files)}\n")

    total_symbols = 0

    for daily_file in daily_files:
        print(f"Using daily file: {daily_file.name}")

        df = pd.read_csv(daily_file)
        if df.empty:
            print("  Skipped empty file")
            continue

        df.columns = [c.lower().strip() for c in df.columns]
        df["date"] = pd.to_datetime(df["date"])
        df["expiry"] = pd.to_datetime(df["expiry"])
        df["symbol"] = df["symbol"].astype(str).str.strip()

        symbols = sorted(df["symbol"].unique())
        print(f"Symbols in file: {len(symbols)}")

        for sym in symbols:
            sym_df = df[df["symbol"] == sym][BASE_COLS].copy()
            out_file = SYMBOLS_DIR / f"{sym}.csv"

            if out_file.exists():
                old = pd.read_csv(out_file)
                old = normalize_old_master(old)
                combined = pd.concat([old, sym_df], ignore_index=True)
                combined = combined.drop_duplicates(
                    subset=["date", "expiry"],
                    keep="last",
                )
            else:
                combined = sym_df

            combined = combined.sort_values(["date", "expiry"]).reset_index(drop=True)
            combined.to_csv(out_file, index=False)

            total_symbols += 1
            print(f"  {sym:<12} rows: {len(combined)}")

        print("")

    print("SYMBOL MASTER UPDATE COMPLETE")
    print(f"Symbols updated: {total_symbols}")


if __name__ == "__main__":
    main()
