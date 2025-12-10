#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Future_Alpha | STEP 5
APPEND cleaned_daily → data/master/symbols

✅ Processes ALL daily_clean_*.csv (in date order)
✅ Per-symbol append with strict duplicate protection
✅ Safe to run multiple times (idempotent)
"""

from pathlib import Path
import pandas as pd
import re

# ---------------- PATHS ----------------
ROOT = Path(__file__).resolve().parents[1]

CLEAN_DAILY_DIR = ROOT / "data" / "cleaned" / "cleaned_daily"
SYMBOLS_DIR = ROOT / "data" / "master" / "symbols"
SYMBOLS_DIR.mkdir(parents=True, exist_ok=True)


# ---------------- HELPERS ----------------
def extract_date_from_name(path: Path):
    """
    Expecting file names like: daily_clean_08122025.csv  (DDMMYYYY)
    """
    m = re.search(r"daily_clean_(\d{2})(\d{2})(\d{4})\.csv", path.name)
    if not m:
        return None
    dd, mm, yyyy = m.groups()
    return pd.to_datetime(f"{yyyy}-{mm}-{dd}")


def get_daily_files():
    files = sorted(CLEAN_DAILY_DIR.glob("daily_clean_*.csv"))
    if not files:
        print("❌ No cleaned daily files found in:", CLEAN_DAILY_DIR)
        return []
    # sort by date extracted from filename (fallback to name if parse fails)
    files_with_dates = []
    for f in files:
        d = extract_date_from_name(f)
        files_with_dates.append((d, f))
    files_with_dates.sort(key=lambda x: (x[0] if x[0] is not None else pd.Timestamp.min, x[1].name))
    return [f for _, f in files_with_dates]


# ---------------- MAIN ----------------
def main():
    print("\n📥 STEP 5 | APPENDING cleaned_daily → master/symbols")
    print("-" * 70)

    daily_files = get_daily_files()
    if not daily_files:
        return

    print(f"📄 Found {len(daily_files)} daily_clean files\n")

    total_updates = 0

    for daily_file in daily_files:
        print(f"🔹 Using daily file → {daily_file.name}")
        df = pd.read_csv(daily_file)

        if df.empty:
            print("   ⚠️ Skipped (empty file)")
            continue

        # normalize columns
        df.columns = [c.lower().strip() for c in df.columns]

        # enforce dtypes
        df["date"] = pd.to_datetime(df["date"])
        df["expiry"] = pd.to_datetime(df["expiry"])
        df["symbol"] = df["symbol"].astype(str).str.strip()

        base_cols = ["date", "open", "high", "low", "close", "volume", "oi", "expiry"]

        symbols = sorted(df["symbol"].unique())
        print(f"   📊 Symbols in this file: {len(symbols)}")

        for sym in symbols:
            sym_df = df[df["symbol"] == sym][base_cols].copy()

            out_file = SYMBOLS_DIR / f"{sym}.csv"

            if out_file.exists():
                old = pd.read_csv(out_file)

                # normalize old file
                old.columns = [c.lower().strip() for c in old.columns]

                # ensure required columns exist in old
                for col in base_cols:
                    if col not in old.columns:
                        if col in ("date", "expiry"):
                            old[col] = pd.NaT
                        else:
                            old[col] = pd.NA

                old["date"] = pd.to_datetime(old["date"], errors="coerce")
                old["expiry"] = pd.to_datetime(old["expiry"], errors="coerce")

                # only keep rows from daily that are strictly newer than last date in master
                           
                combined = pd.concat([old, sym_df], ignore_index=True)
                # strict duplicate protection ⇒ per (date, expiry)
                combined = combined.drop_duplicates(
                    subset=["date", "expiry"],
                    keep="last",
                )
            else:
                combined = sym_df

            combined = combined.sort_values(["date", "expiry"]).reset_index(drop=True)
            combined.to_csv(out_file, index=False)

            total_updates += 1
            print(f"   ✅ {sym:<12} → rows: {len(combined)}")

        print("")  # blank line between daily files

    print("🏁 SYMBOL MASTER UPDATE COMPLETE")
    print(f"🧾 Symbols touched: {total_updates}")


if __name__ == "__main__":
    main()
