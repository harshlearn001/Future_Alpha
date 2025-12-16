from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

# =====================================================
# PATHS
# =====================================================
CLEAN_DIR  = Path("data/cleaned/cleaned_daily")
MASTER_DIR = Path("data/master/symbols")

MASTER_DIR.mkdir(parents=True, exist_ok=True)

# =====================================================
# MAIN LOGIC
# =====================================================
def main():
    print("STEP 3 | APPEND DAILY DATA TO MASTER")
    print("-" * 60)

    daily_files = sorted(CLEAN_DIR.glob("daily_clean_*.csv"))

    if not daily_files:
        print("No cleaned daily files found. Nothing to append.")
        return

    for file in daily_files:
        print(f"Processing: {file.name}")
        df = pd.read_csv(file)

        if df.empty:
            print(f"Empty file skipped: {file.name}")
            continue

        df.columns = [c.strip().upper() for c in df.columns]

        required = {"SYMBOL", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"}
        missing = required - set(df.columns)

        if missing:
            raise ValueError(f"Missing columns {missing} in {file.name}")

        for symbol, sdf in df.groupby("SYMBOL"):
            out_file = MASTER_DIR / f"{symbol}.csv"

            sdf = sdf.copy()
            sdf["DATE"] = file.stem.replace("daily_clean_", "")

            cols = ["DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
            sdf = sdf[cols]

            if out_file.exists():
                old = pd.read_csv(out_file)
                combined = pd.concat([old, sdf], ignore_index=True)
                combined.drop_duplicates(subset=["DATE"], inplace=True)
            else:
                combined = sdf

            combined.to_csv(out_file, index=False)

    print("Daily data appended to master successfully")


# =====================================================
# ENTRY POINT (AUTOMATION SAFE)
# =====================================================
if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print("Append failed:", e)
        sys.exit(1)
