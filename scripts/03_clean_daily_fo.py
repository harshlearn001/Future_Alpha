from __future__ import annotations

import sys
from pathlib import Path
import zipfile
import pandas as pd

# =====================================================
# CONFIG
# =====================================================
RAW_DIR = Path("data/raw/daily_raw")
OUT_DIR = Path("data/cleaned/cleaned_daily")

OUT_DIR.mkdir(parents=True, exist_ok=True)

# =====================================================
# MAIN LOGIC
# =====================================================
def main():
    print("STEP 2 | CLEAN DAILY FO DATA")
    print("-" * 60)

    zips = sorted(RAW_DIR.glob("fo*.zip"))

    if not zips:
        print("No FO zip files found. Nothing to clean.")
        return

    for zip_path in zips:
        date_tag = zip_path.stem.replace("fo", "")
        out_file = OUT_DIR / f"daily_clean_{date_tag}.csv"

        if out_file.exists():
            print(f"Already cleaned: {out_file.name}")
            continue

        print(f"Cleaning: {zip_path.name}")

        with zipfile.ZipFile(zip_path, "r") as z:
            csv_files = [n for n in z.namelist() if n.lower().endswith(".csv")]

            if not csv_files:
                print(f"No CSV inside {zip_path.name}, skipping")
                continue

            with z.open(csv_files[0]) as f:
                df = pd.read_csv(f)

        if df.empty:
            print(f"Empty data in {zip_path.name}, skipping")
            continue

        # -----------------------------
        # NORMALIZE COLUMNS
        # -----------------------------
        df.columns = [c.strip().upper() for c in df.columns]

        required = {"SYMBOL", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"}
        missing = required - set(df.columns)

        if missing:
            raise ValueError(f"Missing columns {missing} in {zip_path.name}")

        df = df[list(required)]

        # -----------------------------
        # SAVE
        # -----------------------------
        df.to_csv(out_file, index=False)
        print(f"Saved: {out_file.name}")

    print("Daily FO cleaning completed successfully")


# =====================================================
# ENTRY POINT (AUTOMATION SAFE)
# =====================================================
if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print("Cleaning failed:", e)
        sys.exit(1)
