#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Future_Alpha | STEP 2
CLEAN DAILY FO BHAVCOPY (FUTURES ONLY)

✔ Handles NSE summary + real table
✔ Detects INSTRUMENT header
✔ Supports OPEN_INT*
✔ FUTIDX + FUTSTK only
✔ Production safe
"""

import zipfile
from io import StringIO
from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
ZIP_DIR = ROOT / "data" / "raw" / "daily_raw"
OUT_DIR = ROOT / "data" / "cleaned" / "cleaned_daily"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# REQUIRED LOGICAL COLUMNS
# --------------------------------------------------
REQUIRED = {"INSTRUMENT", "SYMBOL", "EXP_DATE"}

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def find_header_start(text: str):
    """Find line index where real bhavcopy table starts"""
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if line.strip().upper().startswith("INSTRUMENT"):
            return i
    return None


def clean_latest_zip():
    zips = sorted(ZIP_DIR.glob("fo*.zip"))
    if not zips:
        print(" No FO zip found")
        return

    zip_path = zips[-1]
    print(f" Cleaning daily FO bhavcopy → {zip_path.name}")

    date_str = zip_path.stem.replace("fo", "")

    df = None

    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            if not name.lower().endswith(".csv"):
                continue

            raw = z.read(name).decode("utf-8", errors="ignore")
            start = find_header_start(raw)

            if start is None:
                continue

            tmp = pd.read_csv(StringIO(raw), skiprows=start)

            # Normalize headers
            tmp.columns = (
                tmp.columns.astype(str)
                .str.upper()
                .str.strip()
                .str.replace("*", "", regex=False)  # OPEN_INT*
            )

            if REQUIRED.issubset(tmp.columns):
                print(f"Using futures table → {name}")
                df = tmp
                break

    if df is None:
        print("Instrument-level futures table not found (summary-only file)")
        print("Skipping FO clean for this date")
        return

    # --------------------------------------------------
    # FUTURES ONLY
    # --------------------------------------------------
    df["INSTRUMENT"] = df["INSTRUMENT"].astype(str).str.upper().str.strip()
    df = df[df["INSTRUMENT"].isin(["FUTIDX", "FUTSTK"])]

    if df.empty:
        print("Futures table found but no FUTIDX/FUTSTK rows")
        return

    # --------------------------------------------------
    # RENAME → STANDARD SCHEMA
    # --------------------------------------------------
    df = df.rename(columns={
        "SYMBOL": "symbol",
        "EXP_DATE": "expiry",
        "OPEN_PRICE": "open",
        "HI_PRICE": "high",
        "LO_PRICE": "low",
        "CLOSE_PRICE": "close",
        "TRD_QTY": "volume",
        "OPEN_INT": "oi",
    })

    # --------------------------------------------------
    # DATE HANDLING
    # --------------------------------------------------
    df["date"] = pd.to_datetime(date_str, format="%d%m%Y")
    df["expiry"] = pd.to_datetime(df["expiry"], dayfirst=True, errors="coerce")

    # --------------------------------------------------
    # FINAL COLUMNS
    # --------------------------------------------------
    df = df[
        ["symbol", "date", "open", "high", "low", "close", "volume", "oi", "expiry"]
    ]

    out = OUT_DIR / f"daily_clean_{date_str}.csv"
    df.to_csv(out, index=False)

    print(f" CLEAN DAILY SAVED → {out}")
    print(f" Rows: {len(df)}")


# --------------------------------------------------
# MAIN
# --------------------------------------------------
if __name__ == "__main__":
    clean_latest_zip()
