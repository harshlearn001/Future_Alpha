#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Future_Alpha | STEP 2
CLEAN DAILY FO BHAVCOPY (FUTURES ONLY)

- Handles NSE summary + real table
- Detects INSTRUMENT header
- Supports OPEN_INT*
- FUTIDX + FUTSTK only
- ONE output file (DDMMYYYY)
- PowerShell pipeline safe
"""

import sys
import zipfile
from io import StringIO
from pathlib import Path
import pandas as pd
import warnings

# --------------------------------------------------
# SILENCE ALL WARNINGS (CRITICAL FOR POWERSHELL)
# --------------------------------------------------
warnings.filterwarnings("ignore")

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
def safe_exit(msg: str) -> None:
    print(msg)
    sys.exit(0)


def find_header_start(text: str):
    for i, line in enumerate(text.splitlines()):
        if line.strip().upper().startswith("INSTRUMENT"):
            return i
    return None


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def clean_latest_zip():
    zips = sorted(ZIP_DIR.glob("fo*.zip"))
    if not zips:
        safe_exit("No FO zip found (market closed or download skipped)")

    zip_path = zips[-1]
    date_str = zip_path.stem.replace("fo", "")  # DDMMYYYY

    print("Cleaning daily FO bhavcopy:", zip_path.name)
    print("Trade date detected:", date_str)

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

            tmp.columns = (
                tmp.columns.astype(str)
                .str.upper()
                .str.strip()
                .str.replace("*", "", regex=False)
            )

            if REQUIRED.issubset(tmp.columns):
                print("Using futures table:", name)
                df = tmp
                break

    if df is None:
        safe_exit("Futures table not found (summary-only file)")

    # --------------------------------------------------
    # FUTURES ONLY
    # --------------------------------------------------
    df["INSTRUMENT"] = df["INSTRUMENT"].astype(str).str.upper().str.strip()
    df = df[df["INSTRUMENT"].isin(["FUTIDX", "FUTSTK"])]

    if df.empty:
        safe_exit("No FUTIDX/FUTSTK rows found")

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

    # --------------------------------------------------
    # SAVE (ONE FILE ONLY)
    # --------------------------------------------------
    out_file = OUT_DIR / f"daily_clean_{date_str}.csv"
    df.to_csv(out_file, index=False)

    print("CLEAN DAILY FO SAVED")
    print("Output file:", out_file)
    print("Rows:", len(df))
    print("Symbols:", df["symbol"].nunique())


# --------------------------------------------------
# ENTRY POINT (STRICT)
# --------------------------------------------------
if __name__ == "__main__":
    try:
        clean_latest_zip()
        sys.exit(0)
    except Exception as e:
        print("CLEAN DAILY FO FAILED:", e)
        sys.exit(1)
