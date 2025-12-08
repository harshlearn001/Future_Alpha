# src/config/paths.py
from __future__ import annotations
from pathlib import Path

# Project root (Future_Alpha)
BASE_DIR = Path(__file__).resolve().parents[2]

# Data folders
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
RAW_DAILY_FO_DIR = RAW_DIR / "daily_raw"          # fo_*.csv, fo*.zip
CLEANED_DIR = DATA_DIR / "cleaned"
CLEANED_DAILY_DIR = CLEANED_DIR / "cleaned_daily" # daily_clean_*.csv
CLEANED_HIST_DIR = CLEANED_DIR / "cleaned_historical"

MASTER_DIR = DATA_DIR / "master"
MASTER_SYMBOLS_DIR = MASTER_DIR / "symbols"       # one symbol per file
MASTER_CONT_DIR = CLEANED_HIST_DIR                # *_CONT.csv files

META_DIR = DATA_DIR / "meta"
PROCESSED_DIR = DATA_DIR / "processed"

REPORTS_DIR = BASE_DIR / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"

def ensure_dirs() -> None:
    """
    Create folders if they don't exist.
    Call this at the start of run.py / scripts.
    """
    for p in [
        DATA_DIR, RAW_DIR, RAW_DAILY_FO_DIR,
        CLEANED_DIR, CLEANED_DAILY_DIR, CLEANED_HIST_DIR,
        MASTER_DIR, MASTER_SYMBOLS_DIR,
        META_DIR, PROCESSED_DIR,
        REPORTS_DIR, FIGURES_DIR,
    ]:
        p.mkdir(parents=True, exist_ok=True)
