# src/data/loader.py
from __future__ import annotations
from pathlib import Path
from typing import Optional
import pandas as pd

from src.config.paths import (
    CLEANED_DAILY_DIR,
    MASTER_SYMBOLS_DIR,
    CLEANED_HIST_DIR,
)
from src.utils.dates import infer_date_from_filename

def get_latest_clean_daily_file() -> Optional[Path]:
    files = sorted(CLEANED_DAILY_DIR.glob("daily_clean_*.csv"))
    if not files:
        return None
    # sort by inferred date
    files = sorted(files, key=lambda p: infer_date_from_filename(p) or pd.Timestamp.min)
    return files[-1]

def load_clean_daily(date_str: Optional[str] = None) -> pd.DataFrame:
    """
    date_str format: DD-MM-YYYY (bhavcopy date).
    If None -> use latest daily_clean_* file.
    """
    if date_str is None:
        path = get_latest_clean_daily_file()
        if path is None:
            raise FileNotFoundError("No daily_clean_*.csv found in cleaned_daily.")
    else:
        path = CLEANED_DAILY_DIR / f"daily_clean_{date_str.replace('-', '')}.csv"

    df = pd.read_csv(path)
    # ensure standard columns: SYMBOL, EXPIRY, OPEN, HIGH, LOW, CLOSE, SETTLE_PR, OPEN_INT, TRDQTY, TRDVAL, etc.
    return df

def load_symbol_history(symbol: str) -> pd.DataFrame:
    """
    Load continuous futures history for a symbol.
    Expected schema (your data):
      date, adj_open, adj_high, adj_low, adj_close, volume, oi, expiry
    """
    path = CLEANED_HIST_DIR / f"{symbol}_CONT.csv"
    if not path.exists():
        raise FileNotFoundError(path)

    df = pd.read_csv(path)

    # Normalize column names
    df.columns = [c.strip().upper() for c in df.columns]

    # ---- DATE ----
    if "DATE" not in df.columns:
        raise KeyError(f"DATE column missing in {path.name}")
    df["date"] = pd.to_datetime(df["DATE"])

    # ---- CLOSE PRICE (ADJUSTED) ----
    if "ADJ_CLOSE" not in df.columns:
        raise KeyError(f"ADJ_CLOSE column missing in {path.name}")
    df["close"] = df["ADJ_CLOSE"].astype(float)

    # ---- OPEN INTEREST ----
    if "OI" not in df.columns:
        raise KeyError(f"OI column missing in {path.name}")
    df["open_int"] = df["OI"].astype(float)

    df = df.sort_values("date").reset_index(drop=True)

    return df[["date", "close", "open_int"]]


def load_spot_history(symbol: str) -> pd.DataFrame:
    """
    Optional: from MASTER_SYMBOLS_DIR, spot series for reference.
    """
    path = MASTER_SYMBOLS_DIR / f"{symbol}.csv"
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_csv(path, parse_dates=["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df
