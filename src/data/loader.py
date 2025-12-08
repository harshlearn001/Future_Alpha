from __future__ import annotations
import pandas as pd
from src.config.paths import CLEANED_HIST_DIR


def load_regime_index() -> pd.DataFrame:
    """
    Build a synthetic equal-weight index from all continuous futures.
    Used as market regime proxy.
    """

    frames = []

    for path in CLEANED_HIST_DIR.glob("*_CONT.csv"):
        symbol = path.stem.replace("_CONT", "")
        df = pd.read_csv(path, parse_dates=["date"])
        df = df[["date", "adj_close"]].copy()
        df["SYMBOL"] = symbol
        frames.append(df)

    if not frames:
        raise FileNotFoundError("No continuous futures data found")

    all_df = pd.concat(frames, ignore_index=True)

    index_df = (
        all_df
        .groupby("date")["adj_close"]
        .mean()
        .reset_index()
        .rename(columns={"date": "DATE"})
    )

    print("[INFO] Regime index built from universe (equal-weight)")

    return index_df

# -------------------------------------------------
# Load latest cleaned daily FO (for run.py / live)
# -------------------------------------------------
from pathlib import Path
import pandas as pd

from src.config.paths import CLEANED_DAILY_DIR


def load_clean_daily() -> pd.DataFrame:
    """
    Load latest cleaned daily FO futures file.
    Used by run.py (LIVE PIPELINE).
    """

    files = sorted(
        CLEANED_DAILY_DIR.glob("daily_clean_*.csv"),
        reverse=True
    )

    if not files:
        raise FileNotFoundError(
            f"No daily_clean_*.csv found in {CLEANED_DAILY_DIR}"
        )

    latest = files[0]
    df = pd.read_csv(latest)

    # attach metadata (nice for logging/debug)
    df.attrs["source_path"] = latest

    return df

# -------------------------------------------------
# Load continuous futures history (per symbol)
# -------------------------------------------------
from src.config.paths import CLEANED_HIST_DIR


def load_symbol_history(symbol: str) -> pd.DataFrame:
    """
    Load cleaned continuous futures history for one symbol.
    Used by run.py + feature pipeline.
    """

    path = CLEANED_HIST_DIR / f"{symbol}_CONT.csv"

    if not path.exists():
        raise FileNotFoundError(path)

    df = pd.read_csv(path)

    # normalize columns
    df.columns = [c.lower() for c in df.columns]

    # REQUIRED minimal columns
    required = {"date", "adj_close"}
    if not required.issubset(df.columns):
        raise KeyError(
            f"{path.name} missing required columns {required}, found {df.columns}"
        )

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    return df

