from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import List

from src.config.paths import META_DIR


# -----------------------------
# Helper: normalize columns
# -----------------------------
def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().upper() for c in df.columns]
    return df


# -----------------------------
# Helper: detect symbol column
# -----------------------------
def _detect_symbol_col(df: pd.DataFrame) -> str:
    for col in ["SYMBOL", "UNDERLYING", "SYMBOL_NAME", "NAME"]:
        if col in df.columns:
            return col
    raise KeyError(
        f"No SYMBOL-like column found in universe file. Columns: {list(df.columns)}"
    )


# -----------------------------
# Public API
# -----------------------------
def load_active_fo_universe() -> pd.DataFrame:
    """
    Loads NSE FO active symbols metadata.
    """
    path = META_DIR / "nse_fo_active_symbols.csv"
    if not path.exists():
        raise FileNotFoundError(f"Universe file missing: {path}")

    df = pd.read_csv(path)
    df = _normalize_cols(df)
    return df


def get_active_symbols_list() -> List[str]:
    df = load_active_fo_universe()
    sym_col = _detect_symbol_col(df)
    symbols = (
        df[sym_col]
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )
    return sorted(symbols)
