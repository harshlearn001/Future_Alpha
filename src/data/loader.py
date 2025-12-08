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
