# scripts/run_walkforward_ml.py
# -*- coding: utf-8 -*-

import pandas as pd

from src.backtest.walkforward_ml import main
from src.config.paths import CLEANED_HIST_DIR
from src.data.loader import load_symbol_history
from src.features.momentum import add_momentum_features
from src.labels.forward_returns import build_forward_returns


def load_all_history() -> pd.DataFrame:
    """Load all *_CONT.csv continuous futures and combine into one DataFrame."""
    dfs: list[pd.DataFrame] = []

    for csv_path in CLEANED_HIST_DIR.glob("*_CONT.csv"):
        symbol = csv_path.stem.replace("_CONT", "")
        df_sym = load_symbol_history(symbol)
        dfs.append(df_sym)

    df = pd.concat(dfs, ignore_index=True)

    # Normalize columns
    df = df.rename(columns={"date": "DATE", "symbol": "SYMBOL"})
    df["DATE"] = pd.to_datetime(df["DATE"])
    df = df.sort_values(["SYMBOL", "DATE"]).reset_index(drop=True)

    return df


def run():
    print("🚀 Starting ML walk-forward")

    # -----------------------------
    # LOAD DATA
    # -----------------------------
    df = load_all_history()
    print("✅ History loaded:", df.shape)

    # -----------------------------
    # FEATURES
    # -----------------------------
    df = add_momentum_features(df)
    print("✅ Momentum features added")

    feature_cols = [c for c in df.columns if c.startswith("mom_")]
    if not feature_cols:
        raise RuntimeError("No momentum feature columns found")

    # MultiIndex (DATE, SYMBOL) for features
    features = (
        df.loc[:, ["DATE", "SYMBOL"] + feature_cols]
          .dropna()
          .set_index(["DATE", "SYMBOL"])
          .sort_index()
    )

    print("✅ Feature matrix:", features.shape)
    print("✅ Feature index names:", features.index.names)

    # -----------------------------
    # LABELS (forward returns)
    # -----------------------------
    labels_series = build_forward_returns(df, horizon=1)

    # Put labels onto same MultiIndex (DATE, SYMBOL) and align to features
    labels = (
        df.loc[labels_series.index, ["DATE", "SYMBOL"]]
          .assign(next_ret=labels_series.values)
          .set_index(["DATE", "SYMBOL"])
          .reindex(features.index)["next_ret"]
    )

    print("✅ Labels built")
    print("✅ Labels index names:", labels.index.names)

    # -----------------------------
    # RETURNS FOR PnL
    # -----------------------------
    returns = (
        features
        .reset_index()[["DATE", "SYMBOL"]]
        .assign(next_ret=labels.values)
    )

    print("✅ Returns ready:", returns.shape)

    # -----------------------------
    # RUN WALK-FORWARD
    # -----------------------------
    main(
        features=features,
        labels=labels,
        returns=returns,
        top_n=5,
    )

    print("✅ ML walk-forward completed successfully")


if __name__ == "__main__":
    run()
