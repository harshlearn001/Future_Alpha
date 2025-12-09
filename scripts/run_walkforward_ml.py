# -*- coding: utf-8 -*-

import pandas as pd

from src.backtest.walkforward_ml import main
from src.config.paths import CLEANED_HIST_DIR
from src.data.loader import load_symbol_history
from src.features.momentum import add_momentum_features
from src.labels.forward_returns import build_forward_returns


def load_all_history() -> pd.DataFrame:
    dfs = []

    for csv_path in CLEANED_HIST_DIR.glob("*_CONT.csv"):
        symbol = csv_path.stem.replace("_CONT", "")
        df_sym = load_symbol_history(symbol)
        dfs.append(df_sym)

    df = pd.concat(dfs, ignore_index=True)
    df = df.rename(columns={"date": "DATE", "symbol": "SYMBOL"})
    df["DATE"] = pd.to_datetime(df["DATE"])
    df = df.sort_values(["SYMBOL", "DATE"]).reset_index(drop=True)
    return df


def run():
    print("?? Starting ML walk-forward")

    # -----------------------------
    # LOAD DATA
    # -----------------------------
    df = load_all_history()
    print("? History loaded:", df.shape)

    # -----------------------------
    # FEATURES
    # -----------------------------
    df = add_momentum_features(df)
    print("? Momentum features added")

    feature_cols = [c for c in df.columns if c.startswith("mom_")]
    features = (
        df[["DATE", "SYMBOL"] + feature_cols]
        .dropna()
        .set_index(["DATE", "SYMBOL"])
        .sort_index()
    )

    print("? Feature matrix:", features.shape)
    print("? Feature index names:", features.index.names)

    # -----------------------------
    # LABELS (forward returns)
    # -----------------------------
    next_ret = build_forward_returns(df, horizon=1)

    labels = (
        df.loc[next_ret.index, ["DATE", "SYMBOL"]]
        .assign(next_ret=next_ret.values)
        .set_index(["DATE", "SYMBOL"])
        .reindex(features.index)
        ["next_ret"]
    )

    print("? Labels built")
    print("? Labels index names:", labels.index.names)

    # -----------------------------
    # RETURNS (PnL)
    # -----------------------------
    returns = (
        features
        .reset_index()[["DATE", "SYMBOL"]]
        .assign(next_ret=labels.values)
    )

    print("? Returns ready:", returns.shape)

    # -----------------------------
    # WALK-FORWARD ML
    # -----------------------------
    main(
        features=features,
        labels=labels,
        returns=returns,
        top_n=5,
    )

    print("? ML walk-forward completed successfully")


if __name__ == "__main__":
    run()
