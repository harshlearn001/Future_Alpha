# src/signals/ml_signals.py
from __future__ import annotations

import argparse
from datetime import datetime

import pandas as pd

from src.config.paths import CLEANED_HIST_DIR, PROCESSED_DIR
from src.data.loader import load_symbol_history
from src.features.momentum import add_momentum_features
from src.labels.forward_returns import build_forward_returns
from src.models.xgb_signal_model import XGBSignalModel


# -------------------------------------------------
# Load all continuous futures history
# -------------------------------------------------
def load_all_history() -> pd.DataFrame:
    dfs: list[pd.DataFrame] = []

    for csv_path in CLEANED_HIST_DIR.glob("*_CONT.csv"):
        symbol = csv_path.stem.replace("_CONT", "")
        df_sym = load_symbol_history(symbol)
        dfs.append(df_sym)

    df = pd.concat(dfs, ignore_index=True)

    # Normalize columns to match rest of project
    df = df.rename(
        columns={
            "date": "DATE",
            "symbol": "SYMBOL",
        }
    )

    df["DATE"] = pd.to_datetime(df["DATE"])
    df = df.sort_values(["SYMBOL", "DATE"]).reset_index(drop=True)

    return df


# -------------------------------------------------
# Build feature matrix + label series with MultiIndex
# -------------------------------------------------
def build_features_and_labels(df: pd.DataFrame):
    # -----------------------------
    # FEATURES
    # -----------------------------
    df = add_momentum_features(df)

    feature_cols = [c for c in df.columns if c.startswith("mom_")]
    if not feature_cols:
        raise RuntimeError("No momentum feature columns found (mom_*)")

    features = (
        df[["DATE", "SYMBOL"] + feature_cols]
        .dropna()
        .set_index(["DATE", "SYMBOL"])
        .sort_index()
    )

    # -----------------------------
    # LABELS (forward returns)
    # -----------------------------
    raw_labels = build_forward_returns(df, horizon=1)

    raw_labels = build_forward_returns(df, horizon=1)

# ✅ normalize labels ALWAYS from df
    if isinstance(raw_labels, pd.Series):
        values = raw_labels.values
    elif isinstance(raw_labels, pd.DataFrame):
        if "next_ret" not in raw_labels.columns:
        # DataFrame with single column
            values = raw_labels.iloc[:, 0].values
        else:
            values = raw_labels["next_ret"].values
    else:
        raise TypeError("build_forward_returns returned invalid type")

    lab_df = (
    df.loc[:, ["DATE", "SYMBOL"]]
    .assign(next_ret=values)
)

    labels = (
        lab_df
        .set_index(["DATE", "SYMBOL"])
        .reindex(features.index)
        ["next_ret"]
    )

    return features, labels



# -------------------------------------------------
# Train model up to a given date, score that date
# -------------------------------------------------
def train_and_score_for_date(
    as_of: pd.Timestamp,
    features: pd.DataFrame,
    labels: pd.Series,
    top_n: int = 200,
) -> pd.DataFrame:
    idx_dates = features.index.get_level_values("DATE")

    train_mask = idx_dates < as_of
    score_mask = idx_dates == as_of

    X_train = features.loc[train_mask]
    y_train = labels.loc[train_mask]

    X_score = features.loc[score_mask]

    if X_score.empty:
        raise RuntimeError(f"No feature rows for date {as_of.date()}")

    # Safety cleaning
    train_ok = y_train.notna() & X_train.notna().all(axis=1)
    X_train = X_train.loc[train_ok]
    y_train = y_train.loc[train_ok]

    score_ok = X_score.notna().all(axis=1)
    X_score = X_score.loc[score_ok]

    if len(y_train) == 0:
        raise RuntimeError(f"No valid training samples before {as_of.date()}")

    print(f"✅ Train samples: {len(y_train)}")
    print(f"✅ Score samples: {len(X_score)}")

    # Model
    model = XGBSignalModel()
    model.fit(X_train, y_train)

    scores = model.predict(X_score)

    df_scores = (
        pd.Series(scores, index=X_score.index, name="ml_score")
        .reset_index()
        .sort_values("ml_score", ascending=False)
        .reset_index(drop=True)
    )

    df_scores["RANK"] = df_scores.index + 1

    return df_scores


# -------------------------------------------------
# MAIN CLI
# -------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Build daily ML ranking using XGBoost."
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Target date in YYYY-MM-DD (default: last date in history)",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=200,
        help="How many symbols to keep in ranking (default: 200)",
    )

    args = parser.parse_args()

    print("🚀 ML Signals — building daily ranking")

    # 1) Load history
    df = load_all_history()
    last_date = df["DATE"].max().normalize()
    print(f"✅ History loaded: {df.shape}, last DATE = {last_date.date()}")

    if args.date:
        as_of = pd.to_datetime(args.date).normalize()
    else:
        as_of = last_date

    print(f"🎯 Scoring date: {as_of.date()}")

    # 2) Features & labels
    features, labels = build_features_and_labels(df)
    print(f"✅ Feature matrix: {features.shape}")
    print(f"✅ Labels length : {len(labels)}")

    # 3) Train + score
    df_scores = train_and_score_for_date(
        as_of=as_of,
        features=features,
        labels=labels,
        top_n=args.top_n,
    )

    # Keep top-N
    df_scores = df_scores.head(args.top_n)

    # 4) Save to processed folder
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    out_latest = PROCESSED_DIR / "daily_ranking_latest_ml.csv"
    out_dated = PROCESSED_DIR / f"daily_ranking_ml_{as_of.date()}.csv"

    df_scores.to_csv(out_latest, index=False)
    df_scores.to_csv(out_dated, index=False)

    print(f"💾 Saved latest ML ranking -> {out_latest}")
    print(f"💾 Saved dated ML ranking  -> {out_dated}")
    print("\n✅ ML daily ranking completed")


if __name__ == "__main__":
    main()
