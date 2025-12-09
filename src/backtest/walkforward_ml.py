# src/backtest/walkforward_ml.py
from __future__ import annotations

import pandas as pd

from src.config.paths import REPORTS_DIR
from src.backtest.engine import compute_metrics
from src.models.xgb_signal_model import XGBSignalModel


def build_pnl_from_scores(
    df: pd.DataFrame,
    top_n: int,
) -> pd.DataFrame:
    """
    Build daily PnL using ML scores -> ranks.
    Assumes columns: DATE, SYMBOL, ml_score, next_ret
    """
    results: list[tuple[pd.Timestamp, float]] = []

    for date, day_df in df.groupby("DATE"):
        picks = (
            day_df
            .sort_values("ml_score", ascending=False)
            .head(top_n)
        )

        rets = picks["next_ret"].dropna()
        if len(rets) == 0:
            continue

        results.append((date, rets.mean()))

    pnl = pd.DataFrame(results, columns=["DATE", "portfolio_return"])
    pnl = pnl.sort_values("DATE").reset_index(drop=True)
    pnl["equity"] = (1.0 + pnl["portfolio_return"]).cumprod()
    return pnl


def main(
    features: pd.DataFrame,
    labels: pd.Series,
    returns: pd.DataFrame,
    top_n: int = 5,
) -> None:
    """
    ML walk-forward by calendar year.

    Index of `features` and `labels` must be MultiIndex (DATE, SYMBOL).
    `returns` must have columns: DATE, SYMBOL, next_ret.
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    years = sorted(features.index.get_level_values("DATE").year.unique())
    summary: list[dict] = []

    for year in years:
        if year < 2022:
            continue

        print(f"\n🚀 ML WALK-FORWARD — {year}")

        year_index = features.index.get_level_values("DATE")
        train_mask = year_index < year
        test_mask = year_index == year

        if train_mask.sum() == 0 or test_mask.sum() == 0:
            print(f"⚠️ Skipping year {year} — no train/test data")
            continue

        # -----------------------------
        # TRAIN / TEST SPLIT
        # -----------------------------
        X_train = features.loc[train_mask]
        y_train = labels.loc[train_mask]

        X_test = features.loc[test_mask]
        y_test = labels.loc[test_mask]

        # -----------------------------
        # SAFETY CLEAN
        # -----------------------------
        train_ok = y_train.notna() & X_train.notna().all(axis=1)
        test_ok = y_test.notna() & X_test.notna().all(axis=1)

        X_train = X_train.loc[train_ok]
        y_train = y_train.loc[train_ok]

        X_test = X_test.loc[test_ok]
        y_test = y_test.loc[test_ok]

        if len(y_train) == 0 or len(y_test) == 0:
            print(f"⚠️ Skipping year {year} — no valid samples")
            continue

        print(f"✅ Train samples: {len(y_train)}")
        print(f"✅ Test samples : {len(y_test)}")

        # -----------------------------
        # MODEL
        # -----------------------------
        model = XGBSignalModel()
        model.fit(X_train, y_train)

        scores = model.predict(X_test)

        df_scores = (
            scores
            .rename("ml_score")
            .reset_index()
        )

        df_scores = df_scores.merge(
            returns,
            on=["DATE", "SYMBOL"],
            how="left",
        )

        pnl = build_pnl_from_scores(df_scores, top_n=top_n)
        if pnl.empty:
            print(f"⚠️ No PnL rows for {year}")
            continue

        stats = compute_metrics(pnl["portfolio_return"])

        print(f"CAGR   : {stats['CAGR']:.3f}")
        print(f"Sharpe : {stats['Sharpe']:.3f}")
        print(f"Max DD : {stats['Max_Drawdown']:.3f}")

        pnl.to_csv(
            REPORTS_DIR / f"walkforward_ml_{year}.csv",
            index=False,
        )

        summary.append(
            {
                "YEAR": year,
                "CAGR": stats["CAGR"],
                "Sharpe": stats["Sharpe"],
                "Max_Drawdown": stats["Max_Drawdown"],
            }
        )

    if summary:
        pd.DataFrame(summary).to_csv(
            REPORTS_DIR / "walkforward_ml_summary.csv",
            index=False,
        )
        print("✅ ML walk-forward summary saved.")
