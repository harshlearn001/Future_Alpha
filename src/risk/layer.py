# src/risk/layer.py
from __future__ import annotations

import numpy as np
import pandas as pd

from src.risk.config import RiskConfig


def _compute_volatility(
    df_hist: pd.DataFrame,
    lookback: int,
) -> pd.Series:
    """
    df_hist: history with columns [DATE, SYMBOL, adj_close]
    returns: Series indexed by SYMBOL with recent daily volatility (pct).
    """
    df_hist = df_hist.sort_values(["SYMBOL", "DATE"]).copy()

    df_hist["ret"] = (
        df_hist.groupby("SYMBOL")["adj_close"]
        .pct_change()
    )

    # Take last N returns per symbol
    vol = (
        df_hist
        .groupby("SYMBOL")["ret"]
        .tail(lookback)
        .groupby(df_hist["SYMBOL"])
        .std()
    )

    vol = vol.fillna(0.0)
    vol.name = "daily_vol"
    return vol


def apply_risk_layer(
    ranking_df: pd.DataFrame,
    hist_df: pd.DataFrame,
    cfg: RiskConfig,
) -> pd.DataFrame:
    """
    Inputs:
      ranking_df: output of ML ranking for a single DATE, e.g.
        SYMBOL, ml_score, RANK
      hist_df: full history (DATE, SYMBOL, adj_close, volume, ...)
               same as used in ML
      cfg: RiskConfig

    Returns a DataFrame with position plan:
        SYMBOL, side, qty, price, notional, risk_perc
    """

    # -------------------------------------------------
    # 1) Get latest snapshot for that scoring date
    # -------------------------------------------------
    scoring_date = ranking_df["DATE"].max()
    snap = (
        hist_df.loc[hist_df["DATE"] == scoring_date]
        .copy()
    )

    if snap.empty:
        raise RuntimeError(f"No history for scoring date {scoring_date}")

    # Keep only symbols that appear in ranking
    snap = snap.merge(
        ranking_df[["SYMBOL", "ml_score", "RANK"]],
        on="SYMBOL",
        how="inner",
    )

    # -------------------------------------------------
    # 2) Basic liquidity / price filters
    # -------------------------------------------------
    # compute avg volume over last N days
    vol_window = cfg.vol_lookback
    df_hist_last = (
        hist_df[hist_df["DATE"] <= scoring_date]
        .sort_values(["SYMBOL", "DATE"])
        .groupby("SYMBOL")
        .tail(vol_window)
    )

    avg_vol = (
        df_hist_last
        .groupby("SYMBOL")["volume"]
        .mean()
        .rename("avg_volume")
    )

    snap = snap.merge(avg_vol, on="SYMBOL", how="left")

    snap = snap[
        (snap["avg_volume"] >= cfg.min_avg_volume)
        & (snap["adj_close"] >= cfg.min_price)
        & (snap["adj_close"] <= cfg.max_price)
    ].copy()

    if snap.empty:
        raise RuntimeError("All symbols filtered out by liquidity / price filters")

    # Respect max_positions based on RANK
    snap = snap.sort_values("RANK").head(cfg.max_positions).copy()

    # -------------------------------------------------
    # 3) Volatility-based position sizing
    # -------------------------------------------------
    vol_series = _compute_volatility(
        hist_df[hist_df["DATE"] <= scoring_date],
        lookback=cfg.vol_lookback,
    )

    snap = snap.merge(
        vol_series,
        on="SYMBOL",
        how="left",
    )

    snap["daily_vol"] = snap["daily_vol"].fillna(0.01)  # 1% default

    # Approx stop distance as k * vol * price
    snap["stop_distance"] = (
        cfg.stop_vol_multiplier
        * snap["daily_vol"].abs()
        * snap["adj_close"]
    ).clip(lower=0.01)

    # Risk per trade in currency
    risk_per_trade_value = cfg.account_equity * cfg.risk_per_trade

    # Contract multiplier
    snap["contract_multiplier"] = snap["SYMBOL"].map(
        cfg.get_contract_multiplier
    )

    # Size = how many contracts so that risk_at_stop <= risk_per_trade_value
    snap["qty_raw"] = (
        risk_per_trade_value
        / (snap["stop_distance"] * snap["contract_multiplier"])
    )

    snap["qty"] = np.floor(snap["qty_raw"]).astype(int)
    snap = snap[snap["qty"] > 0].copy()

    if snap.empty:
        raise RuntimeError("No symbol passes volatility sizing (all qty == 0)")

    # -------------------------------------------------
    # 4) Enforce portfolio-level notional caps
    # -------------------------------------------------
    snap["notional"] = (
        snap["qty"] * snap["adj_close"] * snap["contract_multiplier"]
    )

    total_notional = snap["notional"].sum()
    max_notional = cfg.account_equity * cfg.max_gross_exposure

    if total_notional > max_notional:
        scale = max_notional / total_notional
        snap["qty"] = np.floor(snap["qty"] * scale).astype(int)
        snap["notional"] = (
            snap["qty"] * snap["adj_close"] * snap["contract_multiplier"]
        )

        snap = snap[snap["qty"] > 0].copy()

    # Per-position weight cap
    snap["weight"] = snap["notional"] / cfg.account_equity
    snap["side"] = "LONG"  # for now; we can add SHORT rules later

    # Final clean
    cols = [
        "SYMBOL",
        "side",
        "qty",
        "adj_close",
        "notional",
        "weight",
        "daily_vol",
        "stop_distance",
        "ml_score",
        "RANK",
    ]

    snap = snap[cols].sort_values("RANK").reset_index(drop=True)
    return snap
