# src/backtest/engine.py
from __future__ import annotations
import pandas as pd

from src.backtest.costs import apply_costs

def run_simple_daily_backtest(
    signals: pd.DataFrame,
    prices: pd.DataFrame,
    price_col: str = "close"
) -> pd.DataFrame:
    """
    Very simple long-only backtest:
      - 'signals' has columns: date, SYMBOL, weight
      - 'prices' has columns: date, SYMBOL, close
    """
    df = signals.merge(prices[["date", "SYMBOL", price_col]], on=["date", "SYMBOL"], how="left")
    df = df.sort_values(["date", "SYMBOL"])

    # Forward return (1-day)
    df["next_close"] = df.groupby("SYMBOL")[price_col].shift(-1)
    df["ret_1d"] = df["next_close"] / df[price_col] - 1.0

    df["gross_pnl"] = df["weight"] * df["ret_1d"]
    df = apply_costs(df)   # will adjust pnl

    daily = df.groupby("date")["net_pnl"].sum().to_frame("daily_return")
    daily["cum_return"] = (1 + daily["daily_return"]).cumprod() - 1
    return daily.reset_index()
