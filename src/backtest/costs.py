# src/backtest/costs.py
from __future__ import annotations
import pandas as pd
from src.config.settings import BACKTEST_SETTINGS

def apply_costs(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # For now, assume cost per trade on notional = slippage + brokerage
    cost_bps = BACKTEST_SETTINGS.slippage_bps + BACKTEST_SETTINGS.brokerage_per_turnover_bps
    # Approximate turnover = |weight| (1x capital) * |ret| ?
    # Very rough; we can refine later.
    df["turnover"] = df["weight"].abs()
    df["trading_cost"] = df["turnover"] * (cost_bps / 10000.0)
    df["net_pnl"] = df["gross_pnl"] - df["trading_cost"]
    return df
