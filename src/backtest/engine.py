from __future__ import annotations

import numpy as np
import pandas as pd


def compute_metrics(returns):
    returns = returns.dropna()
    equity = (1 + returns).cumprod()

    cumulative = equity.iloc[-1] - 1
    years = len(returns) / 252

    cagr = (1 + cumulative) ** (1 / max(years, 0.25)) - 1

    volatility = returns.std() * (252 ** 0.5)
    sharpe = returns.mean() / volatility * (252 ** 0.5)

    max_dd = (equity / equity.cummax() - 1).min()
    win_rate = (returns > 0).mean()

    return {
        "CAGR": cagr,
        "Volatility": volatility,
        "Sharpe": sharpe,
        "Max_Drawdown": max_dd,
        "Win_Rate": win_rate,
    }
