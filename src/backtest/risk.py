from __future__ import annotations
import numpy as np
import pandas as pd


def volatility_targeting(
    returns: pd.Series,
    target_vol: float = 0.15,
    min_vol: float = 0.05,
) -> float:
    """
    Scale returns to target volatility
    """
    realized_vol = returns.std() * (252 ** 0.5)

    if pd.isna(realized_vol):
        return 1.0

    scale = target_vol / max(realized_vol, min_vol)
    return min(scale, 1.0)


def liquidity_slippage(trdval: float) -> float:
    """
    Liquidity-aware transaction cost
    """
    if trdval <= 0 or pd.isna(trdval):
        return 0.0025

    if trdval < 5e7:
        return 0.0020
    elif trdval < 2e8:
        return 0.0015
    else:
        return 0.0010
