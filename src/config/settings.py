# src/config/settings.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import timedelta

@dataclass
class FeatureSettings:
    # Momentum lookbacks in days
    mom_3d: int = 3
    mom_5d: int = 5
    mom_10d: int = 10

    # Volatility window
    vol_10d: int = 10

@dataclass
class SignalSettings:
    top_n: int = 20          # cross-sectional top N
    min_liquidity: float = 1e7   # example: min notional turnover
    max_positions: int = 10

@dataclass
class BacktestSettings:
    slippage_bps: float = 2.0
    brokerage_per_turnover_bps: float = 1.5
    rollover_days: int = 2   # before expiry

FEATURE_SETTINGS = FeatureSettings()
SIGNAL_SETTINGS = SignalSettings()
BACKTEST_SETTINGS = BacktestSettings()
