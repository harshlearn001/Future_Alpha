# src/risk/config.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class RiskConfig:
    # Account level
    account_equity: float = 10_00_000.0    # 10L example, change for yourself
    risk_per_trade: float = 0.005         # 0.5% of equity per position
    max_gross_exposure: float = 2.0       # 200% of equity (using futures leverage)

    # Portfolio shape
    max_positions: int = 10               # total symbols to hold
    max_weight_per_position: float = 0.15 # 15% of equity per symbol (notional cap)

    # Volatility / stop logic
    vol_lookback: int = 10                # days for volatility estimate
    stop_vol_multiplier: float = 2.0      # stop distance = k * daily_vol

    # Liquidity filters
    min_avg_volume: float = 5_00_000      # min avg volume (adjust for FO universe)
    min_price: float = 50.0               # avoid penny junk
    max_price: float = 5_000.0            # avoid too expensive contracts

    # Futures contract multipliers (you can fill real values gradually)
    contract_multipliers: Dict[str, int] = field(default_factory=dict)

    def get_contract_multiplier(self, symbol: str) -> int:
        # Default if not provided
        return self.contract_multipliers.get(symbol, 1)
