from __future__ import annotations
import pandas as pd

from src.config.paths import PROCESSED_DIR, REPORTS_DIR
from src.backtest.engine import compute_metrics
from src.backtest.risk import volatility_targeting, liquidity_slippage
from src.data.loader import load_regime_index
from src.signals.regime import detect_market_regime



# -------------------------------------------------
# Load historical daily rankings
# -------------------------------------------------
def load_rankings() -> pd.DataFrame:
    path = PROCESSED_DIR / "daily_ranking_history.csv"
    if not path.exists():
        raise FileNotFoundError(path)

    return pd.read_csv(path, parse_dates=["DATE"])


# -------------------------------------------------
# DAILY REBALANCED BACKTEST (HARDENED + REGIME)
# -------------------------------------------------
def run_backtest(top_n: int = 5) -> pd.DataFrame:
    df = load_rankings()
    df = df.sort_values(["SYMBOL", "DATE"]).reset_index(drop=True)

    # ---------------------------------------------
    # MARKET REGIME (NIFTY)
    # ---------------------------------------------
    from src.data.loader import load_regime_index

    index_df = load_regime_index()

    regime = detect_market_regime(index_df)

    # ---------------------------------------------
    # Returns
    # ---------------------------------------------
    df["daily_ret"] = df.groupby("SYMBOL")["adj_close"].pct_change()
    df["next_ret"] = df.groupby("SYMBOL")["daily_ret"].shift(-1)

    results = []

    for date, day_df in df.groupby("DATE"):

        # ❌ RISK OFF → stay flat
        if date not in regime.index or not regime.loc[date]:
            results.append((date, 0.0))
            continue

        picks = (
            day_df
            .sort_values("RANK")
            .head(top_n)
        )

        rets = picks["next_ret"].dropna()
        if len(rets) == 0:
            continue

        raw_ret = rets.mean()

        # Vol targeting
        vol_scale = volatility_targeting(rets)
        scaled_ret = raw_ret * vol_scale

        # Liquidity slippage
        if "TRDVAL" in picks.columns:
            cost = liquidity_slippage(picks["TRDVAL"].median())
        else:
            cost = 0.002

        net_ret = scaled_ret - cost
        results.append((date, net_ret))

    pnl = pd.DataFrame(results, columns=["DATE", "portfolio_return"])
    pnl = pnl.sort_values("DATE").reset_index(drop=True)
    pnl["equity"] = (1 + pnl["portfolio_return"]).cumprod()

    return pnl


# -------------------------------------------------
# Runner
# -------------------------------------------------
def main():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    pnl = run_backtest(top_n=5)
    out = REPORTS_DIR / "backtest_top5_daily_hardened_regime.csv"
    pnl.to_csv(out, index=False)

    stats = compute_metrics(pnl["portfolio_return"])

    print("\n📊 BACKTEST RESULTS — HARDENED + REGIME (TOP 5)")
    for k, v in stats.items():
        print(f"{k}: {v:.4f}")


if __name__ == "__main__":
    main()
