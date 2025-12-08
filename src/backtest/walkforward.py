# src/backtest/walkforward.py
from __future__ import annotations

import pandas as pd

from src.config.paths import PROCESSED_DIR, REPORTS_DIR
from src.backtest.engine import compute_metrics


# -------------------------------------------------
# Load historical daily rankings
# -------------------------------------------------
def load_rankings() -> pd.DataFrame:
    """
    Load the historical daily rankings produced by:
        python -m src.signals.build_daily_rankings
    """
    path = PROCESSED_DIR / "daily_ranking_history.csv"
    if not path.exists():
        raise FileNotFoundError(path)

    df = pd.read_csv(path, parse_dates=["DATE"])
    # Make sure sorted properly
    df = df.sort_values(["SYMBOL", "DATE"]).reset_index(drop=True)
    return df


# -------------------------------------------------
# Prepare per-symbol daily returns with clipping
# -------------------------------------------------
def prepare_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add daily_ret and next_ret columns for each symbol.
    next_ret is clipped to [-5%, +10%] as a simple risk cap.
    """
    df = df.copy()

    # Daily pct change on adj_close (continuous futures)
    df["daily_ret"] = (
        df.groupby("SYMBOL")["adj_close"]
          .pct_change()
    )

    # Forward 1-day return
    df["next_ret"] = (
        df.groupby("SYMBOL")["daily_ret"]
          .shift(-1)
    )

    # Hard risk cap for futures
    df["next_ret"] = df["next_ret"].clip(lower=-0.05, upper=0.10)

    return df


# -------------------------------------------------
# Build daily PnL for a given date range
# -------------------------------------------------
def build_pnl_for_period(
    df: pd.DataFrame,
    top_n: int,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    """
    Build daily portfolio PnL for [start, end] using
    daily top-N rankings with next-day returns.
    """
    mask = (df["DATE"] >= start) & (df["DATE"] <= end)
    sub = df.loc[mask].copy()

    if sub.empty:
        return pd.DataFrame(columns=["DATE", "portfolio_return", "equity"])

    results: list[tuple[pd.Timestamp, float]] = []

    for date, day_df in sub.groupby("DATE"):
        # Take top-N by RANK for that day
        picks = (
            day_df
            .sort_values("RANK")
            .head(top_n)
        )

        rets = picks["next_ret"].dropna()
        if len(rets) == 0:
            continue

        portfolio_ret = rets.mean()
        results.append((date, portfolio_ret))

    pnl = pd.DataFrame(results, columns=["DATE", "portfolio_return"])
    pnl = pnl.sort_values("DATE").reset_index(drop=True)
    pnl["equity"] = (1.0 + pnl["portfolio_return"]).cumprod()

    return pnl


# -------------------------------------------------
# Main walk-forward driver
# -------------------------------------------------
def main(top_n: int = 5) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    df = load_rankings()
    df = prepare_returns(df)

    years = sorted(df["DATE"].dt.year.unique().tolist())

    summary_rows: list[dict] = []

    for year in years:
        # If you want to skip early years, adjust this
        if year < 2022:
            continue

        start = pd.Timestamp(year=year, month=1, day=1)
        end = pd.Timestamp(year=year, month=12, day=31)

        pnl_year = build_pnl_for_period(df, top_n=top_n, start=start, end=end)
        if pnl_year.empty:
            continue

        stats = compute_metrics(pnl_year["portfolio_return"])

        print(f"\n📊 WALK-FORWARD RESULTS — {year}")
        print(f"CAGR: {stats['CAGR']:.3f}")
        print(f"Volatility: {stats['Volatility']:.3f}")
        print(f"Sharpe: {stats['Sharpe']:.3f}")
        print(f"Max_Drawdown: {stats['Max_Drawdown']:.3f}")
        print(f"Win_Rate: {stats['Win_Rate']:.3f}")

        # Save equity curve for that year
        out_pnl = REPORTS_DIR / f"walkforward_pnl_{year}.csv"
        pnl_year.to_csv(out_pnl, index=False)

        summary_rows.append(
            {
                "YEAR": year,
                "CAGR": stats["CAGR"],
                "Volatility": stats["Volatility"],
                "Sharpe": stats["Sharpe"],
                "Max_Drawdown": stats["Max_Drawdown"],
                "Win_Rate": stats["Win_Rate"],
            }
        )

    if summary_rows:
        summary_df = pd.DataFrame(summary_rows).sort_values("YEAR")
        summary_path = REPORTS_DIR / "walkforward_summary.csv"
        summary_df.to_csv(summary_path, index=False)
        print("\n✅ Walk-forward summary saved.")
    else:
        print("No walk-forward periods found / no data.")


if __name__ == "__main__":
    main()
