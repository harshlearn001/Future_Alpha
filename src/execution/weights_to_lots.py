from __future__ import annotations
import pandas as pd


CONTRACT_SPECS = {
    "BAJAJFINSV": {"lot_size": 125, "margin_per_lot": 60000},
    "NATIONALUM": {"lot_size": 5000, "margin_per_lot": 45000},
    "INDUSTOWER": {"lot_size": 4400, "margin_per_lot": 38000},
    "HEROMOTOCO": {"lot_size": 100,  "margin_per_lot": 52000},
    "ASIANPAINT": {"lot_size": 200,  "margin_per_lot": 70000},
}


def convert_weights_to_lots(
    ranked_df: pd.DataFrame,
    capital: float,
    side: str = "BUY",
    max_lots_per_symbol: int = 10,
) -> pd.DataFrame:

    rows = []

    for _, row in ranked_df.iterrows():
        symbol = row["SYMBOL"]
        weight = row["weight"]
        price = row["CLOSE"]

        if symbol not in CONTRACT_SPECS:
            continue

        margin = CONTRACT_SPECS[symbol]["margin_per_lot"]
        allocated = capital * weight

        lots = int(allocated // margin)
        lots = min(lots, max_lots_per_symbol)

        if lots <= 0:
            continue

        rows.append({
            "SYMBOL": symbol,
            "SIDE": side,
            "LOTS": lots,
            "PRICE": round(price, 2),
            "MARGIN_USED": lots * margin,
            "WEIGHT": round(weight, 4),
        })

    return pd.DataFrame(rows)
