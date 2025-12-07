from pathlib import Path
import pandas as pd

ROOT = Path(r"H:\Future_Alpha\data")

HIST_DIR   = ROOT / "cleaned" / "cleaned_historical"
MASTER_DIR = ROOT / "master" / "symbols"

MASTER_DIR.mkdir(parents=True, exist_ok=True)

REQUIRED_COLS = {
    "date", "adj_open", "adj_high", "adj_low",
    "adj_close", "volume", "oi", "expiry"
}

print("Building master from cleaned historical data")

for file in sorted(HIST_DIR.glob("*_CONT.csv")):
    symbol = file.stem.replace("_CONT", "")
    print(f"Processing {symbol}")

    df = pd.read_csv(file)

    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        print(f"Skipping {symbol}, missing columns: {missing}")
        continue

    # rename to canonical names
    df = df.rename(columns={
        "adj_open":  "open",
        "adj_high":  "high",
        "adj_low":   "low",
        "adj_close": "close"
    })

    # clean
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "close"])
    df = df.sort_values("date")
    df = df.drop_duplicates(subset=["date"])

    df = df[["date", "open", "high", "low", "close", "volume", "oi", "expiry"]]

    out_file = MASTER_DIR / f"{symbol}.csv"
    df.to_csv(out_file, index=False)

    print(f"Saved {out_file.name} ({len(df)} rows)")

print("Master build complete")
