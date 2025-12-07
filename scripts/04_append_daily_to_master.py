import pandas as pd
from pathlib import Path

# ---------------- PATHS ----------------
ROOT = Path(__file__).resolve().parents[1]

CLEAN_DAILY_DIR = ROOT / "data" / "cleaned" / "cleaned_daily"
MASTER_DIR = ROOT / "data" / "master"
MASTER_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------
def get_latest_clean_file():
    files = sorted(CLEAN_DAILY_DIR.glob("daily_clean_*.csv"))
    if not files:
        print("❌ No cleaned daily files found")
        return None
    return files[-1]


def main():
    print("\n📥 STEP 4 | APPENDING DAILY CLEAN → MASTER")

    daily_file = get_latest_clean_file()
    if not daily_file:
        return

    print(f"📄 Using daily file → {daily_file.name}")
    df = pd.read_csv(daily_file)

    if df.empty:
        print("⚠️ Daily clean file is empty")
        return

    df["date"] = pd.to_datetime(df["date"])
    df["expiry"] = pd.to_datetime(df["expiry"])

    symbols = df["symbol"].unique()
    print(f"📊 Symbols found: {len(symbols)}\n")

    for sym in sorted(symbols):
        sym_df = df[df["symbol"] == sym].copy()
        out_file = MASTER_DIR / f"{sym}.csv"

        if out_file.exists():
            old = pd.read_csv(out_file)
            old["date"] = pd.to_datetime(old["date"])
            old["expiry"] = pd.to_datetime(old["expiry"])

            combined = pd.concat([old, sym_df], ignore_index=True)

            # Drop exact duplicates (date + expiry + symbol)
            combined = combined.drop_duplicates(
                subset=["symbol", "date", "expiry"]
            )

        else:
            combined = sym_df

        combined = combined.sort_values(["date", "expiry"])
        combined.to_csv(out_file, index=False)

        print(f"✅ {sym:<12} → rows: {len(combined)}")

    print("\n🏁 MASTER UPDATE COMPLETE")


if __name__ == "__main__":
    main()
