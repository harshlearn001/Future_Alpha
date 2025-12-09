import pandas as pd
from pathlib import Path

# ---------------- PATHS ----------------
ROOT = Path(__file__).resolve().parents[1]

CLEAN_DAILY_DIR = ROOT / "data" / "cleaned" / "cleaned_daily"
SYMBOLS_DIR = ROOT / "data" / "master" / "symbols"
SYMBOLS_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------
def get_latest_clean_file():
    files = sorted(CLEAN_DAILY_DIR.glob("daily_clean_*.csv"))
    if not files:
        print("❌ No cleaned daily files found")
        return None
    return files[-1]


def main():
    print("\n📥 STEP 5 | APPENDING DAILY CLEAN → SYMBOL MASTER")

    daily_file = get_latest_clean_file()
    if not daily_file:
        return

    print(f"📄 Using daily file → {daily_file.name}")
    df = pd.read_csv(daily_file)

    if df.empty:
        print("⚠️ Daily clean file is empty")
        return

    # ✅ normalize columns
    df.columns = [c.lower().strip() for c in df.columns]

    # ✅ enforce correct dtypes
    df["date"] = pd.to_datetime(df["date"])
    df["expiry"] = pd.to_datetime(df["expiry"])
    df["symbol"] = df["symbol"].astype(str).str.strip()

    base_cols = ["date", "open", "high", "low", "close", "volume", "oi", "expiry"]

    symbols = sorted(df["symbol"].unique())
    print(f"📊 Symbols found: {len(symbols)}\n")

    for sym in symbols:
        sym_df = df[df["symbol"] == sym][base_cols].copy()

        out_file = SYMBOLS_DIR / f"{sym}.csv"

        if out_file.exists():
            old = pd.read_csv(out_file)

            # ✅ normalize old file as well
            old.columns = [c.lower().strip() for c in old.columns]
            old["date"] = pd.to_datetime(old["date"])
            old["expiry"] = pd.to_datetime(old["expiry"])

            combined = pd.concat([old, sym_df], ignore_index=True)

            # ✅ STRICT duplicate protection
            combined = combined.drop_duplicates(
                subset=["date", "expiry"],
                keep="last"
            )
        else:
            combined = sym_df

        combined = combined.sort_values(["date", "expiry"]).reset_index(drop=True)
        combined.to_csv(out_file, index=False)

        print(f"✅ {sym:<12} → rows: {len(combined)}")

    print("\n🏁 SYMBOL MASTER UPDATE COMPLETE")


if __name__ == "__main__":
    main()
