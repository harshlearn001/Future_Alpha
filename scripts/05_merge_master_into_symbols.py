import pandas as pd
from pathlib import Path

MASTER_DIR = Path(r"H:\Future_Alpha\data\master")
SYMBOLS_DIR = MASTER_DIR / "symbols"

def main():
    master_files = [f for f in MASTER_DIR.glob("*.csv")]

    print(f"📂 Found {len(master_files)} master CSV files")

    for master_file in master_files:
        symbol = master_file.stem.strip()
        symbol_file = SYMBOLS_DIR / f"{symbol}.csv"

        print(f"\n🔄 Processing {symbol}")

        # Load master
        df_master = pd.read_csv(master_file)
        df_master.columns = df_master.columns.str.lower()

        if "date" not in df_master.columns:
            print("⚠️  Skipped (no date column)")
            continue

        # Ensure correct symbol rows
        if "symbol" in df_master.columns:
            df_master = df_master[df_master["symbol"].str.strip() == symbol]
            df_master = df_master.drop(columns=["symbol"])

        df_master["date"] = pd.to_datetime(df_master["date"])

        # Load existing symbol history (if exists)
        if symbol_file.exists():
            df_sym = pd.read_csv(symbol_file)
            df_sym.columns = df_sym.columns.str.lower()
            df_sym["date"] = pd.to_datetime(df_sym["date"])
        else:
            df_sym = pd.DataFrame(columns=df_master.columns)

        # Merge
        df_final = (
            pd.concat([df_sym, df_master], ignore_index=True)
            .drop_duplicates(subset=["date", "expiry"], keep="last")
            .sort_values("date")
        )

        # Save
        df_final.to_csv(symbol_file, index=False)
        print(f"✅ Updated → {symbol_file.name} | rows: {len(df_final)}")

    print("\n🏁 MASTER → SYMBOLS MERGE COMPLETE")

if __name__ == "__main__":
    main()
