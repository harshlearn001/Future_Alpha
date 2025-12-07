import zipfile
from io import StringIO
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
ZIP_DIR = ROOT / "data" / "raw" / "daily_raw"
OUT_DIR = ROOT / "data" / "cleaned" / "cleaned_daily"
OUT_DIR.mkdir(parents=True, exist_ok=True)

REQUIRED = {"INSTRUMENT", "SYMBOL", "EXP_DATE"}


def find_header_start(text: str):
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if line.upper().startswith("INSTRUMENT"):
            return i
    return None


def clean_latest_zip():
    zips = sorted(ZIP_DIR.glob("fo*.zip"))
    if not zips:
        print("❌ No FO zip found")
        return

    zip_path = zips[-1]
    print(f"🧼 Cleaning daily FO bhavcopy → {zip_path.name}")

    date_str = zip_path.stem.replace("fo", "")

    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            if not name.lower().endswith(".csv"):
                continue

            raw = z.read(name).decode("utf-8", errors="ignore")
            start = find_header_start(raw)

            if start is None:
                continue

            df = pd.read_csv(StringIO(raw), skiprows=start)

            df.columns = (
                df.columns.astype(str)
                .str.upper()
                .str.strip()
                .str.replace("*", "", regex=False)
            )

            if REQUIRED.issubset(df.columns):
                print(f"✅ Using futures table → {name}")
                break
        else:
            print("❌ Futures table not found in zip")
            return

    # ✅ Filter futures only
    df["INSTRUMENT"] = df["INSTRUMENT"].astype(str).str.strip().str.upper()
    df = df[df["INSTRUMENT"].isin(["FUTSTK", "FUTIDX"])]

    if df.empty:
        print("⚠️ Futures table found but no FUTSTK/FUTIDX rows")
        return

    df = df.rename(columns={
        "SYMBOL": "symbol",
        "EXP_DATE": "expiry",
        "OPEN_PRICE": "open",
        "HI_PRICE": "high",
        "LO_PRICE": "low",
        "CLOSE_PRICE": "close",
        "TRD_QTY": "volume",
        "OPEN_INT": "oi",
    })

    df["date"] = pd.to_datetime(date_str, format="%d%m%Y")
    df["expiry"] = pd.to_datetime(df["expiry"], dayfirst=True, errors="coerce")

    df = df[
        ["symbol", "date", "open", "high", "low", "close", "volume", "oi", "expiry"]
    ]

    out = OUT_DIR / f"daily_clean_{date_str}.csv"
    df.to_csv(out, index=False)

    print(f"✅ CLEAN DAILY SAVED → {out}")
    print(f"📊 Rows: {len(df)}")


if __name__ == "__main__":
    clean_latest_zip()
