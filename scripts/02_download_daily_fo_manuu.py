"""
MANUAL / FALLBACK SCRIPT

Purpose:
- Manual NSE FO bhavcopy download
- Used only when daily_run.ps1 fails
- NOT part of automated daily pipeline

Safe to keep for debugging / emergency recovery.
"""

import requests
from datetime import datetime, timedelta
from pathlib import Path

# ================= PATHS =================
ROOT = Path(__file__).resolve().parents[1]
SAVE_DIR = ROOT / "data" / "raw" / "daily_raw"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

# ================= NSE URL =================
BASE_URL = "https://nsearchives.nseindia.com/archives/fo/mkt/fo{date}.zip"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}

# ================= DOWNLOAD ONE DAY =================
def try_download(d: datetime) -> bool:
    date_str = d.strftime("%d%m%Y")
    url = BASE_URL.format(date=date_str)
    out = SAVE_DIR / f"fo{date_str}.zip"

    if out.exists():
        print(f"⏩ Already exists: fo{date_str}.zip")
        return True

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)

        if r.status_code == 200 and len(r.content) > 50_000:
            out.write_bytes(r.content)
            print(f"✅ Downloaded: fo{date_str}.zip")
            return True
        else:
            print(f"❌ Not available: fo{date_str}.zip")
            return False

    except Exception as e:
        print(f"⚠ Error {date_str}: {e}")
        return False


# ================= ASK DATE =================
def ask_date():
    user = input("📅 Enter date (DD-MM-YYYY) [Enter = today]: ").strip()

    if not user:
        return datetime.today()

    try:
        return datetime.strptime(user, "%d-%m-%Y")
    except ValueError:
        print("❌ Invalid format. Use DD-MM-YYYY")
        exit(1)


# ================= MAIN =================
if __name__ == "__main__":
    print("\n📥 FUTURE_ALPHA | NSE FO BHAVCOPY DOWNLOADER")

    start_date = ask_date()
    days_back = 10

    d = start_date

    print(f"📅 Starting from: {d.strftime('%d-%b-%Y')}")

    attempts = 0
    while attempts < days_back:
        if d.weekday() < 5:  # Mon–Fri
            success = try_download(d)
            if success:
                break
        d -= timedelta(days=1)
        attempts += 1

    print("✅ Done")
