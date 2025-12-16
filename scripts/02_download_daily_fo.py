# =====================================================
# Future_Alpha | STEP 1
# NSE FO Daily Bhavcopy Downloader (Automation Safe)
# =====================================================

import sys
import requests
from datetime import datetime, timedelta
from pathlib import Path

# ---------------- PATHS ----------------
ROOT = Path(__file__).resolve().parents[1]
SAVE_DIR = ROOT / "data" / "raw" / "daily_raw"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

# ---------------- NSE URL ----------------
BASE_URL = "https://nsearchives.nseindia.com/archives/fo/mkt/fo{date}.zip"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Connection": "keep-alive",
}

# ---------------- HELPERS ----------------
def is_weekday(d: datetime) -> bool:
    return d.weekday() < 5


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

        print(f"❌ Not available: fo{date_str}.zip (status={r.status_code})")
        return False

    except Exception as e:
        print(f"⚠ Network error for {date_str}: {e}")
        return False


# ---------------- MAIN ----------------
def main() -> int:
    print("\n📥 STEP 1 | NSE FO BHAVCOPY DOWNLOAD")
    print("-" * 60)

    d = datetime.today()
    lookback = 10

    print(f"📅 Starting lookup from: {d.strftime('%d-%b-%Y')}")

    for _ in range(lookback):
        if is_weekday(d):
            if try_download(d):
                print("🏁 FO download successful")
                return 0
        d -= timedelta(days=1)

    # NOT A HARD FAILURE (market closed / not published yet)
    print("⚠️ No new FO bhavcopy found in lookback window")
    print("ℹ️ Using last available data")
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
