# src/utils/dates.py
from __future__ import annotations
from datetime import datetime, date
from pathlib import Path
from typing import Optional

BHAVCOPY_FMT = "%d-%m-%Y"   # 05-12-2025

def parse_date(date_str: str) -> date:
    return datetime.strptime(date_str, BHAVCOPY_FMT).date()

def format_date(d: date) -> str:
    return d.strftime(BHAVCOPY_FMT)

def infer_date_from_filename(path: Path) -> Optional[date]:
    """
    For files like:
      fo_05122025.csv
      daily_clean_05122025.csv
    """
    name = path.name
    digits = "".join(ch for ch in name if ch.isdigit())
    if len(digits) != 8:
        return None
    # 05122025 -> 05-12-2025
    dt_str = digits[:2] + "-" + digits[2:4] + "-" + digits[4:]
    return parse_date(dt_str)
