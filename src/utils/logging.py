# src/utils/logging.py
from __future__ import annotations
import logging
from pathlib import Path

def setup_logger(name: str = "future_alpha", log_to_file: bool = False, log_dir: Path | None = None) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    if log_to_file and log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_dir / "future_alpha.log")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger
