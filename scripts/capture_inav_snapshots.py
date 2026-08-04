"""
scripts/capture_inav_snapshots.py
─────────────────────────────────
Fetch one live iNAV snapshot for every symbol in INAV_SYMBOLS directly from the
AMC websites/APIs (Nippon, Zerodha, Mirae, Motilal) — no NSE fallback — and
append each row to market_data/inav/{SYMBOL}/{YYYY}/{MM}/{DD}.csv.

Intended to run every ~5 minutes during NSE market hours via GitHub Actions
(.github/workflows/inav-snapshots.yml).
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.data_importer.fetchers.mirae_inav_fetcher import MIRAE_SYMBOLS, fetch_inav_mirae
from src.data_importer.fetchers.motilal_inav_fetcher import MOTILAL_SYMBOLS, fetch_inav_motilal
from src.data_importer.fetchers.nippon_inav_fetcher import NIPPON_SYMBOL_MAP, fetch_inav_nippon
from src.data_importer.fetchers.zerodha_inav_fetcher import ZERODHA_SYMBOLS, fetch_inav_zerodha
from src.data_importer.inav_csv import write_inav_rows
from src.data_importer.registry import INAV_SYMBOLS

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("capture_inav")

IST = ZoneInfo("Asia/Kolkata")
MARKET_OPEN = (9, 15)
MARKET_CLOSE = (15, 30)


def is_market_open(now_ist: datetime) -> bool:
    if now_ist.weekday() >= 5:
        return False
    open_dt = now_ist.replace(hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0)
    close_dt = now_ist.replace(hour=MARKET_CLOSE[0], minute=MARKET_CLOSE[1], second=0, microsecond=0)
    return open_dt <= now_ist <= close_dt


def _run_fetcher(name, fn, symbols):
    if not symbols:
        return []
    try:
        rows = fn(symbols)
        logger.info("%s AMC: %d/%d rows", name, len(rows), len(symbols))
        return rows
    except Exception as exc:
        logger.warning("%s AMC fetch failed: %s", name, exc)
        return []


def main() -> int:
    now_ist = datetime.now(IST)
    if not is_market_open(now_ist):
        print(f"[skip] Market closed at {now_ist:%Y-%m-%d %H:%M %Z}")
        return 0

    target = {s.upper() for s in INAV_SYMBOLS}
    nippon = sorted(target & set(NIPPON_SYMBOL_MAP))
    zerodha = sorted(target & set(ZERODHA_SYMBOLS))
    mirae = sorted(target & set(MIRAE_SYMBOLS))
    motilal = sorted(target & set(MOTILAL_SYMBOLS))
    uncovered = sorted(target - set(NIPPON_SYMBOL_MAP) - set(ZERODHA_SYMBOLS) - set(MIRAE_SYMBOLS) - set(MOTILAL_SYMBOLS))

    logger.info(
        "Fetching AMC iNAV at %s | Nippon=%d Zerodha=%d Mirae=%d Motilal=%d",
        now_ist.strftime("%Y-%m-%d %H:%M %Z"), len(nippon), len(zerodha), len(mirae), len(motilal),
    )
    if uncovered:
        logger.warning("No AMC fetcher for: %s", ", ".join(uncovered))

    rows: list[dict] = []
    rows += _run_fetcher("Nippon", fetch_inav_nippon, nippon)
    rows += _run_fetcher("Zerodha", fetch_inav_zerodha, zerodha)
    rows += _run_fetcher("Mirae", fetch_inav_mirae, mirae)
    rows += _run_fetcher("Motilal", fetch_inav_motilal, motilal)

    if not rows:
        logger.error("No AMC rows fetched")
        return 1

    written = write_inav_rows(rows)
    total = sum(written.values())
    logger.info("Wrote %d rows to %d files", total, len(written))
    for path, n in sorted(written.items()):
        logger.info("  %3d  %s", n, path.relative_to(REPO_ROOT))
    return 0


if __name__ == "__main__":
    sys.exit(main())
