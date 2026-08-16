"""
src/data_importer/axis_holdings/import_all_axis.py
───────────────────────────────────────────────────
Standalone CLI script to import Axis Mutual Fund monthly portfolio disclosures.

Usage:
    python -m src.data_importer.axis_holdings.import_all_axis
    python -m src.data_importer.axis_holdings.import_all_axis --dry-run
    python -m src.data_importer.axis_holdings.import_all_axis --month 2026-07
    python -m src.data_importer.axis_holdings.import_all_axis --full
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime

from src.data_importer.amc_holdings.importers.axis import AxisImporter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Import Axis Mutual Fund monthly portfolio holdings.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and parse without inserting to ClickHouse.")
    parser.add_argument("--full", action="store_true", help="Re-import full history.")
    parser.add_argument("--from-year", type=int, default=2020, help="Starting year for full re-import (default: 2020).")
    parser.add_argument("--month", type=str, default=None, help="Target month in YYYY-MM format (e.g. 2026-07).")
    parser.add_argument("--fresh", type=int, default=0, help="Freshness threshold in months.")
    args = parser.parse_args()

    target_date = None
    if args.month:
        try:
            dt = datetime.strptime(args.month, "%Y-%m")
            target_date = dt.date()
        except ValueError:
            logger.error("Invalid --month format '%s'. Use YYYY-MM (e.g. 2026-07).", args.month)
            return

    importer = AxisImporter(
        full_reimport=args.full,
        from_year=args.from_year,
        target_month=target_date,
        freshness_months=args.fresh,
    )
    result = importer.run(dry_run=args.dry_run)
    logger.info("Axis Mutual Fund import completed: %s", result)


if __name__ == "__main__":
    main()
