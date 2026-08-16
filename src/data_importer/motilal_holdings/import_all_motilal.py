"""
src/data_importer/motilal_holdings/import_all_motilal.py
─────────────────────────────────────────────────────────
CLI entry-point for importing Motilal Oswal Mutual Fund portfolio holdings into ClickHouse.

Usage:
    python -m src.data_importer.motilal_holdings.import_all_motilal [--dry-run] [--full] [--from-year YYYY] [--month YYYY-MM]
    python src/scripts/motilal/import_all_motilal.py [--dry-run] [--full] [--month YYYY-MM]
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime

from src.data_importer.amc_holdings.importers.motilal import MotilalOswalImporter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import Motilal Oswal Mutual Fund portfolio disclosures into ClickHouse (market_data.mf_holdings)"
    )
    parser.add_argument("--dry-run", action="store_true", help="Parse without inserting into ClickHouse")
    parser.add_argument("--full", action="store_true", help="Full historical backfill across all available years")
    parser.add_argument("--from-year", type=int, default=2017, help="Earliest year to process (default: 2017)")
    parser.add_argument("--month", type=str, default=None, help="Target month in YYYY-MM format")
    args = parser.parse_args()

    target_month = None
    if args.month:
        try:
            target_month = datetime.strptime(args.month, "%Y-%m").date()
        except ValueError:
            parser.error(f"Invalid --month format: '{args.month}'. Expected YYYY-MM (e.g. 2026-07)")

    importer = MotilalOswalImporter(
        from_year=args.from_year,
        full_reimport=args.full,
        target_month=target_month,
    )

    result = importer.run(dry_run=args.dry_run)
    logger.info("Motilal Oswal Mutual Fund import completed: %s", result)


if __name__ == "__main__":
    main()
