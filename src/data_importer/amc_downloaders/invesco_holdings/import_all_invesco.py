"""
src/data_importer/amc_downloaders/invesco_holdings/import_all_invesco.py
─────────────────────────────────────────────────────────
Standalone script to fetch and import Invesco Mutual Fund portfolio holdings into ClickHouse.

Usage:
    python src/data_importer/amc_downloaders/invesco_holdings/import_all_invesco.py
    python src/data_importer/amc_downloaders/invesco_holdings/import_all_invesco.py --full
    python src/data_importer/amc_downloaders/invesco_holdings/import_all_invesco.py --dry-run
    python src/data_importer/amc_downloaders/invesco_holdings/import_all_invesco.py --month 2026-07
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime

# Ensure project root is on sys.path
sys.path.insert(0, os.getcwd())

from rich.console import Console

from src.data_importer.amc_holdings.importers.invesco import InvescoImporter

console = Console()
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Import Invesco Mutual Fund monthly portfolio holdings")
    parser.add_argument("--full", action="store_true", help="Ignore watermarks and re-import all historical files")
    parser.add_argument("--dry-run", action="store_true", help="Download and parse files without writing to ClickHouse")
    parser.add_argument("--month", type=str, default="", help="Specific month to import (YYYY-MM, e.g. 2026-07)")
    parser.add_argument("--fresh", type=int, default=0, help="Re-import the N most recent months")
    parser.add_argument("--year", type=int, default=2023, help="Earliest year to consider (default: 2023)")
    args = parser.parse_args()

    target_month = None
    if args.month:
        try:
            target_month = datetime.strptime(args.month, "%Y-%m").date()
        except ValueError:
            console.print(f"[red]Invalid month format: {args.month}. Expected YYYY-MM.[/red]")
            sys.exit(1)

    console.print(
        f"[bold blue]Starting Invesco Portfolio Import[/bold blue] "
        f"(full={args.full}, dry_run={args.dry_run}, target_month={target_month}, fresh={args.fresh})"
    )

    importer = InvescoImporter(
        full_reimport=args.full,
        from_year=args.year,
        target_month=target_month,
        freshness_months=args.fresh,
    )
    importer.run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
