"""
src/scripts/fund_imports/run.py
────────────────────────────
Unified CLI entry point for all fund importers.

Usage
─────
    python src/scripts/fund_imports/run.py icici [--dry-run] [--test]
    python src/scripts/fund_imports/run.py nippon [--dry-run] [--test] [--from-year 2020] [--full]
    python src/scripts/fund_imports/run.py icici-index [--dry-run] [--test]
    python src/scripts/fund_imports/run.py all [--dry-run]

Run from the project root:
    PYTHONPATH=. python src/scripts/fund_imports/run.py nippon --from-year 2024
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.append(os.getcwd())

from src.data_importer.amc_holdings.factory import REGISTRY, create_importer


def main() -> None:
    parser = argparse.ArgumentParser(
        description="AMC fund holdings importer (factory pattern)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="\n".join([
            "importers:",
            "  icici        ICICI Prudential MF via Morningstar API (snapshot)",
            "  nippon       Nippon India AMC monthly XLS files (2017–present)",
            "  icici-index  ICICI Prudential index constituents (Azure Blob)",
            "  kotak        Kotak Mahindra AMC fund holdings",
            "  hdfc         HDFC Asset Management Company fund holdings",
            "  all          Run all importers in sequence",
        ]),
    )
    parser.add_argument(
        "name",
        choices=[*list(REGISTRY), "all"],
        metavar="name",
        help="Importer to run: " + ", ".join([*list(REGISTRY), "all"]),
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and print counts; skip DB insert")
    parser.add_argument("--test", action="store_true",
                        help="Process first source only; implies --dry-run behaviour")
    parser.add_argument("--from-year", type=int, default=2020,
                        help="Earliest year to import (default: 2020)")
    parser.add_argument("--full", action="store_true",
                        help="Reimport all months, ignoring watermarks")
    args = parser.parse_args()

    names = list(REGISTRY) if args.name == "all" else [args.name]

    for name in names:
        kwargs: dict = {}
        if name in ("nippon", "icici", "dsp", "quant", "bajaj", "kotak", "hdfc"):
            kwargs = {"from_year": args.from_year, "full_reimport": args.full}
        importer = create_importer(name, **kwargs)
        importer.run(dry_run=args.dry_run, test=args.test)


if __name__ == "__main__":
    main()
