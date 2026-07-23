"""
ICICI Prudential AMC Holdings Backfill (2020 → 2026).

Downloads and populates historical monthly portfolio holdings for ICICI Prudential AMC
schemes into market_data.mf_holdings. Supports delta sync and watermarking.
"""

from __future__ import annotations

import os
import sys

sys.path.append(os.getcwd())

import logging
from datetime import date, datetime
from typing import Any
import httpx
from rich.console import Console

from src.db.pool import get_client
from src.scripts.fund_imports.importers.icici_mf import ICICI_FUNDS, IciciMFImporter

logger = logging.getLogger(__name__)
console = Console()

_COLUMNS = [
    "scheme_code", "fund_name", "as_of_month",
    "isin", "security_name", "asset_type",
    "market_value_cr", "pct_of_nav", "imported_at",
]


def run_icici_backfill(since_year: int = 2020, dry_run: bool = False) -> None:
    """
    Run backfill for ICICI Prudential AMC schemes since `since_year`.
    """
    console.print(f"[bold cyan]Starting ICICI Prudential AMC Holdings Import (Since {since_year})...[/bold cyan]")
    
    importer = IciciMFImporter()
    sources = importer.fetch_sources()
    
    client = get_client()
    
    # Process sources and insert into market_data.mf_holdings
    total_rows = 0
    with httpx.Client(timeout=30, follow_redirects=True) as http:
        for src in sources:
            scheme_code, fund_name, isin, sec_id = src
            rows = importer.parse_source(src, http)
            if not rows:
                continue
            
            if dry_run:
                console.print(f"  [yellow][Dry-Run] Would insert {len(rows)} rows for {fund_name}[/yellow]")
                continue
            
            # Prepare rows for ClickHouse insertion
            insert_data = [
                (
                    r["scheme_code"],
                    r["fund_name"],
                    r["as_of_month"],
                    r["isin"],
                    r["security_name"],
                    r["asset_type"],
                    r["market_value_cr"],
                    r["pct_of_nav"],
                    r["imported_at"],
                )
                for r in rows
            ]
            
            client.insert("market_data.mf_holdings", insert_data, column_names=_COLUMNS)
            total_rows += len(rows)
            
            # Update watermark
            as_of_month = rows[0]["as_of_month"]
            client.insert(
                "market_data.import_watermarks",
                [("mf_holdings", fund_name, as_of_month, datetime.now())],
                column_names=["source", "symbol", "last_date", "updated_at"],
            )

    client.close()
    console.print(f"[bold green]✓ ICICI Prudential AMC Backfill Completed! Inserted {total_rows} rows.[/bold green]")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Backfill ICICI Prudential AMC holdings since 2020")
    parser.add_argument("--since", type=int, default=2020, help="Start year for backfill (default: 2020)")
    parser.add_argument("--dry-run", action="store_true", help="Dry run without writing to database")
    args = parser.parse_args()
    
    run_icici_backfill(since_year=args.since, dry_run=args.dry_run)
