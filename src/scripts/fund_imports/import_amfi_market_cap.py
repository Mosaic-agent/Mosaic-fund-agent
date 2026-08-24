"""
src/scripts/fund_imports/import_amfi_market_cap.py
─────────────────────────────────────────────────
Imports AMFI Semi-Annual Average Market Capitalization rankings into ClickHouse.

Usage:
    python src/scripts/fund_imports/import_amfi_market_cap.py
    python src/scripts/fund_imports/import_amfi_market_cap.py --date 2026-06-30
    python src/scripts/fund_imports/import_amfi_market_cap.py --url https://portal.amfiindia.com/spages/...
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from datetime import date

# Bootstrap project root to sys.path
_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from rich.console import Console
from rich.table import Table

from src.data_importer.fetchers.amfi_market_cap_fetcher import fetch_amfi_market_cap
from src.data_importer.clickhouse import ClickHouseImporter

console = Console()


def run_amfi_market_cap_import(period_end: str | None = None, url: str | None = None) -> int:
    console.print(f"[bold cyan]Starting AMFI Average Market Cap Ingestion...[/bold cyan]")
    records = fetch_amfi_market_cap(period_end=period_end, url=url)
    if not records:
        console.print("[bold red]✗ No records fetched from AMFI portal.[/bold red]")
        return 0

    importer = ClickHouseImporter()
    importer.ensure_schema()
    inserted = importer.insert_amfi_market_cap(records)
    console.print(f"[bold green]✓ Successfully inserted {inserted:,} records into market_data.amfi_market_cap.[/bold green]")

    # Summary table
    table = Table(title=f"AMFI Market Cap Summary (Period End: {records[0]['period_end_date']})")
    table.add_column("Rank", justify="right", style="cyan")
    table.add_column("Company Name", style="bold white")
    table.add_column("NSE Symbol", style="yellow")
    table.add_column("Avg Market Cap (₹ Cr)", justify="right", style="green")
    table.add_column("Category", style="magenta")

    for r in records[:5]:
        table.add_row(
            str(r["rank"]),
            r["company_name"],
            r["nse_symbol"],
            f"₹{r['avg_mcap_cr']:,.2f} Cr",
            r["cap_category"]
        )
    table.add_row("...", "...", "...", "...", "...")
    for r in records[98:102]:
        table.add_row(
            str(r["rank"]),
            r["company_name"],
            r["nse_symbol"],
            f"₹{r['avg_mcap_cr']:,.2f} Cr",
            r["cap_category"]
        )
    table.add_row("...", "...", "...", "...", "...")
    for r in records[248:252]:
        table.add_row(
            str(r["rank"]),
            r["company_name"],
            r["nse_symbol"],
            f"₹{r['avg_mcap_cr']:,.2f} Cr",
            r["cap_category"]
        )
    console.print(table)
    return inserted


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import AMFI Average Market Capitalization rankings into ClickHouse")
    parser.add_argument("--date", type=str, default="2026-06-30", help="Period end date (YYYY-MM-DD)")
    parser.add_argument("--url", type=str, default=None, help="Direct URL to AMFI Excel file")
    args = parser.parse_args()

    inserted = run_amfi_market_cap_import(period_end=args.date, url=args.url)
    sys.exit(0 if inserted > 0 else 1)
