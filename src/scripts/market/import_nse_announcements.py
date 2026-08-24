"""
src/scripts/market/import_nse_announcements.py
─────────────────────────────────────────────
Fetch and import official NSE corporate announcements/disclosures for a specific
stock (or list of stocks) directly into ClickHouse (market_data.news_articles)
and embed them into Qdrant RAG (news_articles collection).

Usage:
  python src/scripts/market/import_nse_announcements.py --symbol NUVOCO
  python src/scripts/market/import_nse_announcements.py --symbol ADANIENT --days 180
  python src/scripts/market/import_nse_announcements.py --symbol ALL
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

# Ensure project root is on sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from rich.console import Console
from rich.table import Table

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("import_nse_announcements")
console = Console()


def import_stock_announcements(symbol: str, days: int = 365) -> int:
    """Import official NSE corporate announcements for a given stock symbol."""
    from src.data_importer.fetchers.adapters import NseAnnouncementsFetcher
    from src.data_importer.registry import STOCKS

    clean_sym = symbol.upper().replace(".NS", "").replace(".BO", "").split(":")[0].strip()
    symbols_to_fetch = [clean_sym] if clean_sym != "ALL" else [s[0] if isinstance(s, tuple) else s for s in STOCKS]

    to_dt = date.today()
    from_dt = to_dt - timedelta(days=days)

    console.print(f"\n[bold cyan]── Importing NSE Disclosures for {clean_sym} ({from_dt} to {to_dt}) ──[/bold cyan]")
    fetcher = NseAnnouncementsFetcher(symbols_to_fetch)
    rows = fetcher.fetch(from_dt, to_dt)

    if not rows:
        console.print(f"[yellow]⚠ No material announcements found for {clean_sym}[/yellow]")
        return 0

    from src.db.pool import get_pool
    pool = get_pool()
    n_inserted = fetcher.insert(rows, pool)

    # Display clean summary table
    table = Table(title=f"NSE Announcements Imported — {clean_sym} (Total: {len(rows)})", show_header=True, header_style="bold blue")
    table.add_column("Date", style="cyan", width=12)
    table.add_column("Category", style="magenta", width=25)
    table.add_column("Title", style="white")

    for r in rows[:10]:
        table.add_row(
            r["published_at"][:10],
            r["category"][:25],
            r["title"][:70],
        )

    console.print(table)
    console.print(f"[bold green]✔ Successfully indexed {len(rows)} announcements to Qdrant RAG and ClickHouse ({n_inserted} rows inserted)[/bold green]\n")
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description="Import official NSE corporate announcements for Indian equities.")
    parser.add_argument("--symbol", "-s", type=str, default="NUVOCO", help="NSE Symbol (e.g. NUVOCO, ITC, ADANIENT, or ALL)")
    parser.add_argument("--days", "-d", type=int, default=365, help="Lookback days (default: 365)")
    args = parser.parse_args()

    import_stock_announcements(symbol=args.symbol, days=args.days)


if __name__ == "__main__":
    main()
