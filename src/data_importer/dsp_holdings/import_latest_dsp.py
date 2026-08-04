"""
Fetch and import the latest available DSP month-end portfolio from dspim.com.

Auto-discovers the newest ZIP on DSP's portfolio disclosures page, compares
against the last watermark in ClickHouse, and imports only if there is new data.
Falls back to the hardcoded ZIP_FILES[-1] if scraping fails.
"""

import os
import re
import sys
import calendar
from datetime import date, datetime
from calendar import monthrange

import requests
from bs4 import BeautifulSoup
from rich.console import Console

sys.path.append(os.getcwd())

from src.data_importer.dsp_holdings.import_all_dsp_equity import run_import, ZIP_FILES, BASE_URL as MEDIA_BASE

console = Console()

_DISCLOSURES_URL = "https://www.dspim.com/mandatory-disclosures/portfolio-disclosures"
_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

MONTH_ABBR_TO_NUM = {v.lower(): k for k, v in enumerate(calendar.month_abbr) if v}


def _month_end(year: int, month: int) -> date:
    _, last = monthrange(year, month)
    return date(year, month, last)


def _parse_zip_date(href: str) -> date | None:
    """Extract month-end date from a DSP ZIP URL."""
    lower = href.lower()
    m = re.search(
        r"(january|february|march|april|may|june|july|august|september|october|november|december)"
        r"[\-_]?(\d{1,2})?[\-_]?(\d{4})",
        lower,
    )
    if not m:
        return None
    month_num = MONTH_ABBR_TO_NUM.get(m.group(1)[:3])
    year = int(m.group(3))
    if not month_num:
        return None
    return _month_end(year, month_num)


def discover_latest_zip() -> tuple[str, str] | None:
    """
    Scrape DSP's portfolio disclosures page and return (as_of_date_str, full_url)
    for the most recent month-end ZIP, or None if scraping fails.
    """
    try:
        r = requests.get(_DISCLOSURES_URL, headers=_HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as exc:
        console.print(f"[yellow]DSP website scrape failed: {exc}[/yellow]")
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    best_date: date | None = None
    best_url: str | None = None

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".zip" not in href.lower() or "monthend" not in href.lower():
            continue
        zip_date = _parse_zip_date(href)
        if zip_date is None:
            continue
        if best_date is None or zip_date > best_date:
            best_date = zip_date
            # href may be relative or absolute
            best_url = href if href.startswith("http") else "https://www.dspim.com" + href

    if best_date and best_url:
        return best_date.strftime("%Y-%m-%d"), best_url
    return None


def last_imported_date() -> date | None:
    """Return the latest as_of_month already stored for DSP_MULTI_ASSET."""
    try:
        from src.db.pool import get_client

        client = get_client()
        rows = client.query(
            "SELECT max(last_date) FROM market_data.import_watermarks "
            "WHERE source = 'mf_holdings' AND symbol = 'DSP_MULTI_ASSET'"
        ).result_rows
        client.close()
        if rows and rows[0][0]:
            return rows[0][0]
    except Exception as exc:
        console.print(f"[yellow]Could not read watermark: {exc}[/yellow]")
    return None


if __name__ == "__main__":
    console.rule("[bold cyan]DSP Latest Portfolio Import[/bold cyan]")

    # 1. Try auto-discovery from DSP website
    discovered = discover_latest_zip()

    if discovered:
        as_of_str, zip_url = discovered
        console.print(f"[green]Discovered on dspim.com:[/green] {as_of_str}  →  {zip_url.split('/')[-1]}")
    else:
        # Fallback: last hardcoded entry
        as_of_str, url_suffix = ZIP_FILES[-1]
        zip_url = MEDIA_BASE + url_suffix
        console.print(f"[yellow]Falling back to hardcoded list:[/yellow] {as_of_str}")

    # 2. Check if already imported
    last = last_imported_date()
    as_of_date = datetime.strptime(as_of_str, "%Y-%m-%d").date()

    if last and last >= as_of_date:
        console.print(
            f"[bold yellow]Already up to date.[/bold yellow] "
            f"DB has {last}, site has {as_of_date}. Nothing to import."
        )
        sys.exit(0)

    if last:
        console.print(f"DB watermark: [dim]{last}[/dim]  →  importing [bold]{as_of_date}[/bold]")
    else:
        console.print(f"No watermark found. Importing [bold]{as_of_date}[/bold]")

    # 3. Run import with the discovered URL (strip base prefix if present for run_import)
    run_import(months=[(as_of_str, zip_url)], dry_run=False)
