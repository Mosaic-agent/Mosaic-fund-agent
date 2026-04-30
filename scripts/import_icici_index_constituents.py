"""
scripts/import_icici_index_constituents.py
───────────────────────────────────────────
Download and import ICICI Prudential index-scheme constituent files from the
publicly accessible Azure Blob Storage behind www.icicipruamc.com.

Source page: https://www.icicipruamc.com/about-us/statutory-disclosures
             → "Schemes Constituents" section

Files are listed by enumerating the Azure Blob container at:
  https://www.icicipruamc.com/blob/statutory-disclosures-files
    → Files/index-schemes-constituents/*.xls(x)

Each file contains one row per constituent stock with columns:
  INDEX_NAME, DATE, SYMBOL, ISIN, SECURITY_NAME, BASIC_INDUSTRY,
  CLOSE_PRICE, ISSUE_CAP, INVESTIBLE_FACTOR, CAP_FACTOR, WEIGHTAGE, etc.

NOTE — snapshot limitation
──────────────────────────
The blob stores the LATEST published version of each index file.
There is no public monthly archive; each file reflects the most recent
NSE rebalancing date. Run this script monthly to build a forward-going
time-series in ClickHouse.

Target table: market_data.icici_index_constituents  (auto-created)

Usage
─────
    PYTHONPATH=/path/to/ofin-agent python scripts/import_icici_index_constituents.py
    PYTHONPATH=... python scripts/import_icici_index_constituents.py --dry-run
    PYTHONPATH=... python scripts/import_icici_index_constituents.py --test
"""

from __future__ import annotations

import argparse
import io
import os
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime

import urllib.parse
import httpx
import pandas as pd
import clickhouse_connect
from rich.console import Console
from rich.progress import Progress

sys.path.append(os.getcwd())
from config.settings import settings

console = Console()

# ── Constants ─────────────────────────────────────────────────────────────────

BLOB_LIST_URL = (
    "https://www.icicipruamc.com/blob/statutory-disclosures-files"
    "?restype=container&comp=list"
)
BLOB_BASE_URL = (
    "https://www.icicipruamc.com/blob/statutory-disclosures-files"
    "/Files/index-schemes-constituents"
)
BLOB_FOLDER_PREFIX = "Files/index-schemes-constituents/"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Referer": "https://www.icicipruamc.com/about-us/statutory-disclosures",
}

_REQUEST_DELAY: float = 0.8

# ── ClickHouse DDL ─────────────────────────────────────────────────────────────

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS market_data.icici_index_constituents
(
    index_name      String,
    constituent_date Date,
    symbol          String,
    isin            String,
    security_name   String,
    industry        String,
    close_price     Float64,
    issue_cap       Float64,
    weightage       Float64,
    source_file     String,
    imported_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (index_name, constituent_date, isin)
"""

# ── Blob enumeration ──────────────────────────────────────────────────────────

def list_constituent_files() -> list[tuple[str, str]]:
    """Return list of (filename, proxy_url) for all .xls/.xlsx files in the folder."""
    with httpx.Client(headers=_HEADERS, timeout=30, follow_redirects=True) as client:
        resp = client.get(BLOB_LIST_URL)
        resp.raise_for_status()

    root = ET.fromstring(resp.content)
    files: list[tuple[str, str]] = []
    for blob in root.find("Blobs").findall("Blob"):
        name = blob.findtext("Name") or ""
        if not name.startswith(BLOB_FOLDER_PREFIX):
            continue
        if not (name.endswith(".xls") or name.endswith(".xlsx")):
            continue
        filename = name[len(BLOB_FOLDER_PREFIX):]
        proxy_url = f"{BLOB_BASE_URL}/{urllib.parse.quote(filename)}"
        files.append((filename, proxy_url))
    return files


# ── File parsing ──────────────────────────────────────────────────────────────

def _parse_file(filename: str, content: bytes) -> list[dict]:
    """Parse one XLS/XLSX constituent file into a list of row dicts."""
    engine = "xlrd" if filename.lower().endswith(".xls") else "openpyxl"
    try:
        df = pd.read_excel(io.BytesIO(content), header=None, engine=engine)
    except Exception as exc:
        console.print(f"  [yellow]  Parse error ({filename}): {exc}[/yellow]")
        return []

    # Row 0 is the header, rows 1+ are data
    if df.shape[0] < 2:
        return []

    header = [str(c).strip().upper() for c in df.iloc[0].tolist()]

    def col(name: str) -> int | None:
        try:
            return header.index(name)
        except ValueError:
            return None

    idx_name  = col("INDEX_NAME")
    idx_date  = col("DATE")
    idx_sym   = col("SYMBOL")
    idx_isin  = col("ISIN")
    idx_sec   = col("SECURITY_NAME")
    idx_ind   = col("BASIC_INDUSTRY")
    idx_close = col("CLOSE_PRICE")
    idx_cap   = col("ISSUE_CAP")
    idx_wt    = col("WEIGHTAGE")

    rows: list[dict] = []
    imported_at = datetime.now()

    for _, row in df.iloc[1:].iterrows():
        row = row.tolist()

        def safe_str(i):
            return str(row[i]).strip() if i is not None and i < len(row) and row[i] == row[i] else ""

        def safe_float(i):
            if i is None or i >= len(row):
                return 0.0
            try:
                return float(row[i])
            except (TypeError, ValueError):
                return 0.0

        def safe_date(i):
            if i is None or i >= len(row):
                return None
            val = row[i]
            if isinstance(val, (int, float)) and val != val:
                return None
            try:
                if hasattr(val, "date"):
                    return val.date()
                return pd.to_datetime(str(val)).date()
            except Exception:
                return None

        isin = safe_str(idx_isin)
        if not isin or isin.upper() in ("ISIN", "NAN", "NONE", ""):
            continue

        constituent_date = safe_date(idx_date)
        if constituent_date is None:
            continue

        rows.append({
            "index_name":       safe_str(idx_name),
            "constituent_date": constituent_date,
            "symbol":           safe_str(idx_sym),
            "isin":             isin,
            "security_name":    safe_str(idx_sec),
            "industry":         safe_str(idx_ind),
            "close_price":      safe_float(idx_close),
            "issue_cap":        safe_float(idx_cap),
            "weightage":        safe_float(idx_wt),
            "source_file":      filename,
            "imported_at":      imported_at,
        })

    return rows


# ── Main import ───────────────────────────────────────────────────────────────

def run_import(
    files: list[tuple[str, str]] | None = None,
    dry_run: bool = False,
) -> None:
    console.print("[bold cyan]Enumerating ICICI index constituent files...[/bold cyan]")
    targets = files or list_constituent_files()
    console.print(f"Found [bold]{len(targets)}[/bold] files to import.")

    client = None
    if not dry_run:
        client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )
        client.command(_CREATE_TABLE_SQL)

    all_rows: list[dict] = []
    failed: list[str] = []

    with httpx.Client(headers=_HEADERS, timeout=30, follow_redirects=True) as http:
        with Progress() as progress:
            task = progress.add_task("[cyan]Downloading...", total=len(targets))
            for i, (filename, url) in enumerate(targets):
                progress.console.print(f"  Fetching [bold]{filename}[/bold]")
                try:
                    resp = http.get(url)
                    resp.raise_for_status()
                    rows = _parse_file(filename, resp.content)
                    if rows:
                        wt_sum = sum(r["weightage"] for r in rows)
                        console.print(
                            f"  [green]→ {rows[0]['index_name']}: "
                            f"{len(rows)} constituents, date={rows[0]['constituent_date']}, "
                            f"wt_sum={wt_sum:.1f}%[/green]"
                        )
                        all_rows.extend(rows)
                    else:
                        console.print(f"  [yellow]  No rows parsed from {filename}[/yellow]")
                        failed.append(filename)
                except Exception as exc:
                    console.print(f"  [red]  Error {filename}: {exc}[/red]")
                    failed.append(filename)
                if i < len(targets) - 1:
                    time.sleep(_REQUEST_DELAY)
                progress.advance(task)

    if dry_run:
        console.print(
            f"[bold blue]DRY RUN: {len(all_rows)} constituent rows "
            f"from {len(targets) - len(failed)}/{len(targets)} files — nothing inserted.[/bold blue]"
        )
        return

    if not all_rows:
        console.print("[red]✗ No rows to insert.[/red]")
        if client:
            client.close()
        return

    insert_rows = [
        (
            r["index_name"], r["constituent_date"],
            r["symbol"], r["isin"], r["security_name"],
            r["industry"], r["close_price"], r["issue_cap"],
            r["weightage"], r["source_file"], r["imported_at"],
        )
        for r in all_rows
    ]
    client.insert(
        "market_data.icici_index_constituents",
        insert_rows,
        column_names=[
            "index_name", "constituent_date", "symbol", "isin", "security_name",
            "industry", "close_price", "issue_cap", "weightage",
            "source_file", "imported_at",
        ],
    )
    console.print(
        f"[bold green]✓ Inserted {len(insert_rows)} rows for "
        f"{len(targets) - len(failed)}/{len(targets)} index files.[/bold green]"
    )

    # Watermark per index
    from datetime import date
    today = date.today()
    watermark_rows = [
        ["icici_index_constituents", r["index_name"], today]
        for r in {r["index_name"]: r for r in all_rows}.values()
    ]
    client.insert(
        "market_data.import_watermarks",
        watermark_rows,
        column_names=["source", "symbol", "last_date"],
    )
    console.print(f"[dim]Watermark set for {len(watermark_rows)} indices.[/dim]")
    if failed:
        console.print(f"[yellow]Skipped {len(failed)} files: {failed}[/yellow]")
    client.close()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Import ICICI Prudential index constituent files from statutory disclosures"
    )
    parser.add_argument("--dry-run", action="store_true", help="Download and parse but skip DB insert")
    parser.add_argument("--test", action="store_true", help="Test with first file only, no DB insert")
    args = parser.parse_args()

    if args.test:
        files = list_constituent_files()
        run_import(files=files[:1], dry_run=True)
    elif args.dry_run:
        run_import(dry_run=True)
    else:
        run_import()
