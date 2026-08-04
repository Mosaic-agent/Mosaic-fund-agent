"""
ICICI Prudential index-scheme constituent files from Azure Blob Storage.

Enumerates the blob container dynamically (no hardcoded file list) and
writes to market_data.icici_index_constituents (auto-created on first run).

Snapshot limitation: the blob stores the latest published version of each
index file. Run monthly to build a forward-going time-series.
"""

from __future__ import annotations

import io
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import date, datetime
from typing import Any

import httpx
import pandas as pd

from src.data_importer.amc_holdings.base import BaseFundImporter

# ── Azure Blob constants ──────────────────────────────────────────────────────

_BLOB_LIST_URL = (
    "https://www.icicipruamc.com/blob/statutory-disclosures-files"
    "?restype=container&comp=list"
)
_BLOB_BASE_URL = (
    "https://www.icicipruamc.com/blob/statutory-disclosures-files"
    "/Files/index-schemes-constituents"
)
_BLOB_FOLDER_PREFIX = "Files/index-schemes-constituents/"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Referer": "https://www.icicipruamc.com/about-us/statutory-disclosures",
}

# ── DDL ───────────────────────────────────────────────────────────────────────

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

_COLUMNS = [
    "index_name", "constituent_date", "symbol", "isin", "security_name",
    "industry", "close_price", "issue_cap", "weightage", "source_file", "imported_at",
]


def _list_constituent_files() -> list[tuple[str, str]]:
    with httpx.Client(headers=_HEADERS, timeout=30, follow_redirects=True) as client:
        resp = client.get(_BLOB_LIST_URL)
        resp.raise_for_status()
    root = ET.fromstring(resp.content)
    files: list[tuple[str, str]] = []
    for blob in root.find("Blobs").findall("Blob"):
        name = blob.findtext("Name") or ""
        if not name.startswith(_BLOB_FOLDER_PREFIX):
            continue
        if not (name.endswith(".xls") or name.endswith(".xlsx")):
            continue
        filename = name[len(_BLOB_FOLDER_PREFIX):]
        proxy_url = f"{_BLOB_BASE_URL}/{urllib.parse.quote(filename)}"
        files.append((filename, proxy_url))
    return files


def _parse_file(filename: str, content: bytes) -> list[dict]:
    engine = "xlrd" if filename.lower().endswith(".xls") else "openpyxl"
    try:
        df = pd.read_excel(io.BytesIO(content), header=None, engine=engine)
    except Exception as exc:
        return []
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
        vals = row.tolist()

        def safe_str(i):
            return str(vals[i]).strip() if i is not None and i < len(vals) and vals[i] == vals[i] else ""

        def safe_float(i):
            if i is None or i >= len(vals):
                return 0.0
            try:
                return float(vals[i])
            except (TypeError, ValueError):
                return 0.0

        def safe_date(i):
            if i is None or i >= len(vals):
                return None
            val = vals[i]
            if isinstance(val, (int, float)) and val != val:
                return None
            try:
                return val.date() if hasattr(val, "date") else pd.to_datetime(str(val)).date()
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


class IciciIndexImporter(BaseFundImporter):
    REQUEST_DELAY = 0.8

    def fund_name(self) -> str:
        return "ICICI Prudential Index Constituents"

    def fetch_sources(self) -> list[Any]:
        self._console.print("[bold cyan]Enumerating ICICI index constituent files...[/bold cyan]")
        files = _list_constituent_files()
        self._console.print(f"Found [bold]{len(files)}[/bold] files.")
        return files

    def parse_source(self, source: Any, http: httpx.Client) -> list[dict]:
        filename, url = source
        self._console.print(f"  Fetching [bold]{filename}[/bold]")
        try:
            resp = http.get(url)
            resp.raise_for_status()
        except Exception as exc:
            self._console.print(f"  [red]Error fetching {filename}: {exc}[/red]")
            return []

        rows = _parse_file(filename, resp.content)
        if rows:
            wt_sum = sum(r["weightage"] for r in rows)
            self._console.print(
                f"  [green]→ {rows[0]['index_name']}: {len(rows)} constituents, "
                f"date={rows[0]['constituent_date']}, wt_sum={wt_sum:.1f}%[/green]"
            )
        else:
            self._console.print(f"  [yellow]No rows parsed from {filename}[/yellow]")
        return rows

    def ensure_schema(self, client) -> None:
        client.command(_CREATE_TABLE_SQL)

    def table_name(self) -> str:
        return "market_data.icici_index_constituents"

    def column_names(self) -> list[str]:
        return _COLUMNS

    def watermark_source(self) -> str:
        return "icici_index_constituents"

    def watermark_rows(self, all_rows: list[dict]) -> list[tuple[str, date]]:
        today = date.today()
        seen = {r["index_name"] for r in all_rows if r.get("index_name")}
        return [(name, today) for name in seen]
