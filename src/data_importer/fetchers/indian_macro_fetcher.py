"""
src/importer/fetchers/indian_macro_fetcher.py
─────────────────────────────────────────────
Fetches and parses macro and industry indicators (source: Tijori Finance).
(https://www.tijorifinance.com/in/macro).

Stores monthly, quarterly, and annual indicators into ClickHouse.
"""
from __future__ import annotations

import logging
import re
import urllib.request
from datetime import date, datetime
from typing import Any

from bs4 import BeautifulSoup

from src.data_importer.base_fetcher import Fetcher

log = logging.getLogger(__name__)

_TIJORI_MACRO_URL = "https://www.tijorifinance.com/in/macro"
_TIMEOUT = 30
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}


def _get_text_excluding_td(element: Any) -> str:
    """Helper to extract text from bs4 element excluding child td tags."""
    if isinstance(element, str):
        return element
    if element.name == "td":
        return ""
    return "".join(_get_text_excluding_td(c) for c in element.contents)


def _get_direct_text(td: Any) -> str:
    """Get text directly associated with this td (not nested tds)."""
    text_parts = []
    for content in td.contents:
        if isinstance(content, str):
            text_parts.append(content)
        elif content.name != "td":
            text_parts.append(_get_text_excluding_td(content))
    return "".join(text_parts).strip()


def _is_locked_direct(td: Any) -> bool:
    """Check if this TD contains a premium lock icon directly (not nested in child td)."""
    if td.name == "td":
        classes = td.get("class", []) or []
        if any(c in classes for c in ("data-lock", "upgrade-to-premium")):
            return True
            
    for element in td.descendants:
        if element.name == "td" and element != td:
            # Stop searching if we cross into a nested td
            break
        if hasattr(element, "get"):
            classes = element.get("class", []) or []
            if any(c in classes for c in ("data-lock", "upgrade-to-premium")):
                return True
    return False


class IndianMacroFetcher(Fetcher):
    """
    Scrapes and parses macro/industry indicator tables.
    """

    source_name = "indian_macro"
    symbol_key = "INDIA"
    description = "Indian Macro Indicators"
    overlap_days = 30  # Re-fetch recent months to catch revisions

    def fetch(self, from_date: date, to_date: date) -> list[dict[str, Any]]:
        """
        Scrapes data, parses all visible tables, and filters rows by date.
        """
        log.info("Fetching Indian macro indicators page...")
        req = urllib.request.Request(_TIJORI_MACRO_URL, headers=_HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=_TIMEOUT) as r:
                html = r.read().decode("utf-8")
        except Exception as exc:
            log.error("Failed to download Tijori macro page: %s", exc)
            return []

        soup = BeautifulSoup(html, "html.parser")
        tables = soup.find_all("table")
        log.info("Found %d tables on Tijori macro page", len(tables))

        all_records: list[dict[str, Any]] = []

        for idx, table in enumerate(tables):
            # Parse headers to identify date columns
            thead = table.find("thead")
            header_row = thead.find("tr") if thead else table.find("tr")
            if not header_row:
                log.debug("Table %d: no header row found", idx + 1)
                continue

            headers = []
            for th in header_row.find_all(["th", "td"]):
                text = " ".join(th.get_text().strip().split())
                headers.append(text)

            # Check if this table has date headers (starting at index 2)
            if len(headers) < 3:
                log.debug("Table %d: insufficient headers", idx + 1)
                continue

            # Parse date headers
            date_cols: list[date | None] = []
            for header_str in headers[2:]:
                if not header_str:
                    date_cols.append(None)
                    continue
                try:
                    # e.g., 'May 26', 'Apr 26', 'Mar 26'
                    dt = datetime.strptime(header_str, "%b %y").date()
                    date_cols.append(dt)
                except ValueError:
                    # Try alternate format if any, otherwise skip
                    log.debug("Could not parse date header: %s", header_str)
                    date_cols.append(None)

            # Parse rows
            rows = table.find_all("tr", attrs={"myid": True})
            log.debug("Table %d: parsing %d rows", idx + 1, len(rows))

            for row in rows:
                indicator_code = row["myid"]
                parent_code = row.get("parent", "None")

                # Extract metric name and unit
                name_col = row.find(class_="nameofmetriccol")
                if name_col:
                    name = " ".join(name_col.get_text().strip().split())
                    unit = name_col.get("data-unit", "")
                else:
                    name = indicator_code
                    unit = ""

                # Clean up name if it has trailing '+' or similar indicator indicators
                name = name.rstrip(" +-")

                # Get all TD elements flat list (handles nested TDs due to unclosed tags)
                tds = row.find_all("td")
                if len(tds) != len(headers):
                    log.debug(
                        "Indicator %s: tds count %d does not match headers count %d",
                        indicator_code, len(tds), len(headers)
                    )
                    continue

                data_tds = tds[2:]
                for col_idx, td in enumerate(data_tds):
                    col_date = date_cols[col_idx]
                    if col_date is None:
                        continue

                    # Filter by date range early
                    if not (from_date <= col_date <= to_date):
                        continue

                    # Check if locked
                    if _is_locked_direct(td):
                        continue

                    # Extract value
                    val_str = _get_direct_text(td)
                    # Replace dash/emdash
                    val_str = val_str.replace("\u2014", "-").replace("\u2013", "-").strip()
                    if not val_str or val_str == "-" or val_str == "":
                        continue

                    try:
                        # Clean value of commas, percentages, etc.
                        clean_str = re.sub(r"[^\d.-]", "", val_str)
                        val = float(clean_str)
                    except ValueError:
                        log.debug(
                            "Could not parse numeric value '%s' for indicator %s",
                            val_str, indicator_code
                        )
                        continue

                    all_records.append({
                        "as_of_date": col_date,
                        "indicator_code": indicator_code,
                        "indicator_name": name,
                        "parent_code": parent_code,
                        "value": val,
                        "unit": unit,
                    })

        log.info(
            "Indian macro fetch: parsed %d data points in range %s to %s",
            len(all_records), from_date, to_date
        )
        return all_records

    def insert(self, rows: list[dict[str, Any]], ch) -> int:
        """Insert records into ClickHouse."""
        return ch.insert_indian_macro_indicators(rows)

    def max_date(self, rows: list[dict[str, Any]]) -> date:
        """Extract the latest as_of_date from rows for watermark update."""
        dates = [r["as_of_date"] for r in rows if r.get("as_of_date") is not None]
        if dates:
            latest = max(dates)
            return latest if isinstance(latest, date) else latest.date()
        raise ValueError("IndianMacroFetcher: cannot determine max date from rows")
