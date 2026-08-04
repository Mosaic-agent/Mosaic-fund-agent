"""
Bajaj Finserv AMC monthly + fortnightly portfolio holdings.

Bajaj's "Downloads" page (bajajamc.com/downloads?portfolio=) is a WordPress
plugin (bajaj-downloads) driven entirely by AJAX — there are no static file
URLs. Discovery is three AJAX calls deep:

    1. bajaj_get_filter_options(filter_for=years,  section_id=<id>) -> FY labels
    2. bajaj_get_filter_options(filter_for=months, section_id=<id>, year=<fy>)
       -> only months that actually have a published file
    3. bajaj_get_downloads(section_id=<id>, year=<fy>, month=<name>)
       -> HTML snippet with one <a href> per file (Fortnightly months have TWO:
          mid-month + month-end)

The AJAX nonce is short-lived and embedded in the downloads page's inline
script (`window.bajajDownloads.nonce`), so it is scraped fresh on every run.

section_id 757 = Monthly Portfolio (equity/hybrid/ETF schemes)
section_id 756 = Fortnightly Portfolio (debt/liquid schemes, SEBI mandate)

Each downloaded workbook has one sheet per scheme, all sharing the same
column layout: [code, Name of Instrument, ISIN, Industry/Rating, Quantity,
Market/Fair Value (Rs. Lakhs), % to Net Assets, YTM~, YTC^].

BFMAF (Bajaj Finserv Multi Asset Allocation Fund) is special-cased to the
legacy identity ("152639" / "BAJAJ_MULTI_ASSET") used by the old Morningstar
watchlist entry it replaces, so src/scripts/portfolio/multi_asset_consensus.py
and multi_asset_holdings_mom_yoy.py keep working unchanged.
"""

from __future__ import annotations

import io
import logging
import re
import time
from datetime import date, datetime
from typing import Any

import httpx
import pandas as pd

from src.data_importer.amc_holdings.base import BaseFundImporter, classify_asset, COMMON_HEADERS

logger = logging.getLogger(__name__)

_DOWNLOADS_PAGE_URL = "https://www.bajajamc.com/downloads?portfolio="
_AJAX_URL = "https://www.bajajamc.com/wp-admin/admin-ajax.php"
_TIMEOUT = 30

# data-section-id values on the downloads page for the two accordions we care about
_SECTIONS: dict[str, str] = {
    "monthly": "757",
    "fortnightly": "756",
}

_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")

_ROW_RE = re.compile(
    r'<span class="bd-download-title">(.*?)</span>\s*<a href="([^"]+)"',
    re.S,
)
# Month name is usually abbreviated ("Jun") but has also shown up spelled out
# ("March") or misspelled ("Septemeber", seen in a 2023 title) — capture day,
# month word, and year separately so only the first 3 letters of the month
# need to be right.
_DATE_RE = re.compile(r"as on (\d{1,2}) ([A-Za-z]+) (\d{4})", re.I)

# Legacy identity from the retired Morningstar watchlist entry — preserved so
# downstream cross-fund scripts filtering on scheme_code/fund_name still match.
_SCHEME_OVERRIDES: dict[str, tuple[str, str]] = {
    "BFMAF": ("152639", "BAJAJ_MULTI_ASSET"),
}

_COLUMNS = [
    "scheme_code", "fund_name", "as_of_month", "isin",
    "security_name", "asset_type", "market_value_cr",
    "pct_of_nav", "imported_at",
]


def _normalise_fund_name(raw: str) -> str:
    name = re.sub(r"\s*[\(\n].*$", "", raw, flags=re.DOTALL).strip()
    name = re.sub(r"[&/\\]", "_AND_", name)
    name = re.sub(r"[^A-Za-z0-9_ ]", "", name)
    name = re.sub(r"\s+", "_", name).upper()
    name = re.sub(r"_+", "_", name).strip("_")
    if not name.startswith("BAJAJ"):
        name = "BAJAJ_" + name
    return name[:80]


def _scrape_nonce(http: httpx.Client) -> str:
    resp = http.get(_DOWNLOADS_PAGE_URL)
    resp.raise_for_status()
    m = re.search(r'var bajajDownloads\s*=\s*\{[^}]*"nonce"\s*:\s*"([^"]+)"', resp.text)
    if not m:
        raise ValueError("Could not find bajajDownloads.nonce on downloads page")
    return m.group(1)


def _ajax_post(http: httpx.Client, nonce: str, action: str, **params: str) -> dict:
    data = {"action": action, "nonce": nonce, **params}
    resp = http.post(_AJAX_URL, data=data)
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("success"):
        raise ValueError(f"Bajaj AJAX '{action}' failed: {payload}")
    return payload["data"]


def _get_years(http: httpx.Client, nonce: str, section_id: str) -> list[str]:
    data = _ajax_post(http, nonce, "bajaj_get_filter_options", filter_for="years", section_id=section_id)
    return [o["value"] for o in data.get("options", [])]


def _get_months(http: httpx.Client, nonce: str, section_id: str, year: str) -> list[str]:
    data = _ajax_post(
        http, nonce, "bajaj_get_filter_options",
        filter_for="months", section_id=section_id, year=year,
    )
    return [o["value"] for o in data.get("options", [])]


def _get_download_entries(
    http: httpx.Client, nonce: str, section_id: str, year: str, month: str,
) -> list[tuple[date, str]]:
    data = _ajax_post(
        http, nonce, "bajaj_get_downloads",
        section_id=section_id, year=year, month=month,
    )
    html = data.get("html", "")
    entries: list[tuple[date, str]] = []
    for title, href in _ROW_RE.findall(html):
        m = _DATE_RE.search(title)
        if not m:
            continue
        day, month_word, year_word = m.groups()
        try:
            as_of = datetime.strptime(f"{day} {month_word[:3]} {year_word}", "%d %b %Y").date()
        except ValueError:
            logger.warning("Bajaj: unparseable date in title %r", title)
            continue
        entries.append((as_of, href))
    return entries


class BajajImporter(BaseFundImporter):
    REQUEST_DELAY = 1.0
    _DISCOVERY_DELAY = 0.3  # between the lightweight AJAX discovery calls

    def __init__(self, full_reimport: bool = False, target_month: date | None = None, freshness_months: int = 0, **kwargs) -> None:
        super().__init__(target_month=target_month, freshness_months=freshness_months)
        self.full_reimport = full_reimport
        self._category_dates: dict[str, date] = {}

    def fund_name(self) -> str:
        return "Bajaj Finserv AMC"

    def table_name(self) -> str:
        return "market_data.mf_holdings"

    def column_names(self) -> list[str]:
        return _COLUMNS

    def watermark_source(self) -> str:
        return "mf_holdings"

    # ── Discovery ──────────────────────────────────────────────────────────

    def fetch_sources(self) -> list[tuple[date, str, str]]:
        sources: list[tuple[date, str, str]] = []
        with httpx.Client(headers=COMMON_HEADERS, timeout=_TIMEOUT, follow_redirects=True) as http:
            try:
                nonce = _scrape_nonce(http)
            except Exception as exc:
                self._console.print(f"[red]Bajaj: failed to obtain AJAX nonce: {exc}[/red]")
                return []

            for category, section_id in _SECTIONS.items():
                try:
                    years = _get_years(http, nonce, section_id)
                except Exception as exc:
                    self._console.print(f"[yellow]Bajaj {category}: could not list years: {exc}[/yellow]")
                    continue

                if not self.full_reimport and not self._target_month and self._freshness_months == 0:
                    # Newest FY plus one fallback for the first weeks of a new
                    # financial year, when the current FY has no months yet.
                    years = years[:2]

                for year in years:
                    time.sleep(self._DISCOVERY_DELAY)
                    try:
                        months = _get_months(http, nonce, section_id, year)
                    except Exception as exc:
                        self._console.print(f"[yellow]Bajaj {category} {year}: {exc}[/yellow]")
                        continue

                    for month in months:
                        time.sleep(self._DISCOVERY_DELAY)
                        try:
                            entries = _get_download_entries(http, nonce, section_id, year, month)
                        except Exception as exc:
                            self._console.print(f"[yellow]Bajaj {category} {year}-{month}: {exc}[/yellow]")
                            continue
                        for as_of, url in entries:
                            sources.append((as_of, category, url))

        sources.sort()
        self._console.print(f"[dim]Bajaj: discovered {len(sources)} portfolio file(s).[/dim]")
        return sources

    def filter_sources(
        self, sources: list[tuple[date, str, str]], client,
    ) -> list[tuple[date, str, str]]:
        if self.full_reimport:
            return sources

        watermarks: dict[str, date] = {}
        try:
            rows = client.query(
                "SELECT symbol, max(last_date) FROM market_data.import_watermarks "
                "WHERE source = 'mf_holdings' AND symbol IN ('BAJAJ_MONTHLY', 'BAJAJ_FORTNIGHTLY') "
                "GROUP BY symbol"
            ).result_rows
            watermarks = {sym: dt for sym, dt in rows}
        except Exception as exc:
            self._console.print(f"[yellow]Failed to query Bajaj watermarks: {exc}[/yellow]")

        if not watermarks:
            return sources

        filtered = []
        for as_of, category, url in sources:
            wm = watermarks.get(f"BAJAJ_{category.upper()}")
            if wm is None or as_of > wm:
                filtered.append((as_of, category, url))

        skipped = len(sources) - len(filtered)
        if skipped:
            self._console.print(
                f"[dim]Delta sync: {skipped} Bajaj file(s) already in DB, "
                f"{len(filtered)} to fetch.[/dim]"
            )
        return filtered

    # ── Parsing ────────────────────────────────────────────────────────────

    def parse_source(self, source: tuple[date, str, str], http: httpx.Client) -> list[dict]:
        as_of_date, category, url = source
        try:
            resp = http.get(url)
            resp.raise_for_status()
        except Exception as exc:
            self._console.print(f"  [red]Download failed ({url}): {exc}[/red]")
            return []

        try:
            xl = pd.ExcelFile(io.BytesIO(resp.content), engine="openpyxl")
        except Exception:
            try:
                xl = pd.ExcelFile(io.BytesIO(resp.content), engine="xlrd")
            except Exception as exc:
                self._console.print(f"  [red]Cannot parse Bajaj workbook: {exc}[/red]")
                return []

        all_holdings: list[dict] = []
        imported_at = datetime.now()

        for sheet in xl.sheet_names:
            try:
                df = xl.parse(sheet, header=None)
            except Exception:
                continue
            if df.shape[0] < 5 or df.shape[1] < 7:
                continue

            row0 = df.iloc[0].tolist()
            sheet_code = str(row0[0]).strip() if str(row0[0]) != "nan" else sheet
            fund_name_raw = str(row0[1]).strip() if len(row0) > 1 else sheet

            override = _SCHEME_OVERRIDES.get(sheet_code)
            scheme_code = override[0] if override else sheet_code
            fund_name = override[1] if override else _normalise_fund_name(fund_name_raw)

            raw_rows: list[tuple[str, str, str, float, float]] = []
            for _, row in df.iterrows():
                vals = row.tolist()
                if len(vals) < 7:
                    continue
                isin = str(vals[2]).strip()
                if not _ISIN_RE.match(isin):
                    continue
                name = str(vals[1]).strip()
                industry = str(vals[3]).strip()
                try:
                    mv_lacs = float(vals[5])
                except (TypeError, ValueError):
                    mv_lacs = 0.0
                try:
                    pct_raw = float(vals[6])
                except (TypeError, ValueError):
                    pct_raw = 0.0
                raw_rows.append((isin, name, industry, mv_lacs, pct_raw))

            if not raw_rows:
                continue

            valid_pcts = [r[4] for r in raw_rows if r[4] == r[4] and r[4] != 0]
            max_pct = max(valid_pcts) if valid_pcts else 0.0
            pct_scale = 100.0 if max_pct <= 2.0 else 1.0

            sheet_holdings: list[dict] = []
            for isin, name, industry, mv_lacs, pct_raw in raw_rows:
                sheet_holdings.append({
                    "scheme_code":     scheme_code,
                    "fund_name":       fund_name,
                    "as_of_month":     as_of_date,
                    "isin":            isin,
                    "security_name":   name,
                    "asset_type":      classify_asset(name, industry),
                    "market_value_cr": round(mv_lacs / 100, 4),
                    "pct_of_nav":      round(pct_raw * pct_scale, 4),
                    "imported_at":     imported_at,
                })

            pct_sum = sum(h["pct_of_nav"] for h in sheet_holdings)
            color = "yellow" if pct_sum > 105 else "green"
            self._console.print(
                f"  [{color}]{fund_name}: {len(sheet_holdings)} holdings, "
                f"pct_sum={pct_sum:.1f}% ({as_of_date}, {category})[/{color}]"
            )
            all_holdings.extend(sheet_holdings)

        current_max = self._category_dates.get(category)
        if current_max is None or as_of_date > current_max:
            self._category_dates[category] = as_of_date

        return all_holdings

    def watermark_rows(self, all_rows: list[dict]) -> list[tuple[str, date]]:
        return [
            (f"BAJAJ_{category.upper()}", as_of)
            for category, as_of in self._category_dates.items()
        ]
