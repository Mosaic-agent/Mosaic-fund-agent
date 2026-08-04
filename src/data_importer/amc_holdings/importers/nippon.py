"""
Nippon India AMC (formerly Reliance MF) monthly portfolio holdings.

Downloads monthly XLS files from mf.nipponindiaim.com and writes to
market_data.mf_holdings. Supports delta sync — already-imported months
are skipped unless --full is passed.
"""

from __future__ import annotations

import calendar
import io
import re
from datetime import date, datetime
from typing import Any

import httpx
import pandas as pd

from src.data_importer.amc_holdings.base import BaseFundImporter, classify_asset

BASE_URL = "https://mf.nipponindiaim.com"
_ISIN_RE = re.compile(r'^[A-Z]{2}[A-Z0-9]{10}$')

# ── Monthly file list (Jan 2017 → Dec 2023) ───────────────────────────────────
# Historical entries with irregular URL formats — kept as-is.
# 2024 onward: auto-discovered by _discover_recent_months().
# (as_of_date, url_path)  — date is the last calendar day of the month

XLS_FILES: list[tuple[str, str]] = [
    # 2017 (Reliance MF era)
    ("2017-01-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.01.2017.xls"),
    ("2017-02-28", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-28-02-2017.xlsx"),
    ("2017-03-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-03-2017.xls"),
    ("2017-04-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30-04-2017.xls"),
    ("2017-05-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.05.2017.xls"),
    ("2017-06-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30-06-2017.xls"),
    ("2017-07-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.07.2017.xls"),
    ("2017-08-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-08-2017.xls"),
    ("2017-09-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30.09.2017.xls"),
    ("2017-10-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.10.2017.xls"),
    ("2017-11-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30.11.2017.xls"),
    ("2017-12-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.12.2017.xls"),
    # 2018
    ("2018-01-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.01.2018.xls"),
    ("2018-02-28", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-28.02.2018.xls"),
    ("2018-03-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.03.2018.xls"),
    ("2018-04-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30.04.2018.xls"),
    ("2018-05-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.05.2018.xls"),
    ("2018-06-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30.06.2018.xls"),
    ("2018-07-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.07.2018.xls"),
    ("2018-08-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.08.2018.xls"),
    ("2018-09-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30.09.2018.xls"),
    ("2018-10-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-10-2018.xls"),
    ("2018-11-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30.11.2018.xls"),
    ("2018-12-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-12-2018.xls"),
    # 2019 (Nippon brand takeover mid-year)
    ("2019-01-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-01-2019.xls"),
    ("2019-02-28", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-28-02-2019.xls"),
    ("2019-03-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-03-2019.xls"),
    ("2019-04-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30-04-2019.xls"),
    ("2019-05-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-05-2019.xls"),
    ("2019-06-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30-06-2019.xls"),
    ("2019-07-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-07-2019.xls"),
    ("2019-08-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-08-2019.xls"),
    ("2019-09-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30-09-2019.xls"),
    ("2019-10-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-10-2019.xls"),
    ("2019-11-30", "/InvestorServices/FactsheetsDocuments/NipponIndia-Monthly-Portfolios-30-11-2019.xls"),
    ("2019-12-31", "/InvestorServices/FactsheetsDocuments/NipponIndia-Monthly-Portfolios-31-12-2019.xls"),
    # 2020
    ("2020-01-31", "/InvestorServices/FactsheetsDocuments/NipponIndia-Monthly-Portfolios-31-01-2020.xls"),
    ("2020-02-29", "/InvestorServices/FactsheetsDocuments/NipponIndia-Monthly-Portfolios-29-02-2020.xls"),
    ("2020-03-31", "/InvestorServices/FactsheetsDocuments/NipponIndia-Monthly-Portfolios-31-03-2020.xls"),
    ("2020-04-30", "/InvestorServices/FactsheetsDocuments/NipponIndia-Monthly-Portfolios-30-04-2020.xls"),
    ("2020-05-31", "/InvestorServices/FactsheetsDocuments/NipponIndia-Monthly-Portfolios-May-2020.xls"),
    ("2020-06-30", "/InvestorServices/FactsheetsDocuments/NipponIndia-Monthly-Portfolios-June-2020.xls"),
    ("2020-07-31", "/InvestorServices/FactsheetsDocuments/NIMF-Monthly-Portfolio-July-2020.xls"),
    ("2020-08-31", "/InvestorServices/FactsheetsDocuments/NIMF-Monthly-Portfolio-Aug-2020.xls"),
    ("2020-09-30", "/InvestorServices/FactsheetsDocuments/NIMF-Monthly-Portfolio-Report-Sep-20.xls"),
    ("2020-10-31", "/InvestorServices/FactsheetsDocuments/NIMF-Monthly-PORTFOLIO_REPORT-Oct-20.xls"),
    ("2021-01-31", "/InvestorServices/FactsheetsDocuments/NIMF-Monthly-Portfolio-Jan-2021.xls"),
    ("2021-04-30", "/InvestorServices/FactsheetsDocuments/Monthly-Portfolio-as-on-30-04-2021-with-Riskometer.xls"),
    ("2021-05-31", "/InvestorServices/FactsheetsDocuments/Monthly-portfolio-May-2021-With-riskometer.xls"),
    ("2021-10-31", "/InvestorServices/FactsheetsDocuments/Monthly-portfolio-Oct-21.xls"),
    ("2021-11-30", "/InvestorServices/FactsheetsDocuments/Monthly-Portfolio-as-on-30-11-2021-with-Riskometer.xlsx"),
    ("2021-12-31", "/InvestorServices/FactsheetsDocuments/NIMF-Monthly-Portfolio-Dec-21-With-Riskometer.xls"),
    # 2022
    ("2022-03-31", "/InvestorServices/FactsheetsDocuments/Monthly-Portfolio-Mar-2022.xls"),
    ("2022-05-31", "/InvestorServices/FactsheetsDocuments/NIMF-Monthly-Portfolio-May-2022.xls"),
    ("2022-06-30", "/InvestorServices/FactsheetsDocuments/NIMF-Monthly-Portfolio-30062022.xls"),
    ("2022-07-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-31st-JULY-2022.xls"),
    ("2022-08-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-AUGUST-2022.xls"),
    ("2022-09-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-SEP-2022.xls"),
    ("2022-10-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-Oct-22.xls"),
    ("2022-11-30", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-NOV-2022.xls"),
    ("2022-12-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-DEC-22.xls"),
    # 2023
    ("2023-01-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-JAN-23.xls"),
    ("2023-02-28", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-FEB-23.xls"),
    ("2023-03-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-MAR-23.xls"),
    ("2023-04-30", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-APR-23.xls"),
    ("2023-05-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-MAY-2023.xls"),
    ("2023-06-30", "/InvestorServices/FactsheetsDocuments/NIMF_MONTHLY_PORTFOLIO_June-2023.xls"),
    ("2023-07-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-JULY-2023.xls"),
    ("2023-08-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-AUGUST-2023.xls"),
    ("2023-09-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-Sep-23.xls"),
    ("2023-10-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-OCTOBER-2023.xls"),
    ("2023-11-30", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-NOV-23.xls"),
    ("2023-12-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-DEC-23.xls"),
]

# ── Dynamic discovery for 2024 onward ─────────────────────────────────────────
# Nippon settled on a stable naming convention from 2024:
#   NIMF-MONTHLY-PORTFOLIO-{DD}-{MonthName}-{YY}.xls
# Some months use full names (April, June, July, November) others use short.
# _discover_recent_months() probes all variants via HEAD and returns confirmed URLs.

_MONTH_VARIANTS: dict[int, list[str]] = {
    1:  ["Jan"],
    2:  ["Feb"],
    3:  ["Mar"],
    4:  ["April", "Apr"],
    5:  ["May"],
    6:  ["June", "Jun"],
    7:  ["July", "Jul"],
    8:  ["Aug"],
    9:  ["Sep"],
    10: ["Oct"],
    11: ["Nov", "November"],
    12: ["Dec"],
}

_PATH_TMPL = "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-{dd}-{month}-{yy}.xls"
_PATH_TMPL_NODASH = "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-{month}-{yy}.xls"


def _last_day(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def _candidate_paths(year: int, month: int) -> list[str]:
    dd = f"{_last_day(year, month):02d}"
    yy = str(year)[-2:]
    paths = []
    for name in _MONTH_VARIANTS[month]:
        paths.append(_PATH_TMPL.format(dd=dd, month=name, yy=yy))
        paths.append(_PATH_TMPL_NODASH.format(month=name, yy=yy))
    return paths


def _discover_recent_months(
    from_year: int = 2024,
    http: httpx.Client | None = None,
) -> list[tuple[str, str]]:
    """Probe Nippon's server for monthly XLS files from `from_year` to today.

    Returns confirmed (as_of_date, url_path) pairs in chronological order.
    Only months whose file actually exists (HTTP 200) are returned.
    """
    today = date.today()
    found: list[tuple[str, str]] = []
    close_client = http is None
    if http is None:
        http = httpx.Client(timeout=15, follow_redirects=True)

    try:
        for year in range(from_year, today.year + 1):
            for month in range(1, 13):
                mo = date(year, month, _last_day(year, month))
                # Skip months in the future or the current incomplete month
                if mo >= date(today.year, today.month, 1):
                    break
                as_of_str = mo.strftime("%Y-%m-%d")
                for path in _candidate_paths(year, month):
                    url = BASE_URL + path
                    try:
                        r = http.head(url)
                        if r.status_code == 200:
                            found.append((as_of_str, path))
                            break  # first match wins; skip remaining variants
                    except Exception:
                        continue
    finally:
        if close_client:
            http.close()

    return found

_COLUMNS = [
    "scheme_code", "fund_name", "as_of_month", "isin",
    "security_name", "asset_type", "market_value_cr",
    "pct_of_nav", "imported_at",
]


def _normalise_fund_name(raw: str) -> str:
    import re as _re
    name = _re.sub(r'\s*[\(\n].*$', '', raw, flags=_re.DOTALL).strip()
    name = _re.sub(r'[&/\\]', '_AND_', name)
    name = _re.sub(r'[^A-Za-z0-9_ ]', '', name)
    name = _re.sub(r'\s+', '_', name).upper()
    name = _re.sub(r'_+', '_', name).strip('_')
    if not (name.startswith('NIPPON') or name.startswith('RELIANCE') or name.startswith('NIMF')):
        name = 'NIPPON_' + name
    return name[:80]


class NipponImporter(BaseFundImporter):
    REQUEST_DELAY = 1.0

    def __init__(self, from_year: int = 2017, full_reimport: bool = False, target_month: date | None = None, freshness_months: int = 0) -> None:
        super().__init__(target_month=target_month, freshness_months=freshness_months)
        self._from_year = from_year
        self._full_reimport = full_reimport

    def fund_name(self) -> str:
        return "Nippon India AMC"

    def fetch_sources(self) -> list[Any]:
        # Static historical list (pre-2024 URLs are too irregular to generate)
        static = [(d, p) for d, p in XLS_FILES if int(d[:4]) >= self._from_year]

        # Dynamic discovery for 2024 onward — probes server, no manual updates needed
        discover_from = max(self._from_year, 2024)
        self._console.print(f"[dim]Probing Nippon server for months from {discover_from}…[/dim]")
        dynamic = _discover_recent_months(from_year=discover_from)

        # Merge: dynamic entries override static ones for the same date
        static_dates = {d for d, _ in static}
        dynamic_new = [(d, p) for d, p in dynamic if d not in static_dates]
        merged = sorted(static + dynamic_new, key=lambda x: x[0])

        self._console.print(
            f"[dim]Sources: {len(static)} static + {len(dynamic_new)} discovered "
            f"= {len(merged)} total months[/dim]"
        )
        return merged

    def filter_sources(self, sources: list, client) -> list:
        if self._full_reimport:
            return sources
        done = self._already_imported_months(client)
        if not done:
            return sources
        before = len(sources)
        filtered = [
            (d, p) for d, p in sources
            if datetime.strptime(d, '%Y-%m-%d').date() not in done
        ]
        skipped = before - len(filtered)
        if skipped:
            self._console.print(
                f'[dim]Delta sync: {skipped} months already in DB, '
                f'{len(filtered)} to fetch.[/dim]'
            )
        return filtered

    def _already_imported_months(self, client) -> set[date]:
        try:
            result = client.query(
                "SELECT DISTINCT as_of_month FROM market_data.mf_holdings "
                "WHERE fund_name LIKE 'NIPPON%' OR fund_name LIKE 'RELIANCE%' "
                "OR fund_name LIKE 'NIMF%'"
            )
            return {row[0] for row in result.result_rows}
        except Exception:
            return set()

    def parse_source(self, source: Any, http: httpx.Client) -> list[dict]:
        as_of_str, path = source
        url = path if path.startswith('http') else BASE_URL + path
        try:
            resp = http.get(url)
            resp.raise_for_status()
        except Exception as exc:
            self._console.print(f'  [red]Download failed: {exc}[/red]')
            return []

        engine = 'xlrd' if path.lower().endswith('.xls') else 'openpyxl'
        try:
            xl = pd.ExcelFile(io.BytesIO(resp.content), engine=engine)
        except Exception:
            alt = 'openpyxl' if engine == 'xlrd' else 'xlrd'
            try:
                xl = pd.ExcelFile(io.BytesIO(resp.content), engine=alt)
            except Exception as exc:
                self._console.print(f'  [red]Cannot parse Excel: {exc}[/red]')
                return []

        as_of_date = datetime.strptime(as_of_str, '%Y-%m-%d').date()
        sheets = [s for s in xl.sheet_names if s.lower() != 'index']
        all_holdings: list[dict] = []
        imported_at = datetime.now()

        for sheet in sheets:
            try:
                df = xl.parse(sheet, header=None)
            except Exception:
                continue
            if df.shape[0] < 5 or df.shape[1] < 7:
                continue

            row0 = df.iloc[0].tolist()
            scheme_code = str(row0[0]).strip() if str(row0[0]) != 'nan' else sheet
            fund_name_raw = str(row0[1]).strip() if len(row0) > 1 else sheet
            fund_name = _normalise_fund_name(fund_name_raw)

            raw_rows: list[tuple] = []
            for _, row in df.iterrows():
                vals = row.tolist()
                if len(vals) < 7:
                    continue
                isin = str(vals[1]).strip()
                if not _ISIN_RE.match(isin):
                    continue
                name = str(vals[2]).strip()
                industry = str(vals[3]).strip()
                try:
                    val5 = vals[5]
                    mv_lacs = float(val5) if not pd.isna(val5) else 0.0
                except (TypeError, ValueError):
                    mv_lacs = 0.0
                try:
                    val6 = vals[6]
                    pct_raw = float(val6) if not pd.isna(val6) else 0.0
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
                    'scheme_code':     scheme_code,
                    'fund_name':       fund_name,
                    'as_of_month':     as_of_date,
                    'isin':            isin,
                    'security_name':   name,
                    'asset_type':      classify_asset(name, industry),
                    'market_value_cr': round(mv_lacs / 100, 4),
                    'pct_of_nav':      round(pct_raw * pct_scale, 4),
                    'imported_at':     imported_at,
                })

            pct_sum = sum(h['pct_of_nav'] for h in sheet_holdings)
            color = 'yellow' if pct_sum > 105 else 'green'
            self._console.print(
                f'  [{color}]  {fund_name}: {len(sheet_holdings)} holdings, '
                f'pct_sum={pct_sum:.1f}% ({as_of_str})[/{color}]'
            )
            all_holdings.extend(sheet_holdings)

        return all_holdings

    def table_name(self) -> str:
        return 'market_data.mf_holdings'

    def column_names(self) -> list[str]:
        return _COLUMNS

    def watermark_source(self) -> str:
        return 'mf_holdings'
