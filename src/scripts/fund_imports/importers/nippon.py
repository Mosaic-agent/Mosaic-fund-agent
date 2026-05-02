"""
Nippon India AMC (formerly Reliance MF) monthly portfolio holdings.

Downloads monthly XLS files from mf.nipponindiaim.com and writes to
market_data.mf_holdings. Supports delta sync — already-imported months
are skipped unless --full is passed.
"""

from __future__ import annotations

import io
import re
from datetime import date, datetime
from typing import Any

import httpx
import pandas as pd

from src.scripts.fund_imports.base import BaseFundImporter, classify_asset

BASE_URL = "https://mf.nipponindiaim.com"
_ISIN_RE = re.compile(r'^[A-Z]{2}[A-Z0-9]{10}$')

# ── Monthly file list (Jan 2017 → Mar 2026) ───────────────────────────────────
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
    # 2024
    ("2024-01-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-JAN-2024.xls"),
    ("2024-02-29", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-REPORT-FEB-24.xls"),
    ("2024-03-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-REPORT-March-24.xls"),
    ("2024-04-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-APRIL-2024.xls"),
    # File deleted from Nippon server; archived copy from Wayback Machine (20240704)
    ("2024-05-31", "https://web.archive.org/web/20240704135859/https://mf.nipponindiaim.com/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-May-24.xlsx"),
    ("2024-06-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-June-24.xls"),
    ("2024-07-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-July-24.xls"),
    ("2024-08-31", "/InvestorServices/FactsheetsDocuments/NIMF_MONTHLY_PORTFOLIO_31-Aug-24.xls"),
    ("2024-09-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-30-Sep-24.xls"),
    ("2024-10-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-Oct-24.xls"),
    ("2024-11-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-30-Nov-2024.xls"),
    ("2024-12-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-Dec-24.xls"),
    # 2025
    ("2025-01-31", "/InvestorServices/FactsheetsDocuments/NIMF_MONTHLY_PORTFOLIO_31-Jan-25.xls"),
    ("2025-02-28", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-28-Feb-25.xls"),
    ("2025-03-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-Mar-25.xls"),
    ("2025-04-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-30-April-25.xls"),
    ("2025-05-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-May-25.xls"),
    ("2025-06-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-30-June-25.xls"),
    ("2025-07-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-July-25.xls"),
    ("2025-08-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-Aug-25.xls"),
    ("2025-09-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-30-Sep-25.xls"),
    ("2025-10-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-Oct-25.xls"),
    ("2025-11-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-Nov-25.xls"),
    ("2025-12-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-Dec-25.xls"),
    # 2026
    ("2026-01-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-Jan-26.xls"),
    ("2026-02-28", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-28-Feb-26.xls"),
    ("2026-03-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-Mar-26.xls"),
]

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

    def __init__(self, from_year: int = 2017, full_reimport: bool = False) -> None:
        super().__init__()
        self._from_year = from_year
        self._full_reimport = full_reimport

    def fund_name(self) -> str:
        return "Nippon India AMC"

    def fetch_sources(self) -> list[Any]:
        return [(d, p) for d, p in XLS_FILES if int(d[:4]) >= self._from_year]

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
