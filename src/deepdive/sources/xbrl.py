"""
src/deepdive/sources/xbrl.py
─────────────────────────────
XBRL financial data fetcher using the sec-api.io XbrlApi.

XbrlApi.xbrl_to_json(htm_url) returns a standardised dict with keys:
  StatementsOfIncome
  BalanceSheets
  StatementsOfCashFlows
  (and others — see sec-api.io docs)

Unlike raw EDGAR companyfacts JSON, XbrlApi normalises concept names
across filers so no fallback-chain lookups are needed.

Cache-first: raw XBRL JSON is written to cache_dir/xbrl_{accession_no}.json
on first call; subsequent calls read from disk.

All monetary values in the XBRL response are in USD (not millions).
The extract_* helpers convert to USD millions (÷ 1_000_000) and round to 1dp.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_M = 1_000_000.0  # USD → USD millions divisor


# ── Raw XBRL fetch ────────────────────────────────────────────────────────────

def get_financials(
    htm_url: str,
    accession_no: str,
    cache_dir: Path,
    api_key: str,
) -> dict[str, Any]:
    """
    Fetch XBRL data for a single filing via XbrlApi.xbrl_to_json().

    Args:
        htm_url:       Full URL of the 10-K / 10-Q filing .htm document
        accession_no:  SEC accession number (used as cache filename key)
        cache_dir:     Directory for caching the raw XBRL JSON
        api_key:       sec-api.io API key

    Returns:
        Raw XBRL JSON dict from XbrlApi. Empty dict on failure.

    Cache:
        cache_dir/xbrl_{safe_accession}.json
    """
    safe_acc = accession_no.replace("-", "")
    cache_path = cache_dir / f"xbrl_{safe_acc}.json"

    if cache_path.exists():
        log.debug("xbrl: cache hit %s", cache_path.name)
        return json.loads(cache_path.read_text())

    if not htm_url:
        log.warning("get_financials: empty htm_url for accession %s", accession_no)
        return {}

    try:
        from sec_api import XbrlApi  # noqa: PLC0415
        xbrl_api = XbrlApi(api_key=api_key)
        xbrl_json = xbrl_api.xbrl_to_json(htm_url=htm_url)
    except Exception as exc:
        log.warning("XbrlApi.xbrl_to_json(%s) failed: %s", htm_url, exc)
        return {}

    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(xbrl_json, indent=2, default=str))
    log.info("xbrl cached: %s (%d top-level keys)", cache_path.name, len(xbrl_json))
    return xbrl_json


# ── Internal helpers ──────────────────────────────────────────────────────────

def _usd_m(value: Any) -> float | None:
    """Convert raw XBRL USD value to USD millions, rounded to 1 dp."""
    try:
        v = float(value)
        return round(v / _M, 1)
    except (TypeError, ValueError):
        return None


def _annual_periods(entries: list[dict]) -> list[dict]:
    """
    Filter XBRL entry list to:
      - consolidated entries only (no 'segment' key)
      - annual (12-month) periods only
    Returns entries sorted by period end date ascending, deduplicated by end date.
    """
    from datetime import date as _date

    seen: set[str] = set()
    annual = []
    for e in entries:
        if "segment" in e:
            continue  # skip segment breakdowns, keep consolidated totals only
        start = e.get("period", {}).get("startDate", "")
        end = e.get("period", {}).get("endDate", "")
        if start and end:
            try:
                d_start = _date.fromisoformat(start)
                d_end = _date.fromisoformat(end)
                days = (d_end - d_start).days
                # Annual = approx 12 months (330–395 days)
                if 330 <= days <= 395 and end not in seen:
                    seen.add(end)
                    annual.append(e)
            except ValueError:
                pass
    annual.sort(key=lambda x: x["period"]["endDate"])
    return annual


def _get_concept(
    statement: list[dict] | dict[str, list[dict]], *concept_names: str
) -> dict[str, float | None]:
    """
    Extract a concept value from a XBRL statement entries list or dict.
    Tries each concept_name in order; returns first match.

    Returns dict mapping period_end_date → USD millions value.
    """
    for concept in concept_names:
        if isinstance(statement, dict):
            matches = statement.get(concept, [])
        else:
            matches = [e for e in statement if e.get("concept") == concept]

        annual = _annual_periods(matches)
        if annual:
            return {
                e["period"]["endDate"]: _usd_m(e.get("value"))
                for e in annual
            }
    return {}


def _fiscal_year_label(end_date: str) -> str:
    """
    Convert period endDate (YYYY-MM-DD) to a fiscal year label.
    Uses the calendar year of the end date (e.g. 2025-01-31 → "FY2025").
    """
    try:
        return f"FY{end_date[:4]}"
    except Exception:
        return end_date


# ── Income statement ──────────────────────────────────────────────────────────

def extract_income_statement(
    xbrl_json: dict[str, Any],
    years: int = 5,
) -> list[dict[str, Any]]:
    """
    Parse StatementsOfIncome from XBRL JSON.

    Returns:
        list of dicts sorted by fiscal_year ascending, max `years` entries:
        {
            fiscal_year, period_end, revenue_usd_m, gross_profit_usd_m,
            operating_income_usd_m, net_income_usd_m, rd_expense_usd_m
        }
    """
    statement: list[dict] | dict[str, Any] = xbrl_json.get("StatementsOfIncome", [])
    if not statement:
        log.warning("extract_income_statement: StatementsOfIncome not found in XBRL JSON")
        return []

    # Revenue
    revenue = _get_concept(
        statement,
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "Revenues",
        "SalesRevenueNet",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
    )
    # Gross profit
    gross_profit = _get_concept(statement, "GrossProfit")
    # Operating income
    op_income = _get_concept(
        statement, "OperatingIncomeLoss", "IncomeLossFromOperations"
    )
    # Net income
    net_income = _get_concept(
        statement,
        "NetIncomeLoss",
        "ProfitLoss",
        "NetIncomeLossAvailableToCommonStockholdersBasic",
    )
    # R&D expense
    rd = _get_concept(
        statement,
        "ResearchAndDevelopmentExpense",
        "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost",
    )

    period_ends = sorted(revenue.keys())[-years:]
    rows = []
    for pe in period_ends:
        rows.append(
            {
                "fiscal_year": _fiscal_year_label(pe),
                "period_end": pe,
                "revenue_usd_m": revenue.get(pe),
                "gross_profit_usd_m": gross_profit.get(pe),
                "operating_income_usd_m": op_income.get(pe),
                "net_income_usd_m": net_income.get(pe),
                "rd_expense_usd_m": rd.get(pe),
            }
        )
    return rows


# ── Cash flow statement ───────────────────────────────────────────────────────

def extract_cash_flow(
    xbrl_json: dict[str, Any],
    years: int = 5,
) -> list[dict[str, Any]]:
    """
    Parse StatementsOfCashFlows from XBRL JSON.

    Returns:
        list of dicts sorted by fiscal_year ascending:
        {fiscal_year, period_end, operating_cf_usd_m, capex_usd_m, free_cash_flow_usd_m}
    """
    statement: list[dict] = xbrl_json.get("StatementsOfCashFlows", [])
    if not statement:
        log.warning("extract_cash_flow: StatementsOfCashFlows not found in XBRL JSON")
        return []

    # Operating cash flow
    op_cf = _get_concept(
        statement,
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
    )
    # CapEx (typically negative in XBRL — take absolute value)
    capex_raw = _get_concept(
        statement,
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "CapitalExpendituresIncurredButNotYetPaid",
        "PaymentsForCapitalImprovements",
    )

    period_ends = sorted(op_cf.keys())[-years:]
    rows = []
    for pe in period_ends:
        op = op_cf.get(pe)
        cap = capex_raw.get(pe)
        # CapEx in XBRL is usually reported as positive outflow; subtract from OCF
        fcf: float | None = None
        if op is not None and cap is not None:
            fcf = round(op - abs(cap), 1)
        rows.append(
            {
                "fiscal_year": _fiscal_year_label(pe),
                "period_end": pe,
                "operating_cf_usd_m": op,
                "capex_usd_m": abs(cap) if cap is not None else None,
                "free_cash_flow_usd_m": fcf,
            }
        )
    return rows


# ── Balance sheet ─────────────────────────────────────────────────────────────

def extract_balance_sheet(
    xbrl_json: dict[str, Any],
    years: int = 5,
) -> list[dict[str, Any]]:
    """
    Parse BalanceSheets from XBRL JSON.

    Returns:
        list of dicts sorted by period ascending:
        {fiscal_year, period_end, total_assets_usd_m, total_liabilities_usd_m,
         total_equity_usd_m, cash_usd_m, total_debt_usd_m}
    """
    statement: list[dict] = xbrl_json.get("BalanceSheets", [])
    if not statement:
        log.warning("extract_balance_sheet: BalanceSheets not found in XBRL JSON")
        return []

    assets = _get_concept(statement, "Assets")
    liabilities = _get_concept(statement, "Liabilities")
    equity = _get_concept(
        statement,
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    )
    cash = _get_concept(
        statement,
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsAndShortTermInvestments",
    )
    debt = _get_concept(
        statement,
        "LongTermDebt",
        "LongTermDebtAndCapitalLeaseObligations",
        "LongTermDebtNoncurrent",
    )

    period_ends = sorted(assets.keys())[-years:]
    rows = []
    for pe in period_ends:
        rows.append(
            {
                "fiscal_year": _fiscal_year_label(pe),
                "period_end": pe,
                "total_assets_usd_m": assets.get(pe),
                "total_liabilities_usd_m": liabilities.get(pe),
                "total_equity_usd_m": equity.get(pe),
                "cash_usd_m": cash.get(pe),
                "total_debt_usd_m": debt.get(pe),
            }
        )
    return rows


# ── Convenience: full financial snapshot ──────────────────────────────────────

def build_financials(
    xbrl_json: dict[str, Any],
    years: int = 5,
) -> list[dict[str, Any]]:
    """
    Merge income_statement + cash_flow into a single list of AnnualFinancials-compatible
    dicts, keyed by fiscal_year, sorted ascending.

    Used by runner.py to populate CompanyDataset.financials.
    """
    income = {r["fiscal_year"]: r for r in extract_income_statement(xbrl_json, years)}
    cf = {r["fiscal_year"]: r for r in extract_cash_flow(xbrl_json, years)}

    all_years = sorted(set(income) | set(cf))
    rows = []
    for fy in all_years:
        i = income.get(fy, {})
        c = cf.get(fy, {})
        rev = i.get("revenue_usd_m")
        gp = i.get("gross_profit_usd_m")
        op = i.get("operating_income_usd_m")
        gross_margin = (
            round(gp / rev * 100, 1) if rev and gp and rev != 0 else None
        )
        op_margin = (
            round(op / rev * 100, 1) if rev and op and rev != 0 else None
        )
        rows.append(
            {
                "fiscal_year": fy,
                "revenue_usd_m": rev,
                "gross_profit_usd_m": gp,
                "operating_income_usd_m": op,
                "net_income_usd_m": i.get("net_income_usd_m"),
                "rd_expense_usd_m": i.get("rd_expense_usd_m"),
                "free_cash_flow_usd_m": c.get("free_cash_flow_usd_m"),
                "capex_usd_m": c.get("capex_usd_m"),
                "gross_margin_pct": gross_margin,
                "operating_margin_pct": op_margin,
            }
        )
    return rows
