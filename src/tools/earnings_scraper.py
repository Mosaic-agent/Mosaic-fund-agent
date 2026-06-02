"""
src/tools/earnings_scraper.py
──────────────────────────────
LangChain tool for fetching quarterly earnings and financial results
for Indian companies (NSE/BSE) using free web sources.

Data sources (in priority order):
  1. Screener.in – structured financial data, no auth required
  2. BSE Corporate Announcements – official exchange filings
  3. Yahoo Finance quarterly financials fallback

All scraping uses a polite delay (SCRAPE_DELAY_SECONDS from config)
and a rotating user-agent to avoid being blocked.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any

import requests
from bs4 import BeautifulSoup
from langchain_core.tools import tool

from config.settings import settings
from src.models.portfolio import QuarterlyResult

logger = logging.getLogger(__name__)

# ── HTTP headers for web scraping ─────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

SCREENER_BASE = "https://www.screener.in"


# ── Screener.in scraper ───────────────────────────────────────────────────────

def _parse_number(text: str) -> float:
    """Parse Indian number format like '1,23,456.78' → 123456.78"""
    cleaned = re.sub(r"[^\d.\-]", "", text.replace(",", ""))
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def fetch_from_screener(symbol: str) -> QuarterlyResult | None:
    """
    Scrape the latest quarterly results from Screener.in for an NSE symbol.

    Args:
        symbol: NSE trading symbol e.g. 'RELIANCE', 'TCS', 'INFY'

    Returns:
        QuarterlyResult if data found, None on failure.
    """
    time.sleep(settings.scrape_delay_seconds)

    url = f"{SCREENER_BASE}/company/{symbol}/consolidated/"
    logger.info("Scraping Screener.in for %s: %s", symbol, url)

    def _fetch_and_parse(page_url: str):
        """Fetch a Screener.in URL and return (soup, rows) or (None, None)."""
        try:
            r = requests.get(page_url, headers=HEADERS, timeout=15)
        except Exception as exc:
            logger.warning("Screener.in request failed for %s: %s", page_url, exc)
            return None, None
        if r.status_code != 200:
            return None, None
        s = BeautifulSoup(r.text, "lxml")
        qs = s.find("section", {"id": "quarters"})
        if not qs:
            return None, None
        t = qs.find("table")
        if not t:
            return None, None
        tr = t.find_all("tr")
        # Guard: a valid table has data cells (>1 cell per row).
        # Consolidated pages sometimes return 200 but contain only label
        # cells with no data — treat that as a miss and fall through.
        data_rows = [r for r in tr[1:] if len(r.find_all(["th", "td"])) > 1]
        if not data_rows:
            return None, None
        return s, tr

    try:
        soup, rows = _fetch_and_parse(url)

        # Fall through to standalone URL if consolidated had no data
        if rows is None:
            url = f"{SCREENER_BASE}/company/{symbol}/"
            logger.info("Screener.in: consolidated empty for %s — trying standalone", symbol)
            soup, rows = _fetch_and_parse(url)

        if rows is None:
            logger.warning("No quarterly data found on Screener.in for %s", symbol)
            return None

        # Header row: Mar 2024, Jun 2024, Sep 2024, Dec 2024 ...
        header_row = rows[0]
        headers = [th.get_text(strip=True) for th in header_row.find_all(["th", "td"])]

        # Get the latest quarter (last column)
        latest_period = headers[-1] if len(headers) > 1 else ""

        def get_latest_value(label_pattern: str) -> float:
            """Find a row by label and return the latest (last) column value."""
            for row in rows[1:]:
                cells = row.find_all(["th", "td"])
                if not cells:
                    continue
                row_label = cells[0].get_text(strip=True).lower()
                if re.search(label_pattern, row_label, re.IGNORECASE):
                    if len(cells) > 1:
                        return _parse_number(cells[-1].get_text(strip=True))
            return 0.0

        def get_prev_value(label_pattern: str) -> float:
            """Return second-to-last column value (YoY comparison)."""
            for row in rows[1:]:
                cells = row.find_all(["th", "td"])
                if not cells:
                    continue
                row_label = cells[0].get_text(strip=True).lower()
                if re.search(label_pattern, row_label, re.IGNORECASE):
                    # YoY: 4 columns back from latest (same quarter last year)
                    if len(cells) > 5:
                        return _parse_number(cells[-5].get_text(strip=True))
                    elif len(cells) > 2:
                        return _parse_number(cells[-2].get_text(strip=True))
            return 0.0

        # Extract key financials
        revenue = get_latest_value(r"sales|revenue|net sales")
        net_profit = get_latest_value(r"net profit|profit after tax|pat")
        eps = get_latest_value(r"eps|earning per share")

        revenue_prev = get_prev_value(r"sales|revenue|net sales")
        profit_prev = get_prev_value(r"net profit|profit after tax|pat")
        eps_prev = get_prev_value(r"eps|earning per share")

        # YoY calculations
        revenue_yoy = (
            ((revenue - revenue_prev) / revenue_prev * 100) if revenue_prev else 0.0
        )
        profit_yoy = (
            ((net_profit - profit_prev) / profit_prev * 100) if profit_prev else 0.0
        )
        eps_yoy = (
            ((eps - eps_prev) / eps_prev * 100) if eps_prev else 0.0
        )

        return QuarterlyResult(
            period=latest_period,
            revenue_cr=round(revenue, 2),
            net_profit_cr=round(net_profit, 2),
            eps=round(eps, 2),
            revenue_yoy_pct=round(revenue_yoy, 2),
            profit_yoy_pct=round(profit_yoy, 2),
            eps_yoy_pct=round(eps_yoy, 2),
            source_url=url,
        )

    except Exception as exc:
        logger.warning("Screener.in scrape failed for %s: %s", symbol, exc)
        return None


def fetch_from_yahoo_financials(symbol: str, exchange: str = "NSE") -> QuarterlyResult | None:
    """
    Fallback: fetch quarterly financials from Yahoo Finance.

    Args:
        symbol:   NSE trading symbol
        exchange: 'NSE' or 'BSE'
    """
    try:
        import yfinance as yf

        if exchange.upper() in ("US", "NASDAQ", "NYSE"):
            yf_symbol = symbol
        else:
            suffix = settings.bse_suffix if exchange.upper() == "BSE" else settings.nse_suffix
            yf_symbol = f"{symbol}{suffix}"
        ticker = yf.Ticker(yf_symbol)

        # Quarterly income statement
        qf = ticker.quarterly_financials
        if qf is None or qf.empty:
            return None

        # Latest quarter is the first column
        latest_col = qf.columns[0]
        period_label = str(latest_col.date()) if hasattr(latest_col, "date") else str(latest_col)

        # Retrieve values using the original row labels
        # (Total Revenue/Net Income are standard labels yfinance returns)
        revenue = _safe_get(qf, "Total Revenue", latest_col)
        net_income = _safe_get(qf, "Net Income", latest_col)

        # Get prev year same quarter for YoY (column index 4 ≈ same Q last year)
        prev_col = qf.columns[4] if len(qf.columns) > 4 else None
        revenue_prev = _safe_get(qf, "Total Revenue", prev_col) if prev_col else 0
        income_prev = _safe_get(qf, "Net Income", prev_col) if prev_col else 0

        # Convert from absolute currency value (Yahoo Finance returns absolute values)
        def to_cr(val: float) -> float:
            if exchange.upper() in ("US", "NASDAQ", "NYSE"):
                # Scale USD absolute values to Millions
                return round(val / 1e6, 2) if val else 0.0
            # Scale INR absolute values to Crores
            return round(val / 1e7, 2) if val else 0.0

        eps = _safe_get(qf, "Basic EPS", latest_col) or _safe_get(qf, "Diluted EPS", latest_col)
        eps_prev = _safe_get(qf, "Basic EPS", prev_col) or _safe_get(qf, "Diluted EPS", prev_col) if prev_col else 0.0

        revenue_yoy = (
            ((revenue - revenue_prev) / abs(revenue_prev) * 100) if revenue_prev else 0.0
        )
        profit_yoy = (
            ((net_income - income_prev) / abs(income_prev) * 100) if income_prev else 0.0
        )
        eps_yoy = (
            ((eps - eps_prev) / abs(eps_prev) * 100) if eps_prev else 0.0
        )

        return QuarterlyResult(
            period=period_label,
            revenue_cr=to_cr(revenue),
            net_profit_cr=to_cr(net_income),
            eps=round(eps, 2),
            revenue_yoy_pct=round(revenue_yoy, 2),
            profit_yoy_pct=round(profit_yoy, 2),
            eps_yoy_pct=round(eps_yoy, 2),
            source_url=f"https://finance.yahoo.com/quote/{yf_symbol}/financials",
        )

    except Exception as exc:
        logger.warning("Yahoo Finance financials fallback failed for %s: %s", symbol, exc)
        return None


def _safe_get(df: Any, row_label: str, col: Any) -> float:
    """Safely retrieve a value from a yfinance DataFrame."""
    try:
        if row_label in df.index:
            val = df.loc[row_label, col]
            return float(val) if val == val else 0.0  # NaN guard
    except Exception:
        pass
    return 0.0


# ── LangChain Tool ────────────────────────────────────────────────────────────

@tool
def get_quarterly_results(input_str: str) -> dict[str, Any]:
    """
    Fetch the latest quarterly financial results for an Indian company.

    Tries Screener.in first (best for Indian companies), then falls back
    to Yahoo Finance quarterly financials. Both sources are free.

    Input format: "SYMBOL" or "SYMBOL:EXCHANGE"
    Examples:
      "RELIANCE"      → NSE:RELIANCE results from Screener.in
      "HDFCBANK:NSE"  → NSE:HDFCBANK results

    Returns revenue (₹ Crore), net profit (₹ Crore), EPS,
    YoY growth percentages, and data source URL.
    """
    parts = input_str.strip().split(":")
    symbol = parts[0].strip().upper()
    exchange = parts[1].strip().upper() if len(parts) > 1 else "NSE"

    # Priority 1: Screener.in
    result = fetch_from_screener(symbol)

    # Priority 2: Yahoo Finance fallback
    if result is None:
        logger.info("Falling back to Yahoo Finance financials for %s", symbol)
        result = fetch_from_yahoo_financials(symbol, exchange)

    if result is None:
        return {
            "symbol": symbol,
            "error": "Could not fetch quarterly results from any source.",
            "sources_tried": ["screener.in", "yahoo_finance"],
        }

    return {
        "symbol": symbol,
        "period": f"Quarter Ending {result.period}",
        "note": "These are quarterly financial results (not annual/FY figures).",
        "revenue_cr": result.revenue_cr,
        "net_profit_cr": result.net_profit_cr,
        "eps": result.eps,
        "revenue_yoy_pct": result.revenue_yoy_pct,
        "profit_yoy_pct": result.profit_yoy_pct,
        "eps_yoy_pct": result.eps_yoy_pct,
        "guidance": result.guidance,
        "source_url": result.source_url,
    }


@tool
def get_shareholding_pattern(symbol: str) -> dict:
    """
    Fetch the latest quarterly shareholding pattern for an Indian NSE/BSE stock
    from Screener.in. Returns Promoter, FII, DII, Government, and Public/Retail
    holding percentages plus QoQ change for each category.

    Args:
        symbol: NSE symbol — e.g. "HDFCBANK", "RELIANCE", "LICI"

    Returns:
        Dict with latest_quarter, promoter_pct, fii_pct, dii_pct,
        government_pct, public_pct, num_shareholders, and QoQ deltas.
    """
    sym = symbol.strip().upper()
    for path in (f"/company/{sym}/consolidated/", f"/company/{sym}/"):
        try:
            resp = requests.get(
                f"{SCREENER_BASE}{path}",
                headers=HEADERS,
                timeout=15,
            )
            if resp.status_code != 200:
                continue

            soup = BeautifulSoup(resp.text, "lxml")
            sh_section = soup.find("section", {"id": "shareholding"})
            if not sh_section:
                continue

            div = sh_section.find("div", {"id": "quarterly-shp"})
            if not div:
                continue

            table = div.find("table")
            if not table:
                continue

            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
            latest_quarter = headers[-1] if headers else ""

            def _pct(text: str) -> float:
                cleaned = text.replace("%", "").strip()
                try:
                    return round(float(cleaned), 2)
                except ValueError:
                    return 0.0

            result: dict = {"symbol": sym, "latest_quarter": latest_quarter, "source": f"{SCREENER_BASE}{path}"}
            label_map = {
                "promoters":     "promoter_pct",
                "fiis":          "fii_pct",
                "diis":          "dii_pct",
                "government":    "government_pct",
                "public":        "public_pct",
            }
            for row in rows[1:]:
                cells = row.find_all(["th", "td"])
                if len(cells) < 2:
                    continue
                raw_label = cells[0].get_text(strip=True).lower().replace("+", "").strip()
                # Number of shareholders — store separately
                if "shareholders" in raw_label or "no. of" in raw_label:
                    result["num_shareholders"] = cells[-1].get_text(strip=True)
                    continue
                for key, field in label_map.items():
                    if key in raw_label:
                        latest_val = _pct(cells[-1].get_text(strip=True))
                        prev_val   = _pct(cells[-2].get_text(strip=True)) if len(cells) > 2 else 0.0
                        result[field]               = latest_val
                        result[f"{field}_prev"]     = prev_val
                        result[f"{field}_qoq_delta"] = round(latest_val - prev_val, 2)
                        break

            if any(k in result for k in ("fii_pct", "dii_pct", "public_pct")):
                logger.info("Shareholding fetched for %s: FII=%.2f%% DII=%.2f%% Public=%.2f%%",
                            sym, result.get("fii_pct", 0), result.get("dii_pct", 0), result.get("public_pct", 0))
                return result

        except Exception as exc:
            logger.warning("Shareholding scrape failed for %s at %s: %s", sym, path, exc)

    return {"symbol": sym, "error": "Shareholding data not found on Screener.in"}


# Convenience list
EARNINGS_TOOLS = [get_quarterly_results, get_shareholding_pattern]
