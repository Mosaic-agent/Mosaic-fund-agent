"""
src/importer/fetchers/mf_holdings_fetcher.py
─────────────────────────────────────────────
Fetches portfolio holdings for each fund in MF_HOLDINGS_WATCHLIST
by calling the Morningstar sal-service API directly.

Previous approach (mstarpy)
───────────────────────────
The mstarpy library called global.morningstar.com's screener to resolve
ISIN → securityID (requiring Selenium + browser cookies).  That endpoint
is behind AWS WAF and started returning 403 errors.

Current approach (direct API)
─────────────────────────────
1. We store Morningstar securityIDs in a lookup table (ISIN → secID).
2. We call https://api-global.morningstar.com/sal-service/v1/fund/
   portfolio/holding/v2/{secID}/data  with the public API key.
3. No Selenium, no mstarpy, no WAF issues.

Morningstar limitation: holdings() always returns the CURRENT live
snapshot. The `as_of_month` argument is only used as a label.  To build
a genuine time-series, run this importer once per month going forward.

Public entry point
──────────────────
    from src.importer.fetchers.mf_holdings_fetcher import fetch_holdings
    rows = fetch_holdings(MF_HOLDINGS_WATCHLIST, as_of_month=date.today().replace(day=1))
"""

from __future__ import annotations

import logging
import time
from datetime import date

import httpx

logger = logging.getLogger(__name__)

# ── Morningstar API constants ─────────────────────────────────────────────────

_SAL_BASE = "https://api-global.morningstar.com/sal-service/v1"

# Public API key embedded in mstarpy and Morningstar's own JS bundles.
# This is NOT a secret — it is shipped in client-side JavaScript.
_API_KEY = "lstzFDEOhfFNMLikKa0am9mgEKLBl49T"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "apikey": _API_KEY,
}

_PARAMS = {
    "clientId": "MDC",
    "version": "4.71.0",
    "premiumNum": "10000",
    "freeNum": "10000",
}

# Delay between per-fund API calls to be respectful to Morningstar's servers
_REQUEST_DELAY: float = 1.0

# ── ISIN → Morningstar securityID lookup ──────────────────────────────────────
# These IDs are stable; they change only if Morningstar re-indexes a fund.
# To find a securityID for a new fund:
#   1. Visit https://www.morningstar.in and search for the fund
#   2. The URL will contain the securityID, e.g. /funds/.../F00001ABCD/...
#   3. Or use the screener API: global.morningstar.com/api/v1/en-gb/tools/screener/_data
#      with query: "_ ~= '<ISIN>'"  and fields: "isin,name"
#      → result[0]['meta']['securityID']

_ISIN_TO_SEC_ID: dict[str, str] = {
    "INF740KA1TE9": "F00001GOTE",   # DSP Multi Asset Allocation Fund Direct Growth
    "INF966L01580": "F00000PDWV",   # Quant Multi Asset Fund Direct Growth
    "INF109K015K4": "F00000PE3K",   # ICICI Prudential Multi-Asset Fund Direct Growth
    "INF0QA701821": "F00001L0O1",   # Bajaj Finserv Multi Asset Alloc Fund Direct Growth
}

# ── Fallback: resolve ISIN → securityID via lightweight search ────────────────

def _resolve_security_id(isin: str, fund_name: str = "") -> str | None:
    """
    Try to resolve an ISIN to a Morningstar securityID.

    Strategy:
    1. Hardcoded lookup (instant, no network)
    2. morningstar.in autocomplete API (no WAF, no Selenium)
    """
    # Fast path: hardcoded lookup
    if isin in _ISIN_TO_SEC_ID:
        return _ISIN_TO_SEC_ID[isin]

    # Slow path: morningstar.in autocomplete handler (not WAF-protected)
    if fund_name:
        try:
            import xml.etree.ElementTree as ET

            with httpx.Client(timeout=15, follow_redirects=True) as client:
                # The autocomplete returns XML with <ID> tags containing securityIDs
                url = "https://www.morningstar.in/handlers/autocompletehandler.ashx"
                resp = client.get(url, params={"criteria": fund_name}, headers={
                    "User-Agent": _HEADERS["User-Agent"],
                    "Accept": "*/*",
                })
                if resp.status_code == 200 and "<ID>" in resp.text:
                    root = ET.fromstring(resp.text)
                    for table in root.findall("Table"):
                        desc = (table.findtext("Description") or "").lower()
                        sec_id = table.findtext("ID")
                        # Match: "Direct" + "Growth" variant (our ISINs are Direct Growth)
                        if sec_id and "direct" in desc and "growth" in desc:
                            logger.info(
                                "Resolved %s → %s via morningstar.in autocomplete",
                                fund_name, sec_id,
                            )
                            _ISIN_TO_SEC_ID[isin] = sec_id  # cache for session
                            return sec_id
        except Exception as exc:
            logger.debug("morningstar.in lookup failed for %s: %s", fund_name, exc)

    logger.warning(
        "Could not resolve securityID for ISIN %s (%s). "
        "Add it to _ISIN_TO_SEC_ID in mf_holdings_fetcher.py",
        isin, fund_name,
    )
    return None


# ── Asset-type classification ─────────────────────────────────────────────────

_EQUITY_KEYWORDS = ("stock", "equity", "share", "common", "preferred")
_BOND_KEYWORDS   = ("bond", "debt", "fixed income", "debenture", "ncd",
                     "government", "gilt", "treasury", "paper", "deposit")
_GOLD_KEYWORDS   = ("gold", "silver", "precious metal", "commodity etf")
_CASH_KEYWORDS   = ("cash", "money market", "liquid", "overnight", "repo")


def _classify_asset_type(type_id: str, security_name: str) -> str:
    """
    Map Morningstar's holdingTypeId / securityType to our 5-bucket taxonomy.
    Buckets: equity | gold | bond | cash | other
    """
    combined = f"{type_id} {security_name}".lower()
    if any(k in combined for k in _GOLD_KEYWORDS):
        return "gold"
    if any(k in combined for k in _BOND_KEYWORDS):
        return "bond"
    if any(k in combined for k in _CASH_KEYWORDS):
        return "cash"
    if any(k in combined for k in _EQUITY_KEYWORDS):
        return "equity"
    # holdingTypeId fallbacks
    tid = str(type_id).lower()
    if tid in ("stock", "equity", "e"):
        return "equity"
    if tid in ("bond", "fixed income", "fi", "b"):
        return "bond"
    if tid in ("cash", "c"):
        return "cash"
    return "other"


# ── Core fetch ────────────────────────────────────────────────────────────────

def _fetch_one_fund(
    scheme_code: str,
    fund_name: str,
    isin: str,
    as_of_month: date,
) -> list[dict]:
    """
    Fetch portfolio holdings for a single fund via the Morningstar
    sal-service API directly (no mstarpy, no Selenium).

    Returns a list of row dicts ready for ClickHouse insertion.
    """
    sec_id = _resolve_security_id(isin, fund_name)
    if not sec_id:
        logger.warning("Skipping %s (%s): no securityID", fund_name, isin)
        return []

    logger.info(
        "Fetching holdings for %s (ISIN=%s, secID=%s, month=%s)",
        fund_name, isin, sec_id, as_of_month.strftime("%Y-%m"),
    )

    url = f"{_SAL_BASE}/fund/portfolio/holding/v2/{sec_id}/data"

    try:
        with httpx.Client(timeout=30, follow_redirects=True) as client:
            resp = client.get(url, headers=_HEADERS, params=_PARAMS)
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        logger.warning(
            "Morningstar API returned %d for %s (%s): %s",
            exc.response.status_code, fund_name, isin, exc,
        )
        return []
    except Exception as exc:
        logger.warning("Failed to fetch holdings for %s (%s): %s", fund_name, isin, exc)
        return []

    # Parse the holdings response
    # Structure: { "equityHoldingPage": { "holdingList": [...] },
    #              "boldHoldingPage":   { "holdingList": [...] },  # bonds
    #              "otherHoldingPage":  { "holdingList": [...] } }
    rows: list[dict] = []

    for page_key in ("equityHoldingPage", "boldHoldingPage", "otherHoldingPage"):
        page_data = data.get(page_key)
        if not page_data:
            continue
        holding_list = page_data.get("holdingList", [])
        for h in holding_list:
            security_name: str = str(h.get("securityName") or "Unknown")

            # weighting is in percentage points (e.g. 4.32 = 4.32% of NAV)
            try:
                pct_of_nav = float(h.get("weighting") or 0.0)
            except (TypeError, ValueError):
                pct_of_nav = 0.0

            # holdingTypeId: "Stock", "Bond", "Cash", "Other", etc.
            type_id: str = str(h.get("holdingTypeId") or h.get("holdingType") or "")
            asset_type = _classify_asset_type(type_id, security_name)

            # ISIN of the underlying security
            holding_isin: str = str(h.get("isin") or h.get("secId") or "")

            # marketValue is in fund's base currency (INR); convert to crores
            try:
                mv_raw = float(h.get("marketValue") or 0.0)
                market_value_cr = round(mv_raw / 1e7, 4)  # 1 crore = 10^7
            except (TypeError, ValueError):
                market_value_cr = 0.0

            rows.append({
                "scheme_code":     scheme_code,
                "fund_name":       fund_name,
                "as_of_month":     as_of_month,
                "isin":            holding_isin or security_name[:20],
                "security_name":   security_name,
                "asset_type":      asset_type,
                "market_value_cr": market_value_cr,
                "pct_of_nav":      pct_of_nav,
            })

    logger.info("  → %d holdings parsed for %s", len(rows), fund_name)
    return rows


# ── Public entry point ────────────────────────────────────────────────────────

def fetch_holdings(
    watchlist: list[tuple[str, str, str]],
    as_of_month: date,
) -> list[dict]:
    """
    Fetch portfolio holdings for all funds in *watchlist*.

    Parameters
    ----------
    watchlist   : list of (amfi_scheme_code, short_name, isin_growth)
    as_of_month : first day of the target month

    Returns
    -------
    Flat list of row dicts compatible with ClickHouseClient.insert_mf_holdings().
    """
    all_rows: list[dict] = []
    for i, (scheme_code, fund_name, isin) in enumerate(watchlist):
        rows = _fetch_one_fund(scheme_code, fund_name, isin, as_of_month)
        all_rows.extend(rows)
        # Rate-limit: don't hammer Morningstar between funds
        if i < len(watchlist) - 1:
            time.sleep(_REQUEST_DELAY)

    logger.info("Total holdings fetched: %d rows for %d funds", len(all_rows), len(watchlist))
    return all_rows
