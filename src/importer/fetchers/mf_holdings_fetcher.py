"""
src/importer/fetchers/mf_holdings_fetcher.py
─────────────────────────────────────────────
Fetches portfolio holdings for each fund in MF_HOLDINGS_WATCHLIST
using the mstarpy library (https://pypi.org/project/mstarpy/).

mstarpy calls Morningstar's internal REST APIs and returns structured JSON,
giving us security name, weight, sector, ISIN, and asset type per holding.

IMPORTANT — mstarpy limitation
───────────────────────────────
mstarpy.Funds.holdings() has NO date/month parameter. It always returns
the CURRENT live snapshot from Morningstar. The `as_of_month` argument is
only used as a label when storing to ClickHouse. To build a genuine
time-series, run this importer once per month going forward.

Public entry point
──────────────────
    from src.importer.fetchers.mf_holdings_fetcher import fetch_holdings
    rows = fetch_holdings(MF_HOLDINGS_WATCHLIST, as_of_month=date.today().replace(day=1))
"""

from __future__ import annotations

import logging
import signal as _signal
import threading
import time
from datetime import date

logger = logging.getLogger(__name__)

# Delay between per-fund API calls to be respectful to Morningstar's servers
_REQUEST_DELAY: float = 1.0

# ── Asset-type classification ──────────────────────────────────────────────────

_EQUITY_KEYWORDS   = ("stock", "equity", "share", "common", "preferred")
_BOND_KEYWORDS     = ("bond", "debt", "fixed income", "debenture", "ncd",
                      "government", "gilt", "treasury", "paper", "deposit")
_GOLD_KEYWORDS     = ("gold", "silver", "precious metal", "commodity etf")
_CASH_KEYWORDS     = ("cash", "money market", "liquid", "overnight", "repo")


def _classify_asset_type(type_id: str, security_name: str) -> str:
    """
    Map mstarpy's holdingTypeId / securityType to our 5-bucket taxonomy.
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
    # mstarpy type_id fallbacks
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
    Fetch portfolio holdings for a single fund via mstarpy.
    Returns a list of row dicts ready for ClickHouse insertion.

    mstarpy.Funds(term, language).holdings() returns a pandas DataFrame
    with columns including: securityName, holdingTypeId, weighting,
    marketValue, isin, sector, holdingType, country, etc.
    """
    try:
        # mstarpy/utils.py calls signal.signal(SIGTERM, ...) at import time,
        # which raises ValueError when imported from a non-main thread
        # (e.g. Streamlit worker). Stub signal.signal for the duration of the
        # import, then restore the real handler immediately after.
        if threading.current_thread() is not threading.main_thread():
            _orig_signal_fn = _signal.signal
            _signal.signal = lambda *a, **kw: None  # type: ignore[assignment]
            try:
                import mstarpy  # type: ignore[import]
            finally:
                _signal.signal = _orig_signal_fn  # type: ignore[assignment]
        else:
            import mstarpy  # type: ignore[import]
    except ImportError as exc:
        raise RuntimeError(
            "mstarpy is not installed. Run: pip install mstarpy"
        ) from exc

    logger.info("Fetching holdings for %s (ISIN=%s, month=%s)",
                fund_name, isin, as_of_month.strftime("%Y-%m"))

    try:
        fund = mstarpy.Funds(term=isin, language="en-gb")
        df = fund.holdings()
    except Exception as exc:
        logger.warning("mstarpy failed for %s (%s): %s", fund_name, isin, exc)
        return []

    if df is None or (hasattr(df, "empty") and df.empty):
        logger.warning("Empty holdings returned for %s", fund_name)
        return []

    rows: list[dict] = []
    for _, h in df.iterrows():
        security_name: str = str(h.get("securityName") or "Unknown")

        # weighting is already in percentage points (e.g. 4.32 = 4.32% of NAV)
        try:
            pct_of_nav = float(h.get("weighting") or 0.0)
        except (TypeError, ValueError):
            pct_of_nav = 0.0

        # holdingTypeId: "Stock", "Bond", "Cash", "Other", etc.
        type_id: str = str(h.get("holdingTypeId") or h.get("holdingType") or "")
        asset_type = _classify_asset_type(type_id, security_name)

        # ISIN of the underlying security (different from the fund ISIN)
        holding_isin: str = str(h.get("isin") or h.get("secId") or "")

        # marketValue is in fund's base currency (INR); convert to crores
        try:
            mv_raw = float(h.get("marketValue") or 0.0)
            market_value_cr = round(mv_raw / 1e7, 4)   # 1 crore = 10^7
        except (TypeError, ValueError):
            market_value_cr = 0.0

        rows.append({
            "scheme_code":      scheme_code,
            "fund_name":        fund_name,
            "as_of_month":      as_of_month,
            "isin":             holding_isin or security_name[:20],
            "security_name":    security_name,
            "asset_type":       asset_type,
            "market_value_cr":  market_value_cr,
            "pct_of_nav":       pct_of_nav,
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
