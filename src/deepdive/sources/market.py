"""
src/deepdive/sources/market.py
───────────────────────────────
Market data and valuation multiples via yfinance.

Uses yfinance directly (not the Indian-equity fetch_yahoo_data() wrapper, which
appends .NS/.BO suffixes and is built for Zerodha symbols).

For each ticker, fetches:
  - Price and volume info (market cap, current price, 52w range)
  - Trailing / forward P/E
  - EV/Revenue, EV/EBITDA
  - Free cash flow, operating cash flow

Computes peer median multiples for P/E, EV/Revenue, EV/EBITDA.

Cache-first: writes market_snapshot.json on first call; reads from disk on re-run.
"""

from __future__ import annotations

import json
import logging
import statistics
from pathlib import Path
from typing import Any

import yfinance as yf

from src.deepdive.models import ValuationSnapshot

log = logging.getLogger(__name__)

# Default Autodesk peer group (same plan.md definition)
ADSK_PEERS: list[str] = ["ADBE", "PTC", "ANSS", "DASTY", "BSY"]


def _safe(value: Any, default: float | None = None) -> float | None:
    """Safely coerce to float; return default on failure or None/NaN."""
    try:
        if value is None:
            return default
        f = float(value)
        return None if f != f else f  # NaN check
    except (TypeError, ValueError):
        return default


def _ticker_snapshot(symbol: str) -> dict[str, Any]:
    """
    Fetch yfinance .info dict for a single US ticker.
    Returns empty dict on failure — never raises.
    """
    try:
        info = yf.Ticker(symbol).info or {}
        return {
            "symbol": symbol,
            "name": info.get("shortName", symbol),
            "market_cap_usd": _safe(info.get("marketCap")),
            "current_price": _safe(info.get("currentPrice") or info.get("regularMarketPrice")),
            "pe_trailing": _safe(info.get("trailingPE")),
            "pe_forward": _safe(info.get("forwardPE")),
            "ev_revenue": _safe(info.get("enterpriseToRevenue")),
            "ev_ebitda": _safe(info.get("enterpriseToEbitda")),
            "free_cash_flow": _safe(info.get("freeCashflow")),
            "revenue_ttm": _safe(info.get("totalRevenue")),
            "fifty_two_week_high": _safe(info.get("fiftyTwoWeekHigh")),
            "fifty_two_week_low": _safe(info.get("fiftyTwoWeekLow")),
        }
    except Exception as exc:
        log.warning("yfinance fetch failed for %s: %s", symbol, exc)
        return {"symbol": symbol}


def _median(values: list[float | None]) -> float | None:
    """Return median of non-None, non-negative values. None if empty."""
    clean = [v for v in values if v is not None and v > 0]
    if not clean:
        return None
    return round(statistics.median(clean), 2)


def fetch_market_snapshot(
    ticker: str,
    peers: list[str],
    cache_path: Path,
    report_date: str,
) -> dict[str, Any]:
    """
    Fetch valuation multiples for ticker + each peer via yfinance.
    Compute peer medians for P/E, EV/Revenue, EV/EBITDA.
    Write to cache_path. Return ValuationSnapshot-compatible dict.

    Args:
        ticker:      Primary ticker (e.g. "ADSK")
        peers:       Peer ticker list (e.g. ADSK_PEERS)
        cache_path:  Path to write market_snapshot.json
        report_date: ISO date string used as as_of_date in ValuationSnapshot

    Returns:
        Dict matching ValuationSnapshot field names plus a "raw" key with
        all per-ticker data.
    """
    if cache_path.exists():
        log.debug("market: cache hit %s", cache_path.name)
        return json.loads(cache_path.read_text())

    log.info("market: fetching %s + %d peers", ticker, len(peers))

    primary = _ticker_snapshot(ticker)
    peer_data = []
    for p in peers:
        snap = _ticker_snapshot(p)
        peer_data.append(snap)
        log.debug("market: %s → P/E %.1f  EV/Rev %.1f",
                  p,
                  snap.get("pe_trailing") or 0,
                  snap.get("ev_revenue") or 0)

    # Peer medians (exclude primary)
    peer_pe = _median([p.get("pe_trailing") for p in peer_data])
    peer_ev_rev = _median([p.get("ev_revenue") for p in peer_data])
    peer_ev_ebitda = _median([p.get("ev_ebitda") for p in peer_data])

    # FCF yield = FCF / market cap
    mc = primary.get("market_cap_usd")
    fcf = primary.get("free_cash_flow")
    fcf_yield = round(fcf / mc * 100, 2) if mc and fcf and mc > 0 else None

    result = {
        "as_of_date": report_date,
        "pe_trailing": primary.get("pe_trailing"),
        "pe_forward": primary.get("pe_forward"),
        "ev_revenue": primary.get("ev_revenue"),
        "ev_ebitda": primary.get("ev_ebitda"),
        "fcf_yield_pct": fcf_yield,
        "market_cap_usd_b": round(mc / 1e9, 2) if mc else None,
        "peer_pe_median": peer_pe,
        "peer_ev_ebitda_median": peer_ev_ebitda,
        "peer_ev_revenue_median": peer_ev_rev,
        # Full raw data for sources.md traceability
        "raw": {
            "primary": primary,
            "peers": peer_data,
        },
    }

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(result, indent=2, default=str))
    log.info(
        "market: %s  P/E=%.1f  EV/Rev=%.1f  FCF yield=%.1f%%  market cap=$%.1fB",
        ticker,
        result.get("pe_trailing") or 0,
        result.get("ev_revenue") or 0,
        result.get("fcf_yield_pct") or 0,
        result.get("market_cap_usd_b") or 0,
    )
    return result


def build_valuation_snapshot(market_data: dict[str, Any]) -> ValuationSnapshot:
    """Convert fetch_market_snapshot() output to a ValuationSnapshot model."""
    return ValuationSnapshot(
        as_of_date=market_data.get("as_of_date", ""),
        pe_trailing=market_data.get("pe_trailing"),
        pe_forward=market_data.get("pe_forward"),
        ev_revenue=market_data.get("ev_revenue"),
        ev_ebitda=market_data.get("ev_ebitda"),
        fcf_yield_pct=market_data.get("fcf_yield_pct"),
        market_cap_usd_b=market_data.get("market_cap_usd_b"),
        peer_pe_median=market_data.get("peer_pe_median"),
        peer_ev_ebitda_median=market_data.get("peer_ev_ebitda_median"),
        peer_ev_revenue_median=market_data.get("peer_ev_revenue_median"),
    )


def fetch_price_history(
    ticker: str,
    cache_path: Path,
    years: int = 2,
) -> list[dict[str, Any]]:
    """
    Fetch 2-year daily OHLCV + volume history for a US ticker via yfinance.

    Cache-first: writes price_history.json on first call.

    Returns:
        List of dicts: {date, open, high, low, close, adj_close, volume}
        Dates are ISO strings (YYYY-MM-DD).  Empty list on failure.
    """
    if cache_path.exists():
        log.debug("market: price_history cache hit %s", cache_path.name)
        return json.loads(cache_path.read_text())

    try:
        import pandas as pd  # noqa: PLC0415
        period = f"{years}y"
        hist = yf.Ticker(ticker).history(period=period, interval="1d", auto_adjust=True)
        if hist.empty:
            log.warning("market: price history empty for %s", ticker)
            return []

        rows = []
        for ts, row in hist.iterrows():
            rows.append({
                "date":      ts.strftime("%Y-%m-%d"),
                "open":      round(float(row["Open"]),   4),
                "high":      round(float(row["High"]),   4),
                "low":       round(float(row["Low"]),    4),
                "close":     round(float(row["Close"]),  4),
                "volume":    int(row.get("Volume", 0)),
            })

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(rows, indent=2))
        log.info("market: price history %s — %d rows (%dy)", ticker, len(rows), years)
        return rows

    except Exception as exc:
        log.warning("market: price_history fetch failed for %s: %s", ticker, exc)
        return []
