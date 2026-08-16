"""
src/importer/fetchers/nse_corporate_actions_fetcher.py
───────────────────────────────────────────────────────
Fetches historical corporate actions for NSE-listed stocks from the NSE website.

Endpoint:
    https://www.nseindia.com/api/corporates-corporateActions
    ?index=equities&symbol=<SYMBOL>

Returns all historical events: splits, bonuses, dividends, demergers, rights,
face-value reductions, buybacks, AGM/EGM notices.

Only price-impacting action types are stored (splits, bonuses, demergers,
rights, face-value splits). Dividends stored separately — large special
dividends can cause >5% price moves but regular quarterly dividends should
not suppress anomaly detection.

Output schema (for `market_data.corporate_actions`):
    symbol       String
    ex_date      Date
    action_type  LowCardinality(String)   -- split | bonus | demerger | rights | dividend | other
    ratio        String                   -- e.g. "1:1", "1:2", "Rs 0.25"
    purpose      String                   -- raw purpose string from NSE
    series       String                   -- "EQ" almost always
    face_val     Float64
    record_date  Date
    source       String DEFAULT 'nse'
    imported_at  DateTime DEFAULT now()
"""

from __future__ import annotations

import logging
import re
import threading
import time
from datetime import date
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_NSE_CA_URL  = "https://www.nseindia.com/api/corporates-corporateActions"
_NSE_WARMUP  = "https://www.nseindia.com/get-quotes/equity"
# Full browser-equivalent header set + HTTP/2 (both required — NSE's Akamai bot
# manager 403s a plain HTTP/1.1 client with a minimal header set even though the
# underlying API has real data; verified working manually against
# corporates-corporateActions and top-corp-info before landing this).
_NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":            "application/json, text/plain, */*",
    "Accept-Language":   "en-US,en;q=0.9",
    "Accept-Encoding":   "gzip, deflate, br",
    "Connection":        "keep-alive",
    "Sec-Fetch-Dest":    "empty",
    "Sec-Fetch-Mode":    "cors",
    "Sec-Fetch-Site":    "same-origin",
}
_TIMEOUT = 15

# ── Warmed-up session reuse ────────────────────────────────────────────────────
# NSE's session cookies (nsit/ak_bmsc/bm_sz) are valid for ~2h, so re-warming
# (extra GET + 0.8s sleep) on every single symbol lookup is pure waste once more
# than one symbol is queried in the same process. Cache one warmed client and
# reuse it; per-symbol Referer is passed per-request instead of baked into the
# client so the cached client stays symbol-agnostic.
_SESSION_TTL = 1800  # 30 min — conservative vs NSE's ~2h cookie expiry
_session_lock = threading.Lock()
_cached_client: httpx.Client | None = None
_cached_client_ts: float = 0.0


def _get_warmed_client() -> httpx.Client:
    global _cached_client, _cached_client_ts
    with _session_lock:
        now = time.monotonic()
        if _cached_client is not None and (now - _cached_client_ts) < _SESSION_TTL:
            return _cached_client
        if _cached_client is not None:
            _cached_client.close()
        client = httpx.Client(
            headers=_NSE_HEADERS,
            follow_redirects=True,
            timeout=_TIMEOUT,
            http2=True,
        )
        client.get(_NSE_WARMUP, params={"symbol": "NIFTY"}, timeout=10)
        time.sleep(0.8)
        _cached_client = client
        _cached_client_ts = now
        return client


def _invalidate_cached_client() -> None:
    global _cached_client, _cached_client_ts
    with _session_lock:
        if _cached_client is not None:
            _cached_client.close()
        _cached_client = None
        _cached_client_ts = 0.0

# Action types whose ex-dates suppress anomaly detection (price jumps on these
# dates are mechanical, not informational).
PRICE_IMPACTING_TYPES = {"split", "bonus", "demerger", "rights", "face_value_split"}


def _classify_action(purpose: str) -> str:
    """
    Map NSE `purpose` string to a normalised action_type.

    NSE purpose examples:
      "BONUS 1:1"
      "STOCK SPLIT - From Rs 2/- To Re 1/-"
      "Face Value Split (Sub-Division) - From Rs 10/- To Rs 1/-"
      "Demerger"
      "Rights 3:17 at Rs 835"
      "Annual General Meeting/Dividend - Rs 0.25 Per Share"
      "Interim Dividend - Rs 1.50 Per Share"
      "Buyback"
    """
    p = purpose.lower()
    if "demerger" in p or "de-merger" in p:
        return "demerger"
    if "face value split" in p or "sub-division" in p or "sub division" in p:
        return "face_value_split"
    if "split" in p and "stock" in p:
        return "split"
    if "bonus" in p:
        return "bonus"
    if "rights" in p:
        return "rights"
    if "dividend" in p:
        return "dividend"
    if "buyback" in p or "buy-back" in p or "buy back" in p:
        return "buyback"
    return "other"


def _parse_date(s: str) -> date | None:
    """Parse NSE date strings: '14-Jul-2025', '14-07-2025', '2025-07-14'."""
    if not s or s.strip() == "-":
        return None
    s = s.strip()
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            from datetime import datetime
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    logger.debug("Could not parse date string: %r", s)
    return None


def _extract_ratio(purpose: str) -> str:
    """Pull ratio or amount from the purpose string (best-effort)."""
    # Match patterns like "1:1", "3:17", "Rs 0.25", "Re 1", "2:10"
    m = re.search(r"(\d+\s*:\s*\d+)", purpose)
    if m:
        return m.group(1).replace(" ", "")
    m = re.search(r"Rs?\s*([\d.]+)", purpose, re.IGNORECASE)
    if m:
        return f"Rs {m.group(1)}"
    return ""


def fetch_corporate_actions(symbol: str) -> list[dict[str, Any]]:
    """
    Fetch all historical corporate actions for an NSE-listed symbol.

    Returns a list of dicts ready for ClickHouse insertion:
        symbol, ex_date, action_type, ratio, purpose,
        series, face_val, record_date, source
    """
    symbol_upper = symbol.strip().upper()
    rows: list[dict[str, Any]] = []
    data = None
    referer = {"Referer": f"{_NSE_WARMUP}?symbol={symbol_upper}"}

    for attempt in (1, 2):
        try:
            client = _get_warmed_client()
            resp = client.get(
                _NSE_CA_URL,
                params={"index": "equities", "symbol": symbol_upper},
                headers=referer,
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            break

        except Exception as exc:
            logger.warning(
                "NSE corporate actions fetch failed for %s (attempt %d): %s",
                symbol_upper, attempt, exc,
            )
            # The cached session may have gone stale (expired cookies, one-off
            # 403) — drop it and retry once with a freshly warmed client before
            # falling through to the yfinance fallback below.
            _invalidate_cached_client()

    if isinstance(data, list):
        for item in data:
            # NSE's corporateActions JSON uses "subject" and "recDate" (not the
            # "purpose"/"recordDate" this fetcher originally expected — a schema
            # drift that silently produced 0 rows for every symbol and fell through
            # to the yfinance fallback below). Keep the old keys as a fallback in
            # case NSE reverts or another endpoint variant uses them.
            purpose    = str(item.get("subject") or item.get("purpose") or "").strip()
            ex_raw     = str(item.get("exDate") or item.get("ex_date") or "")
            rec_raw    = str(item.get("recDate") or item.get("recordDate") or item.get("record_date") or "")
            series     = str(item.get("series") or "EQ").strip()
            face_val_s = str(item.get("faceVal") or item.get("face_val") or "0")

            ex_date  = _parse_date(ex_raw)
            rec_date = _parse_date(rec_raw)

            if ex_date is None or not purpose:
                continue

            try:
                face_val = float(str(face_val_s).replace(",", ""))
            except ValueError:
                face_val = 0.0

            rows.append({
                "symbol":      symbol_upper,
                "ex_date":     ex_date,
                "action_type": _classify_action(purpose),
                "ratio":       _extract_ratio(purpose),
                "purpose":     purpose[:500],
                "series":      series,
                "face_val":    face_val,
                "record_date": rec_date or ex_date,
                "source":      "nse",
            })

    if not rows:
        logger.info(
            "NSE corporate actions returned no data for %s. Trying yfinance fallback...",
            symbol_upper,
        )
        try:
            import yfinance as yf
            yf_symbol = f"{symbol_upper}.NS"
            ticker = yf.Ticker(yf_symbol)
            actions = ticker.actions
            if actions is not None and not actions.empty:
                for dt, row in actions.iterrows():
                    ex_date = dt.date()
                    div = float(row.get("Dividends", 0.0))
                    split = float(row.get("Stock Splits", 0.0))

                    if div > 0:
                        rows.append({
                            "symbol":      symbol_upper,
                            "ex_date":     ex_date,
                            "action_type": "dividend",
                            "ratio":       f"Rs {div}",
                            "purpose":     f"Dividend Rs {div} (yfinance fallback)",
                            "series":      "EQ",
                            "face_val":    0.0,
                            "record_date": ex_date,
                            "source":      "yfinance",
                        })
                    if split > 0 and split != 1.0:
                        rows.append({
                            "symbol":      symbol_upper,
                            "ex_date":     ex_date,
                            "action_type": "split",
                            "ratio":       str(split),
                            "purpose":     f"Stock Split / Bonus ratio {split} (yfinance fallback)",
                            "series":      "EQ",
                            "face_val":    0.0,
                            "record_date": ex_date,
                            "source":      "yfinance",
                        })
        except Exception as yf_exc:
            logger.warning("yfinance corporate actions fallback failed for %s: %s", symbol_upper, yf_exc)

    logger.info(
        "NSE corporate actions: %d events fetched for %s", len(rows), symbol_upper
    )
    return rows
