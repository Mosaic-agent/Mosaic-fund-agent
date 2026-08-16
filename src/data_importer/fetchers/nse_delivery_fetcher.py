"""
src/data_importer/fetchers/nse_delivery_fetcher.py
────────────────────────────────────────────────────
Fetches NSE's daily "Security-wise Delivery Position" report — delivery
quantity and % delivery-to-traded-quantity for every listed security — the
standard Indian-market proxy for institutional-vs-speculative volume (intraday
/ algo volume doesn't result in a delivery, so a high delivery % on a volume
spike is the classic "quiet accumulation" signature).

Endpoint (bulk daily bhavcopy, ALL symbols in one file):
    https://archives.nseindia.com/products/content/sec_bhavdata_full_{DDMMYYYY}.csv

Unlike www.nseindia.com's JSON APIs (see nse_corporate_actions_fetcher.py),
archives.nseindia.com does NOT require session warming / HTTP2 / bot-detection
headers — verified live: a plain httpx GET with a standard User-Agent returns
200 with real CSV data.

The file's own DATE1 column is the authoritative trade date — NOT the DDMMYYYY
in the URL. On weekends/holidays NSE keeps serving the last trading day's file
under the current date's URL rather than 404ing, so the URL date and DATE1 can
diverge; duplicate (symbol, trade_date, series) rows across two URL attempts
are harmless (ReplacingMergeTree dedupes on insert).

Column quirks (verified live, 2026-08-16):
    - Header/values have leading spaces (" SYMBOL", " DELIV_PER", ...) — must
      strip.
    - DELIV_QTY / DELIV_PER are "-" for some non-equity series (debt/G-Secs) —
      parsed as None (SQL NULL), not 0, so "not published" is distinguishable
      from "0% delivered".

Output schema (for market_data.nse_delivery):
    trade_date, symbol, series, prev_close, open_price, high_price, low_price,
    last_price, close_price, avg_price, ttl_trd_qty, turnover_lacs,
    no_of_trades, deliv_qty, deliv_per, source="nse"
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from io import StringIO
from typing import Any

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

_BHAVCOPY_URL = "https://archives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv"
_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
_TIMEOUT = 20

_INT_COLS = {"ttl_trd_qty": "TTL_TRD_QNTY", "no_of_trades": "NO_OF_TRADES"}
_FLOAT_COLS = {
    "prev_close":    "PREV_CLOSE",
    "open_price":    "OPEN_PRICE",
    "high_price":    "HIGH_PRICE",
    "low_price":     "LOW_PRICE",
    "last_price":    "LAST_PRICE",
    "close_price":   "CLOSE_PRICE",
    "avg_price":     "AVG_PRICE",
    "turnover_lacs": "TURNOVER_LACS",
}


def _parse_nullable_number(value: str, cast) -> Any:
    """NSE stamps '-' for delivery fields on series where it isn't published
    (e.g. debt/G-Secs) — that must become NULL, not 0, so callers can tell
    'not published' apart from a genuine 0% delivery day."""
    text = str(value).strip()
    if not text or text == "-":
        return None
    try:
        return cast(text)
    except ValueError:
        return None


def _fetch_one_day(client: httpx.Client, day: date) -> list[dict[str, Any]]:
    url = _BHAVCOPY_URL.format(ddmmyyyy=day.strftime("%d%m%Y"))
    try:
        resp = client.get(url, timeout=_TIMEOUT)
    except Exception as exc:
        logger.debug("NSE bhavcopy request failed for %s: %s", day, exc)
        return []

    if resp.status_code != 200 or not resp.content:
        logger.debug("NSE bhavcopy unavailable for %s (status=%s) — skipping", day, resp.status_code)
        return []

    try:
        df = pd.read_csv(StringIO(resp.content.decode("utf-8", errors="replace")))
    except Exception as exc:
        logger.warning("NSE bhavcopy CSV parse failed for %s: %s", day, exc)
        return []

    df.columns = [c.strip() for c in df.columns]
    if "SYMBOL" not in df.columns or "DATE1" not in df.columns:
        logger.warning("NSE bhavcopy for %s missing expected columns: %s", day, list(df.columns))
        return []

    rows: list[dict[str, Any]] = []
    for _, r in df.iterrows():
        trade_date_str = str(r["DATE1"]).strip()
        try:
            trade_date = pd.to_datetime(trade_date_str, format="%d-%b-%Y").date()
        except ValueError:
            continue

        row: dict[str, Any] = {
            "trade_date": trade_date,
            "symbol":     str(r["SYMBOL"]).strip(),
            "series":     str(r.get("SERIES", "EQ")).strip() or "EQ",
            "source":     "nse",
        }
        for out_key, csv_col in _FLOAT_COLS.items():
            row[out_key] = _parse_nullable_number(r.get(csv_col, "-"), float) or 0.0
        for out_key, csv_col in _INT_COLS.items():
            row[out_key] = int(_parse_nullable_number(r.get(csv_col, "-"), float) or 0)
        row["deliv_qty"] = _parse_nullable_number(r.get("DELIV_QTY", "-"), lambda v: int(float(v)))
        row["deliv_per"] = _parse_nullable_number(r.get("DELIV_PER", "-"), float)
        rows.append(row)

    return rows


def fetch_nse_delivery(from_date: date, to_date: date) -> list[dict[str, Any]]:
    """
    Fetch NSE's daily delivery-position bhavcopy for every calendar day in
    [from_date, to_date] (inclusive). Weekends/holidays return no data for
    that URL and are silently skipped — never raises.
    """
    rows: list[dict[str, Any]] = []
    seen_dates: set[date] = set()

    with httpx.Client(headers=_HEADERS, follow_redirects=True, timeout=_TIMEOUT) as client:
        day = from_date
        while day <= to_date:
            day_rows = _fetch_one_day(client, day)
            for r in day_rows:
                if r["trade_date"] not in seen_dates:
                    rows.append(r)
            if day_rows:
                seen_dates.add(day_rows[0]["trade_date"])
            day += timedelta(days=1)

    logger.info(
        "NSE delivery: %d rows fetched across %d trading day(s) (%s→%s)",
        len(rows), len(seen_dates), from_date, to_date,
    )
    return rows
