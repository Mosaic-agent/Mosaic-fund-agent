"""
src/importer/fetchers/shoonya_fetcher.py
─────────────────────────────────────────
Fetches daily OHLCV data for NSE-listed symbols via the Shoonya (Finvasia)
brokerage API — a reliable alternative to Yahoo Finance for Indian market data.

Session management
──────────────────
Shoonya requires a daily login with TOTP. The session token is cached to
output/.cache/shoonya_session.json and reused until it expires.  Set
SHOONYA_TOTP_SECRET in .env to enable automated TOTP generation via pyotp.

Symbol format
─────────────
NSE equities and ETFs use the "-EQ" segment suffix: MASPTOP50 → MASPTOP50-EQ
Exchange is always "NSE" for cash-market symbols.

Limitations
───────────
Shoonya covers NSE/BSE/MCX listed symbols only.  Global indices (^GSPC),
commodity futures (GC=F), US ETFs (GLD), and FX pairs (USDINR=X) still
require Yahoo Finance — those are not replaced by this fetcher.
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_SESSION_CACHE = Path("output/.cache/shoonya_session.json")
_SESSION_TTL_HOURS = 20  # Shoonya tokens last ~24h; refresh conservatively


def _load_cached_session() -> dict | None:
    """Return cached session dict if it exists and is not stale."""
    try:
        if not _SESSION_CACHE.exists():
            return None
        data = json.loads(_SESSION_CACHE.read_text())
        saved_at = datetime.fromisoformat(data.get("saved_at", "2000-01-01"))
        if datetime.now() - saved_at > timedelta(hours=_SESSION_TTL_HOURS):
            return None
        return data
    except Exception:
        return None


def _save_session(session: dict) -> None:
    _SESSION_CACHE.parent.mkdir(parents=True, exist_ok=True)
    session["saved_at"] = datetime.now().isoformat()
    _SESSION_CACHE.write_text(json.dumps(session))


def get_shoonya_api():
    """
    Return an authenticated ShoonyaApiPy instance.

    Tries the cached session first; falls back to a fresh login using
    credentials from config.settings.  Returns None if Shoonya is not
    configured or login fails.
    """
    try:
        from NorenRestApiPy.NorenApi import NorenApi  # type: ignore

        class ShoonyaApiPy(NorenApi):
            def __init__(self):
                NorenApi.__init__(
                    self,
                    host="https://api.shoonya.com/NorenWClientTP",
                    websocket="wss://api.shoonya.com/NorenWSTP/",
                )
    except ImportError:
        log.debug("NorenRestApiPy not installed — skipping Shoonya")
        return None

    from config.settings import settings

    user    = getattr(settings, "shoonya_user_id",    "")
    pwd     = getattr(settings, "shoonya_password",   "")
    vc      = getattr(settings, "shoonya_vendor_code","")
    secret  = getattr(settings, "shoonya_api_secret", "")
    imei    = getattr(settings, "shoonya_imei",        "")
    totp_s  = getattr(settings, "shoonya_totp_secret", "")

    if not all([user, pwd, vc, secret]):
        log.debug("Shoonya credentials not configured — skipping")
        return None

    api = ShoonyaApiPy()

    # Try cached session
    cached = _load_cached_session()
    if cached and cached.get("susertoken"):
        try:
            api.set_session(userid=user, password=pwd, usertoken=cached["susertoken"])
            log.debug("Shoonya: reused cached session for %s", user)
            return api
        except Exception as exc:
            log.debug("Shoonya: cached session invalid (%s), re-logging in", exc)

    # Resolve 2FA code: static PIN (numeric ≤ 8 digits) or base32 TOTP secret
    totp_code = ""
    if not totp_s:
        log.warning("SHOONYA_TOTP_SECRET not set — cannot automate Shoonya login")
        return None
    if totp_s.strip().isdigit() and len(totp_s.strip()) <= 8:
        # Static PIN — pass through directly
        totp_code = totp_s.strip()
    else:
        try:
            import pyotp  # type: ignore
            totp_code = pyotp.TOTP(totp_s).now()
        except ImportError:
            log.warning("pyotp not installed — cannot generate TOTP for Shoonya login")
            return None
        except Exception as exc:
            log.warning("TOTP generation failed: %s", exc)
            return None

    try:
        ret = api.login(
            userid=user,
            password=pwd,
            twoFA=totp_code,
            vendor_code=vc,
            api_secret=secret,
            imei=imei or "mac",
        )
        if ret and ret.get("stat") == "Ok":
            _save_session({"susertoken": ret["susertoken"]})
            log.info("Shoonya: logged in successfully as %s", user)
            return api
        else:
            log.warning("Shoonya login failed: %s", ret)
            return None
    except Exception as exc:
        log.warning("Shoonya login error: %s", exc)
        return None


def fetch_shoonya_ohlcv(
    symbols: list[tuple[str, str]],  # [(nse_symbol, yahoo_ticker), ...]
    category: str,
    from_date: date,
    to_date: date,
) -> list[dict[str, Any]]:
    """
    Fetch daily OHLCV for NSE-listed symbols via Shoonya get_daily_price_series.

    Accepts the same (nse_symbol, yahoo_ticker) tuple format as yfinance_fetcher
    so it is a transparent drop-in.  Symbols that fail individually are skipped
    and logged; the caller should fall back to yfinance for missing symbols.

    Returns rows with keys: symbol, category, trade_date, open, high, low, close, volume
    """
    api = get_shoonya_api()
    if api is None:
        return []

    start_ts = int(datetime.combine(from_date, datetime.min.time()).timestamp())
    end_ts   = int(datetime.combine(to_date,   datetime.max.time()).timestamp())

    rows: list[dict[str, Any]] = []

    for nse_sym, _yahoo_sym in symbols:
        tradingsymbol = f"{nse_sym}-EQ"
        try:
            data = api.get_daily_price_series(
                exchange="NSE",
                tradingsymbol=tradingsymbol,
                startdate=start_ts,
                enddate=end_ts,
            )
        except Exception as exc:
            log.warning("Shoonya: get_daily_price_series failed for %s: %s", nse_sym, exc)
            continue

        if not data or (isinstance(data, dict) and data.get("stat") != "Ok"):
            log.debug("Shoonya: no data for %s (response: %s)", nse_sym, str(data)[:80])
            continue

        for bar in data:
            if not isinstance(bar, dict):
                continue
            try:
                # Parse date — Shoonya returns "DD-MM-YYYY HH:MM:SS" or "DD-MM-YYYY"
                raw_time = bar.get("time", "")
                trade_date = _parse_shoonya_date(raw_time)
                if trade_date is None:
                    continue
                if not (from_date <= trade_date <= to_date):
                    continue

                close = float(bar.get("intc") or bar.get("c") or 0)
                if close <= 0:
                    continue

                rows.append({
                    "symbol":     nse_sym,
                    "category":   category,
                    "trade_date": trade_date,
                    "open":       float(bar.get("into") or bar.get("o") or close),
                    "high":       float(bar.get("inth") or bar.get("h") or close),
                    "low":        float(bar.get("intl") or bar.get("l") or close),
                    "close":      close,
                    "volume":     float(bar.get("intv") or bar.get("v") or bar.get("volume") or 0),
                })
            except (ValueError, TypeError) as exc:
                log.debug("Shoonya: bad bar for %s: %s — %s", nse_sym, bar, exc)
                continue

        # Be polite — Shoonya has no documented rate limit but avoid hammering
        time.sleep(0.1)

    log.info("Shoonya: fetched %d rows for %s (%s→%s)", len(rows), category, from_date, to_date)
    return rows


def _parse_shoonya_date(raw: str) -> date | None:
    """Parse Shoonya date strings: 'DD-MM-YYYY HH:MM:SS' or 'DD-MM-YYYY'."""
    for fmt in ("%d-%m-%Y %H:%M:%S", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).date()
        except ValueError:
            continue
    return None
