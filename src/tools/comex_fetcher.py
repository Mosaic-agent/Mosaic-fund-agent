"""
src/tools/comex_fetcher.py
───────────────────────────
Fetches live COMEX commodity prices from gold-api.com and compares them to
the previous trading day's close to generate BULLISH / BEARISH / NEUTRAL
signals before the Indian market opens (NSE opens at 09:15 IST).

Primary source  : gold-api.com  — live spot prices (XAU, XAG, XPT, XPD, HG)
Previous close  : Yahoo Finance futures tickers (GC=F, SI=F, PL=F, PA=F, HG=F)
                  (gold-api.com has no history endpoint — yfinance fills the gap)

Signal thresholds:
  STRONG BULLISH  : day change > +1.0 %
  BULLISH         : day change > +0.3 %
  NEUTRAL         : day change within ±0.3 %
  BEARISH         : day change < −0.3 %
  STRONG BEARISH  : day change < −1.0 %

NSE ETF impact map — which Indian ETFs are directly affected by each commodity:
  XAU  →  GOLDBEES, KOTAKGOLD, HDFCGOLD, ICICIGOLD, NIPPON GOLDBEES
  XAG  →  SILVERBEES
  HG   →  Vedanta (VEDL), Hindalco (HINDALCO) — indirect

Pre-market window: run before 09:15 IST to get a "before-open" signal.

Prompt-Injection Protection
────────────────────────────
External API fields (name, symbol, updatedAt) are validated before use:
  • symbol  : must be a known COMEX symbol (whitelist)
  • name    : must be a short plain string (≤ 50 chars, no special sequences)
  • price   : must be a positive float
  • updatedAt: must parse as ISO-8601 UTC datetime
Any field that fails validation is replaced with a safe placeholder so that
adversarially crafted API responses cannot inject instructions into the LLM
or corrupt downstream output.

[SENSITIVE] GOLD_API_KEY must be set in .env — never hard-coded here.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Optional

import requests
import yfinance as yf

from config.settings import settings

logger = logging.getLogger(__name__)

# ── Commodity catalogue ───────────────────────────────────────────────────────

# [NON-SENSITIVE] gold-api.com symbol list (from /symbols endpoint, static copy)
_COMEX_SYMBOLS: dict[str, dict] = {
    "XAU": {
        "name":          "Gold",
        "unit":          "USD/troy oz",
        "yahoo_ticker":  "GC=F",          # COMEX Gold futures
        "nse_etfs":      ["GOLDBEES", "KOTAKGOLD", "HDFCGOLD", "ICICIGOLD"],
        "emoji":         "🥇",
    },
    "XAG": {
        "name":          "Silver",
        "unit":          "USD/troy oz",
        "yahoo_ticker":  "SI=F",          # COMEX Silver futures
        "nse_etfs":      ["SILVERBEES"],
        "emoji":         "🥈",
    },
    "XPT": {
        "name":          "Platinum",
        "unit":          "USD/troy oz",
        "yahoo_ticker":  "PL=F",          # COMEX Platinum futures
        "nse_etfs":      [],
        "emoji":         "⚪",
    },
    "XPD": {
        "name":          "Palladium",
        "unit":          "USD/troy oz",
        "yahoo_ticker":  "PA=F",          # COMEX Palladium futures
        "nse_etfs":      [],
        "emoji":         "⚡",
    },
    "HG": {
        "name":          "Copper",
        "unit":          "USD/lb",
        "yahoo_ticker":  "HG=F",          # COMEX Copper futures
        "nse_etfs":      ["VEDL", "HINDALCO"],   # indirect exposure
        "emoji":         "🔷",
    },
}

# Default set fetched on every run (skip cryptos — not COMEX)
_DEFAULT_FETCH: list[str] = ["XAU", "XAG", "XPT", "XPD", "HG"]

# Signal thresholds (%)
_STRONG_BULL = 1.0
_BULL        = 0.3
_BEAR        = -0.3
_STRONG_BEAR = -1.0

# ── Prompt-injection protection ───────────────────────────────────────────────

# Patterns that indicate an adversarially crafted payload
_INJECTION_PATTERNS = re.compile(
    r"ignore\s+(previous|above|all)\s+instruction"
    r"|system\s*:"
    r"|you\s+are\s+(now|a|an)\s"
    r"|<\s*/?instructions?\s*>"
    r"|\bprompt\b.*\binjection\b"
    r"|\bact\s+as\b"
    r"|\bdisregard\b",
    re.IGNORECASE,
)

_KNOWN_SYMBOLS: set[str] = set(_COMEX_SYMBOLS.keys())


def _safe_str(value: object, max_len: int = 50, field_name: str = "field") -> str:
    """
    Validate and sanitise a string field from an external API response.

    - Casts to str, strips whitespace
    - Rejects values longer than max_len (truncates to "[SANITIZED]")
    - Rejects values matching injection patterns (replaces with "[SANITIZED]")
    - Allows only printable ASCII + common Unicode (no control chars)

    Returns a safe string guaranteed not to contain prompt-injection payloads.
    """
    raw = str(value).strip()
    if len(raw) > max_len:
        logger.warning(
            "[SECURITY] %s field too long (%d chars) — sanitising", field_name, len(raw)
        )
        return "[SANITIZED]"
    if _INJECTION_PATTERNS.search(raw):
        logger.warning(
            "[SECURITY] Prompt-injection pattern detected in %s field — sanitising",
            field_name,
        )
        return "[SANITIZED]"
    # Remove ASCII control characters (0x00–0x1F, 0x7F) except tab/newline
    sanitised = re.sub(r"[\x00-\x08\x0b-\x1f\x7f]", "", raw)
    return sanitised


def _safe_price(value: object, field_name: str = "price") -> Optional[float]:
    """
    Validate a price field from an external API response.
    Returns None and logs a warning for any non-positive or non-numeric value.
    """
    try:
        price = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        logger.warning("[SECURITY] Non-numeric %s value: %r — ignoring", field_name, value)
        return None
    if price <= 0:
        logger.warning("[SECURITY] Non-positive %s value: %r — ignoring", field_name, value)
        return None
    return round(price, 6)


def _safe_symbol(value: object) -> Optional[str]:
    """
    Validate that a symbol from the API is in our known whitelist.
    This prevents the API from injecting unknown symbols into the pipeline.
    """
    raw = str(value).strip().upper()
    if raw not in _KNOWN_SYMBOLS:
        logger.warning("[SECURITY] Unknown symbol from API: %r — ignoring", raw)
        return None
    return raw


def _safe_timestamp(value: object) -> Optional[str]:
    """
    Validate an ISO-8601 UTC timestamp string.
    Returns None if the value cannot be parsed.
    """
    try:
        # gold-api returns "2026-02-22T15:33:39Z"
        dt_str = str(value).strip()
        if dt_str.endswith("Z"):
            dt_str = dt_str[:-1] + "+00:00"
        dt = datetime.fromisoformat(dt_str)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except (TypeError, ValueError) as exc:
        logger.debug("[SECURITY] Invalid timestamp %r: %s", value, exc)
        return None


# ── Live price fetcher (gold-api.com) ────────────────────────────────────────

def _fetch_live_price(symbol: str) -> Optional[dict]:
    """
    Fetch the live spot price for one COMEX symbol from gold-api.com.

    Args:
        symbol: COMEX symbol string, e.g. "XAU"

    Returns:
        Validated dict {symbol, name, price, updated_at} or None on failure.
    """
    key = settings.gold_api_key
    if not key:
        logger.warning(
            "GOLD_API_KEY is not set. COMEX enrichment skipped. "
            "Get a free key at https://gold-api.com/"
        )
        return None

    url = f"https://api.gold-api.com/price/{symbol}"
    try:
        resp = requests.get(
            url,
            headers={
                "x-access-token": key,
                "User-Agent": "PortfolioInsightAgent/1.0",
                "Accept": "application/json",
            },
            timeout=10,
        )
        resp.raise_for_status()
        raw: dict = resp.json()
    except requests.RequestException as exc:
        logger.debug("gold-api.com request failed for %s: %s", symbol, exc)
        return None
    except ValueError as exc:
        logger.debug("gold-api.com non-JSON response for %s: %s", symbol, exc)
        return None

    # ── Validate every field coming from the external API ─────────────────
    validated_symbol = _safe_symbol(raw.get("symbol", ""))
    if validated_symbol != symbol:
        logger.warning("[SECURITY] Symbol mismatch: expected %s, got %r", symbol, raw.get("symbol"))
        return None

    price = _safe_price(raw.get("price"), "price")
    if price is None:
        return None

    name       = _safe_str(raw.get("name", symbol), max_len=50, field_name="name")
    updated_at = _safe_timestamp(raw.get("updatedAt", ""))

    return {
        "symbol":     validated_symbol,
        "name":       name,
        "price":      price,
        "updated_at": updated_at or "unknown",
    }


# ── Previous-day close (Yahoo Finance futures) ────────────────────────────────

def _fetch_prev_close(yahoo_ticker: str) -> Optional[float]:
    """
    Fetch yesterday's closing price from Yahoo Finance futures.

    Uses the last two 1-day bars; returns the second-to-last close as
    "previous trading day close". This gracefully handles weekends and
    public holidays.

    Args:
        yahoo_ticker: Yahoo Finance ticker e.g. "GC=F"

    Returns:
        Previous close price (float) or None on failure.
    """
    try:
        ticker = yf.Ticker(yahoo_ticker)
        hist = ticker.history(period="5d", interval="1d")
        if hist.empty or len(hist) < 2:
            logger.debug("Not enough bars for previous close: %s", yahoo_ticker)
            return None
        prev_close = round(float(hist["Close"].iloc[-2]), 6)
        return prev_close
    except Exception as exc:
        logger.debug("Yahoo previous-close fetch failed for %s: %s", yahoo_ticker, exc)
        return None


# ── Signal logic ──────────────────────────────────────────────────────────────

def _compute_signal(change_pct: float) -> str:
    """Classify a day-over-day change percentage into a signal string."""
    if change_pct >= _STRONG_BULL:
        return "STRONG BULLISH"
    if change_pct >= _BULL:
        return "BULLISH"
    if change_pct <= _STRONG_BEAR:
        return "STRONG BEARISH"
    if change_pct <= _BEAR:
        return "BEARISH"
    return "NEUTRAL"


def _is_pre_market_india() -> bool:
    """
    Returns True if the current IST time is before NSE open (09:15 IST).
    Useful for adding a 'pre-market' note to the signal output.
    """
    try:
        from zoneinfo import ZoneInfo
        ist_now = datetime.now(ZoneInfo("Asia/Kolkata"))
        market_open = ist_now.replace(hour=9, minute=15, second=0, microsecond=0)
        return ist_now < market_open
    except Exception:
        return False


# ── Public API ────────────────────────────────────────────────────────────────

def get_comex_signals(symbols: list[str] | None = None) -> dict:
    """
    Fetch live COMEX commodity prices and generate pre-market signals for India.

    For each commodity:
      1. Fetch live spot price from gold-api.com (validated / sanitised)
      2. Fetch previous trading-day close from Yahoo Finance futures
      3. Compute day-over-day change %
      4. Classify as STRONG BULLISH / BULLISH / NEUTRAL / BEARISH / STRONG BEARISH

    Args:
        symbols: List of COMEX symbols to fetch. Defaults to all 5.

    Returns a dict:
    {
        "run_time_ist":  "2026-02-22 09:05:31 IST",
        "pre_market":    True,
        "commodities": {
            "XAU": {
                "name":          "Gold",
                "emoji":         "🥇",
                "live_price":    5107.90,
                "prev_close":    5082.10,
                "change_usd":    +25.80,
                "change_pct":    +0.51,
                "signal":        "BULLISH",
                "unit":          "USD/troy oz",
                "updated_at":    "2026-02-22 15:33:39 UTC",
                "nse_etfs":      ["GOLDBEES", "KOTAKGOLD", ...],
                "source":        "gold-api.com + Yahoo Finance",
            },
            ...
        },
        "summary": "BULLISH: XAU (+0.51%), XAG (+0.12%)  |  BEARISH: HG (-0.45%)",
        "overall_signal": "BULLISH",
    }

    Returns {"error": "..."} on complete failure.
    """
    symbols = [s.upper() for s in (symbols or _DEFAULT_FETCH)]
    unknown = [s for s in symbols if s not in _KNOWN_SYMBOLS]
    if unknown:
        logger.warning("Unknown COMEX symbols requested: %s — skipping", unknown)
        symbols = [s for s in symbols if s in _KNOWN_SYMBOLS]

    if not symbols:
        return {"error": "No valid COMEX symbols to fetch."}

    # IST context
    try:
        from zoneinfo import ZoneInfo
        ist_now = datetime.now(ZoneInfo("Asia/Kolkata"))
        run_time_ist = ist_now.strftime("%Y-%m-%d %H:%M:%S IST")
    except Exception:
        run_time_ist = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    pre_market = _is_pre_market_india()

    commodities: dict[str, dict] = {}

    for sym in symbols:
        meta = _COMEX_SYMBOLS[sym]
        live = _fetch_live_price(sym)
        if live is None:
            logger.debug("Skipping %s — live price unavailable", sym)
            continue

        live_price = live["price"]
        prev_close = _fetch_prev_close(meta["yahoo_ticker"])

        change_usd: Optional[float] = None
        change_pct: Optional[float] = None
        signal: str = "UNKNOWN"

        if prev_close is not None and prev_close > 0:
            change_usd = round(live_price - prev_close, 4)
            change_pct = round((change_usd / prev_close) * 100, 3)
            signal = _compute_signal(change_pct)

        commodities[sym] = {
            "name":       meta["name"],
            "emoji":      meta["emoji"],
            "live_price": live_price,
            "prev_close": prev_close,
            "change_usd": change_usd,
            "change_pct": change_pct,
            "signal":     signal,
            "unit":       meta["unit"],
            "updated_at": live["updated_at"],
            "nse_etfs":   meta["nse_etfs"],
            "source":     "gold-api.com (live) + Yahoo Finance (prev close)",
        }
        logger.info(
            "COMEX %s (%s): $%.4f  prev_close=$%.4f  change=%.3f%%  → %s",
            sym, meta["name"], live_price,
            prev_close or 0, change_pct or 0, signal,
        )

    if not commodities:
        return {
            "error": "All COMEX price fetches failed. Check GOLD_API_KEY and network.",
            "run_time_ist": run_time_ist,
        }

    # ── Build summary string ───────────────────────────────────────────────
    bullish  = [s for s, c in commodities.items() if "BULLISH" in c["signal"]]
    bearish  = [s for s, c in commodities.items() if "BEARISH" in c["signal"]]
    neutral  = [s for s, c in commodities.items() if c["signal"] == "NEUTRAL"]

    def _fmt_parts(syms: list[str]) -> str:
        parts = []
        for s in syms:
            c = commodities[s]
            pct_str = f"{c['change_pct']:+.2f}%" if c["change_pct"] is not None else "N/A"
            parts.append(f"{s} ({pct_str})")
        return ", ".join(parts)

    summary_parts: list[str] = []
    if bullish:
        summary_parts.append(f"↑ BULLISH: {_fmt_parts(bullish)}")
    if neutral:
        summary_parts.append(f"→ NEUTRAL: {_fmt_parts(neutral)}")
    if bearish:
        summary_parts.append(f"↓ BEARISH: {_fmt_parts(bearish)}")
    summary = "  |  ".join(summary_parts) or "No signals computed."

    # Overall: majority-vote; tie goes to bearish (conservative for portfolio)
    if len(bullish) > len(bearish):
        overall_signal = "BULLISH"
    elif len(bearish) > len(bullish):
        overall_signal = "BEARISH"
    else:
        overall_signal = "NEUTRAL"

    return {
        "run_time_ist":   run_time_ist,
        "pre_market":     pre_market,
        "commodities":    commodities,
        "summary":        summary,
        "overall_signal": overall_signal,
    }
