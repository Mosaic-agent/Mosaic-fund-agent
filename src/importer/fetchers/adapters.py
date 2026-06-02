"""
src/importer/fetchers/adapters.py
───────────────────────────────────
Concrete Fetcher adapters for the core daily-import categories.

Each class wraps an existing fetcher function in the Fetcher ABC so the
orchestrator (MarketDataRepository.run_fetcher) can treat all data sources
uniformly — same watermark logic, same dry-run path, same error handling.

Registry
────────
FETCHER_REGISTRY maps CLI category names to Fetcher instances.
Add a new source by appending here — the CLI loop picks it up automatically.

    from src.importer.fetchers.adapters import FETCHER_REGISTRY
    for category in ["etfs", "fx_rates", "fii_dii"]:
        result = repo.run_fetcher(FETCHER_REGISTRY[category])
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

from src.importer.base_fetcher import Fetcher

log = logging.getLogger(__name__)


# ── nselib OHLCV — NSE direct, Yahoo Finance fallback ───────────────────────

class NSElibFetcher(Fetcher):
    """
    Daily OHLCV for NSE-listed ETFs via nselib (direct NSE source, no auth).

    Falls back to Yahoo Finance per symbol when nselib returns no data.
    Accepts the same (nse_symbol, yahoo_ticker) tuple format as YFinanceFetcher.
    """
    overlap_days = 3

    def __init__(self, category: str, symbols: list[tuple[str, str]]) -> None:
        self.category    = category
        self.symbols     = symbols
        self.source_name = "nselib"
        self.symbol_key  = category.upper()
        self.description = f"nselib OHLCV — {category} ({len(symbols)} symbols)"

    def fetch(self, from_date: date, to_date: date) -> list[dict[str, Any]]:
        from src.importer.fetchers.nselib_fetcher import fetch_nselib_ohlcv
        from src.importer.fetchers.yfinance_fetcher import fetch_ohlcv as yf_fetch

        nse_rows = fetch_nselib_ohlcv(self.symbols, self.category, from_date, to_date)

        covered = {r["symbol"] for r in nse_rows}
        missing = [(nse, yf) for nse, yf in self.symbols if nse not in covered]

        if missing:
            log.info(
                "%s: nselib missing %d symbol(s) — falling back to yfinance: %s",
                self.category, len(missing), [s[0] for s in missing[:5]],
            )
            nse_rows.extend(yf_fetch(missing, self.category, from_date, to_date))

        return nse_rows

    def insert(self, rows: list[dict], ch) -> int:
        return ch.insert_prices(rows)

    def validate(self, rows: list[dict]) -> list[dict]:
        return [r for r in rows if r.get("close") and r["close"] > 0]

    def max_date(self, rows: list[dict]) -> date:
        return max(r["trade_date"] for r in rows)


# ── Shoonya OHLCV — NSE primary, Yahoo Finance fallback ─────────────────────

class ShoonyaFetcher(Fetcher):
    """
    Daily OHLCV for NSE-listed symbols via Shoonya brokerage API.

    Falls back transparently to Yahoo Finance per symbol when:
      - Shoonya credentials are not configured
      - Shoonya returns no data for a symbol
      - The symbol is non-NSE (global indices, FX, US ETFs) — those always
        go to Yahoo Finance

    Same (nse_symbol, yahoo_ticker) tuple interface as YFinanceFetcher.
    """
    overlap_days = 3

    def __init__(self, category: str, symbols: list[tuple[str, str]]) -> None:
        self.category    = category
        self.symbols     = symbols
        self.source_name = "shoonya"
        self.symbol_key  = category.upper()
        self.description = f"Shoonya OHLCV — {category} ({len(symbols)} symbols)"

    def fetch(self, from_date: date, to_date: date) -> list[dict[str, Any]]:
        from src.importer.fetchers.shoonya_fetcher import fetch_shoonya_ohlcv
        from src.importer.fetchers.yfinance_fetcher import fetch_ohlcv as yf_fetch_ohlcv

        rows = fetch_shoonya_ohlcv(self.symbols, self.category, from_date, to_date)
        covered = {r["symbol"] for r in rows}
        missing = [(nse, yf) for nse, yf in self.symbols if nse not in covered]

        # For NSE-listed categories: try nselib before falling back to yfinance
        if missing and self.category in {"etfs", "stocks"}:
            from src.importer.fetchers.nselib_fetcher import fetch_nselib_ohlcv
            nse_rows = fetch_nselib_ohlcv(missing, self.category, from_date, to_date)
            rows.extend(nse_rows)
            covered = {r["symbol"] for r in rows}
            missing = [(nse, yf) for nse, yf in self.symbols if nse not in covered]
            if nse_rows:
                log.info("%s: nselib covered %d symbol(s) as Shoonya fallback", self.category, len(nse_rows))

        # Final fallback: yfinance for anything still missing
        if missing:
            log.info(
                "%s: falling back to yfinance for %d symbol(s): %s",
                self.category, len(missing), [s[0] for s in missing[:5]],
            )
            rows.extend(yf_fetch_ohlcv(missing, self.category, from_date, to_date))

        return rows

    def insert(self, rows: list[dict], ch) -> int:
        return ch.insert_prices(rows)

    def validate(self, rows: list[dict]) -> list[dict]:
        return [r for r in rows if r.get("close") and r["close"] > 0]

    def max_date(self, rows: list[dict]) -> date:
        return max(r["trade_date"] for r in rows)


# ── yfinance OHLCV (stocks / ETFs / commodities / indices) ──────────────────

class YFinanceFetcher(Fetcher):
    """
    Daily OHLCV bars from Yahoo Finance for a fixed symbol list.

    One instance per category (etfs, stocks, commodities, indices) because
    each category has its own symbol list and watermark namespace.
    """
    overlap_days = 3

    def __init__(self, category: str, symbols: list[tuple[str, str]]) -> None:
        self.category    = category
        self.symbols     = symbols
        self.source_name = "yfinance"
        self.symbol_key  = category.upper()
        self.description = f"yfinance OHLCV — {category} ({len(symbols)} symbols)"

    def fetch(self, from_date: date, to_date: date) -> list[dict[str, Any]]:
        try:
            from src.importer.fetchers.yfinance_fetcher import fetch_ohlcv
            return fetch_ohlcv(self.symbols, self.category, from_date, to_date)
        except Exception as exc:
            log.warning("%s fetch failed: %s", self, exc)
            return []

    def insert(self, rows: list[dict], ch) -> int:
        return ch.insert_prices(rows)

    def validate(self, rows: list[dict]) -> list[dict]:
        return [r for r in rows if r.get("close") and r["close"] > 0]

    def max_date(self, rows: list[dict]) -> date:
        return max(r["trade_date"] for r in rows)


# ── MF NAV (MFAPI.in) ────────────────────────────────────────────────────────

class MFNavFetcher(Fetcher):
    """Daily mutual fund NAV from MFAPI.in for a fixed set of scheme codes."""

    source_name  = "mfapi"
    symbol_key   = "ALL"
    description  = "MF NAV (MFAPI.in)"
    overlap_days = 3

    def __init__(self, scheme_codes: list[str]) -> None:
        self.scheme_codes = scheme_codes

    def fetch(self, from_date: date, to_date: date) -> list[dict[str, Any]]:
        try:
            from src.importer.fetchers.mfapi_fetcher import fetch_all_nav
            return fetch_all_nav(self.scheme_codes, from_date, to_date)
        except Exception as exc:
            log.warning("%s fetch failed: %s", self, exc)
            return []

    def insert(self, rows: list[dict], ch) -> int:
        return ch.insert_nav(rows)

    def max_date(self, rows: list[dict]) -> date:
        return max(r["nav_date"] for r in rows)


# ── FII / DII daily cash flows (NSE via Sensibull) ──────────────────────────

class FIIDIIFetcher(Fetcher):
    """
    FII + DII daily cash-market flows from Sensibull/NSE.
    Also writes F&O OI and monthly aggregates as a side-effect.
    """

    source_name  = "nse_fii_dii"
    symbol_key   = "MARKET"
    description  = "FII/DII institutional flows (NSE)"
    overlap_days = 3

    def fetch(self, from_date: date, to_date: date) -> list[dict[str, Any]]:
        try:
            from src.importer.fetchers.fii_dii_fetcher import fetch_fii_dii
            return fetch_fii_dii(from_date=from_date)
        except Exception as exc:
            log.warning("%s fetch failed: %s", self, exc)
            return []

    def insert(self, rows: list[dict], ch) -> int:
        n = ch.insert_fii_dii_flows(rows)
        # Side-effect: also import F&O OI and monthly aggregates
        try:
            from src.importer.fetchers.fii_dii_fetcher import fetch_fii_dii_fno, fetch_fii_dii_monthly
            fno_rows     = fetch_fii_dii_fno()
            monthly_rows = fetch_fii_dii_monthly()
            if fno_rows:
                ch.insert_fii_dii_fno_daily(fno_rows)
            if monthly_rows:
                ch.insert_fii_dii_monthly(monthly_rows)
        except Exception as exc:
            log.warning("FII/DII F&O/monthly side-effect failed: %s", exc)
        return n

    def max_date(self, rows: list[dict]) -> date:
        return max(r["trade_date"] for r in rows)


# ── FX Rates (USD pairs via Yahoo Finance) ───────────────────────────────────

class FXRatesFetcher(Fetcher):
    """
    Daily OHLC for USD currency pairs from Yahoo Finance.
    Uses per-pair watermarks internally; symbol_key covers the group.
    """

    source_name  = "yfinance_fx"
    symbol_key   = "FX_GROUP"
    description  = "FX rates — USD pairs (Yahoo Finance)"
    overlap_days = 3

    def fetch(self, from_date: date, to_date: date) -> list[dict[str, Any]]:
        try:
            from src.importer.fetchers.fx_rates_fetcher import fetch_fx_rates
            return fetch_fx_rates(from_date=from_date, to_date=to_date)
        except Exception as exc:
            log.warning("%s fetch failed: %s", self, exc)
            return []

    def insert(self, rows: list[dict], ch) -> int:
        return ch.insert_fx_rates(rows)

    def max_date(self, rows: list[dict]) -> date:
        return max(r["trade_date"] for r in rows)


# ── CFTC COT — Gold (Managed Money) ─────────────────────────────────────────

class COTGoldFetcher(Fetcher):
    """
    CFTC Disaggregated Commitments of Traders — Gold (commodity code 088).
    Weekly release (Fridays). Falls back to direct CFTC download when
    Socrata is stale.
    """

    source_name  = "cot"
    symbol_key   = "GOLD"
    description  = "CFTC COT — Gold managed money (weekly)"
    overlap_days = 21  # 3-week overlap to catch late CFTC releases

    def fetch(self, from_date: date, to_date: date) -> list[dict[str, Any]]:
        try:
            from src.importer.fetchers.cot_fetcher import fetch_cot_gold
            return fetch_cot_gold(from_date=from_date)
        except Exception as exc:
            log.warning("%s fetch failed: %s", self, exc)
            return []

    def insert(self, rows: list[dict], ch) -> int:
        return ch.insert_cot_gold(rows)

    def max_date(self, rows: list[dict]) -> date:
        return max(r["report_date"] for r in rows)


# ── World Bank Macro Indicators ───────────────────────────────────────────────

class WorldBankMacroFetcher(Fetcher):
    """
    Annual macro indicators for India + G4 peers from World Bank WDI API.
    Covers: GDP growth, CPI, current account, govt debt, savings, FDI,
    unemployment, exports.  ~12-month lag; no auth required.
    """

    source_name  = "world_bank"
    symbol_key   = "MACRO_GROUP"
    description  = "World Bank WDI macro indicators (India + G4 peers)"
    overlap_days = 365  # annual data — always re-check the last full year

    def fetch(self, from_date: date, to_date: date) -> list[dict[str, Any]]:
        from_year = from_date.year
        to_year   = to_date.year
        try:
            from src.importer.fetchers.worldbank_macro_fetcher import fetch_worldbank_macro
            return fetch_worldbank_macro(from_year=from_year, to_year=to_year)
        except Exception as exc:
            log.warning("%s fetch failed: %s", self, exc)
            return []

    def insert(self, rows: list[dict], ch) -> int:
        return ch.insert_macro_indicators(rows)

    def validate(self, rows: list[dict]) -> list[dict]:
        return [r for r in rows if r.get("value") is not None]

    def max_date(self, rows: list[dict]) -> date:
        max_year = max(int(r["ref_year"]) for r in rows)
        return date(max_year, 12, 31)


# ── IMF WEO Projections ───────────────────────────────────────────────────────

class IMFWEOFetcher(Fetcher):
    """
    IMF World Economic Outlook projections via DataMapper API.
    Covers: GDP growth, CPI, current account, fiscal balance, unemployment.
    Published twice yearly (Apr/Oct); includes 2–3 year forecasts.
    No auth required.
    """

    source_name  = "imf_weo"
    symbol_key   = "MACRO_GROUP"
    description  = "IMF WEO projections (India + G4 peers)"
    overlap_days = 180  # semi-annual release — overlap half a year

    def fetch(self, from_date: date, to_date: date) -> list[dict[str, Any]]:
        from_year = from_date.year
        # Extend to_year by 3 to capture WEO forecasts (published for 3 years ahead)
        to_year = to_date.year + 3
        try:
            from src.importer.fetchers.imf_weo_fetcher import fetch_imf_weo
            return fetch_imf_weo(from_year=from_year, to_year=to_year)
        except Exception as exc:
            log.warning("%s fetch failed: %s", self, exc)
            return []

    def insert(self, rows: list[dict], ch) -> int:
        return ch.insert_macro_indicators(rows)

    def validate(self, rows: list[dict]) -> list[dict]:
        return [r for r in rows if r.get("value") is not None]

    def max_date(self, rows: list[dict]) -> date:
        # Use only actuals (is_forecast=0) for the watermark
        actual_rows = [r for r in rows if not r.get("is_forecast", 0)]
        if actual_rows:
            max_year = max(int(r["ref_year"]) for r in actual_rows)
        else:
            max_year = max(int(r["ref_year"]) for r in rows)
        return date(max_year, 12, 31)


# ── Registry ──────────────────────────────────────────────────────────────────
# Maps CLI category name → Fetcher instance.
# The orchestrator loops over this — adding a new source = one line here.

def _build_registry() -> dict[str, Fetcher]:
    from src.importer.registry import (
        get_symbols_for_categories,
        MF_SCHEME_CODES,
    )
    sym_map = get_symbols_for_categories(["stocks", "etfs", "commodities", "indices"])

    registry: dict[str, Fetcher] = {}
    for cat, sym_list in sym_map.items():
        if cat in {"etfs", "stocks"}:
            registry[cat] = ShoonyaFetcher(cat, sym_list)  # Shoonya → nselib (etfs) → yfinance
        else:
            registry[cat] = YFinanceFetcher(cat, sym_list) # global symbols

    registry["mf"]      = MFNavFetcher(MF_SCHEME_CODES)
    registry["fii_dii"] = FIIDIIFetcher()
    registry["fx_rates"] = FXRatesFetcher()
    registry["cot"]     = COTGoldFetcher()
    registry["world_bank"] = WorldBankMacroFetcher()
    registry["imf_weo"]    = IMFWEOFetcher()
    return registry


# Lazily initialised on first access to avoid import-time side effects
_REGISTRY: dict[str, Fetcher] | None = None


def get_registry() -> dict[str, Fetcher]:
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = _build_registry()
    return _REGISTRY
