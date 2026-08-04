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

    from src.data_importer.fetchers.adapters import FETCHER_REGISTRY
    for category in ["etfs", "fx_rates", "fii_dii"]:
        result = repo.run_fetcher(FETCHER_REGISTRY[category])
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

from src.data_importer.base_fetcher import Fetcher

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
        from src.data_importer.fetchers.nselib_fetcher import fetch_nselib_ohlcv
        from src.data_importer.fetchers.yfinance_fetcher import fetch_ohlcv as yf_fetch

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
    supports_parallel = True
    supports_source_override = True

    def __init__(self, category: str, symbols: list[tuple[str, str]]) -> None:
        self.category    = category
        self.symbols     = symbols
        self.source_name = "shoonya"
        self.symbol_key  = category.upper()
        self.description = f"Shoonya OHLCV — {category} ({len(symbols)} symbols)"

    def fetch(self, from_date: date, to_date: date, *, source: str | None = None) -> list[dict[str, Any]]:
        from src.data_importer.fetchers.yfinance_fetcher import fetch_ohlcv as yf_fetch_ohlcv

        if source == "nse":
            return NSElibFetcher(self.category, self.symbols).fetch(from_date, to_date)
        if source == "yfinance":
            return yf_fetch_ohlcv(self.symbols, self.category, from_date, to_date)

        from src.data_importer.fetchers.shoonya_fetcher import fetch_shoonya_ohlcv

        rows = fetch_shoonya_ohlcv(self.symbols, self.category, from_date, to_date)
        covered = {r["symbol"] for r in rows}
        missing = [(nse, yf) for nse, yf in self.symbols if nse not in covered]

        # For NSE-listed categories: try nselib before falling back to yfinance
        if missing and self.category in {"etfs", "stocks"}:
            from src.data_importer.fetchers.nselib_fetcher import fetch_nselib_ohlcv
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


# ── Stocks (prices + earnings + insider + valuation) ────────────────────────

_STOCKS_INITIAL_SLEEP = 0.3   # stagger thread start
_STOCKS_BETWEEN_CALLS = 0.4   # spacing between sequential API calls per symbol


class StocksFetcher(Fetcher):
    """
    Per-symbol OHLCV + earnings + insider trades + valuation for stocks/us_stocks.

    Ports parallel_importer.run_parallel_stock_import's per-symbol behavior
    into the Fetcher ABC: when run with supports_parallel workers, the
    orchestrator calls fetch() once per symbol (via for_symbol()) so each of
    the four sub-fetches below runs inside its own thread, same as today.

    Rows carry a "_dataset" tag (prices|earnings|insider|valuation) so a
    single fetch() batch spanning multiple ClickHouse tables can be routed
    by insert()/write_group_watermarks() without a second fetch.
    """
    overlap_days = 3
    supports_parallel = True
    supports_source_override = True
    # Each symbol resolves its own watermark independently (matches
    # parallel_importer.import_single_stock's per-thread ch.get_watermark
    # call) rather than one worst-case watermark shared across the whole
    # symbol list — adding one new symbol must not force a full-lookback
    # re-fetch of every other symbol already caught up.
    per_symbol_watermark = True

    def __init__(self, category: str, symbols: list[tuple[str, str]]) -> None:
        self.category    = category
        self.symbols     = symbols
        self.source_name = "shoonya" if category == "stocks" else "yfinance"
        self.symbol_key  = category.upper()
        self.description = f"Stocks (prices+earnings+insider+valuation) — {category} ({len(symbols)} symbols)"
        # us_stocks has no Shoonya/NSE presence — --data-source is ignored for
        # it today (import_single_stock always passes data_source="yfinance"
        # for us_stocks, regardless of the CLI override), so only "stocks"
        # actually honors a source override.
        self.supports_source_override = category == "stocks"

    def fetch(self, from_date: date, to_date: date, *, source: str | None = None) -> list[dict[str, Any]]:
        import time
        from src.data_importer.fetchers.earnings_fetcher import fetch_earnings
        from src.data_importer.fetchers.insider_fetcher import fetch_insider_trades
        from src.data_importer.fetchers.valuation_fetcher import fetch_valuation
        from src.data_importer.fetchers.yfinance_fetcher import fetch_ohlcv as yf_fetch_ohlcv

        time.sleep(_STOCKS_INITIAL_SLEEP)

        rows: list[dict[str, Any]] = []
        effective_source = source or self.source_name

        if self.category == "stocks":
            if effective_source == "nse":
                prices = NSElibFetcher(self.category, self.symbols).fetch(from_date, to_date)
            elif effective_source == "yfinance":
                prices = yf_fetch_ohlcv(self.symbols, self.category, from_date, to_date)
            else:
                prices = ShoonyaFetcher(self.category, self.symbols).fetch(from_date, to_date)
        else:
            prices = yf_fetch_ohlcv(self.symbols, self.category, from_date, to_date)
        for r in prices:
            r["_dataset"] = "prices"
        rows.extend(prices)

        time.sleep(_STOCKS_BETWEEN_CALLS)
        earnings = fetch_earnings(self.symbols)
        for r in earnings:
            r["_dataset"] = "earnings"
        rows.extend(earnings)

        time.sleep(_STOCKS_BETWEEN_CALLS)
        insider = fetch_insider_trades(self.symbols)
        for r in insider:
            r["_dataset"] = "insider"
        rows.extend(insider)

        time.sleep(_STOCKS_BETWEEN_CALLS)
        valuation = fetch_valuation(self.symbols)
        for r in valuation:
            r["_dataset"] = "valuation"
        rows.extend(valuation)

        return rows

    def insert(self, rows: list[dict], ch) -> int:
        by_dataset: dict[str, list[dict]] = {"prices": [], "earnings": [], "insider": [], "valuation": []}
        for r in rows:
            by_dataset.setdefault(r.get("_dataset", "prices"), []).append(r)

        n = ch.insert_prices(by_dataset["prices"]) if by_dataset["prices"] else 0
        if by_dataset["earnings"]:
            ch.insert_stock_earnings(by_dataset["earnings"])
        if by_dataset["insider"]:
            ch.insert_stock_insider(by_dataset["insider"])
        if by_dataset["valuation"]:
            ch.insert_stock_valuation(by_dataset["valuation"])
        return n

    def validate(self, rows: list[dict]) -> list[dict]:
        return [
            r for r in rows
            if r.get("_dataset") != "prices" or (r.get("close") and r["close"] > 0)
        ]

    def max_date(self, rows: list[dict]) -> date:
        price_rows = [r for r in rows if r.get("_dataset") == "prices"]
        if price_rows:
            return max(r["trade_date"] for r in price_rows)
        return date.today()

    def count_insertable(self, rows: list[dict]) -> int:
        # dry-run preview must match insert()'s real-run count — prices only.
        return len([r for r in rows if r.get("_dataset") == "prices"])

    def write_group_watermarks(
        self, ch, rows: list[dict], dry_run: bool, *, source: str | None = None,
    ) -> None:
        if dry_run or not rows:
            return
        # Watermark the source that actually produced these rows, not the
        # fetcher's static default — a --data-source nse run must not have
        # its watermark silently read back by tomorrow's default-source run.
        effective_source = source or self.source_name
        today = date.today()
        by_symbol_dataset: dict[tuple[str, str], list[dict]] = {}
        for r in rows:
            key = (r["symbol"], r.get("_dataset", "prices"))
            by_symbol_dataset.setdefault(key, []).append(r)

        for (sym, ds), ds_rows in by_symbol_dataset.items():
            if ds == "prices":
                ch.set_watermark(effective_source, sym, max(r["trade_date"] for r in ds_rows), dataset="prices")
            else:
                ch.set_watermark("yfinance", sym, today, dataset=ds)


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
            from src.data_importer.fetchers.yfinance_fetcher import fetch_ohlcv
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
    supports_parallel = True  # per-scheme group watermark, not per-scheme threaded fetch (workers stays 1)

    def __init__(self, scheme_codes: dict[str, str]) -> None:
        self.scheme_codes = scheme_codes  # {nse_symbol: amfi_scheme_code}
        self.symbols = list(scheme_codes.items())

    def fetch(self, from_date: date, to_date: date, *, source: str | None = None) -> list[dict[str, Any]]:
        try:
            from src.data_importer.fetchers.mfapi_fetcher import fetch_all_nav
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
        self._last_from_date = from_date
        try:
            from src.data_importer.fetchers.fii_dii_fetcher import fetch_fii_dii
            return fetch_fii_dii(from_date=from_date)
        except Exception as exc:
            log.warning("%s fetch failed: %s", self, exc)
            return []

    def insert(self, rows: list[dict], ch) -> int:
        n = ch.insert_fii_dii_flows(rows)

        if rows:
            latest_fii = sorted(rows, key=lambda r: r["trade_date"])[-1]
            log.info(
                "Latest Cash (%s): FII Net ₹%s Cr  |  DII Net ₹%s Cr",
                latest_fii["trade_date"],
                f"{latest_fii.get('fii_net_cr', 0.0):+,.0f}",
                f"{latest_fii.get('dii_net_cr', 0.0):+,.0f}",
            )

        # Side-effect: also import F&O OI and monthly aggregates
        try:
            from src.data_importer.fetchers.fii_dii_fetcher import fetch_fii_dii_fno, fetch_fii_dii_monthly
            fno_from = getattr(self, "_last_from_date", None)
            fno_rows     = fetch_fii_dii_fno(from_date=fno_from) if fno_from else fetch_fii_dii_fno()
            monthly_rows = fetch_fii_dii_monthly()
            if fno_rows:
                ch.insert_fii_dii_fno_daily(fno_rows)
                latest_fno = sorted(fno_rows, key=lambda r: r["trade_date"])[-1]
                log.info(
                    "Latest F&O (%s): FII Fut Net %s | FII Opt OI %s",
                    latest_fno["trade_date"],
                    f"{latest_fno.get('fii_fut_net_oi', 0.0):+,.0f}",
                    f"{latest_fno.get('fii_opt_overall_net_oi', 0.0):+,.0f}",
                )
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
    supports_parallel = True  # per-pair group watermark, not per-pair threaded fetch (workers stays 1)

    def __init__(self) -> None:
        from src.data_importer.fetchers.fx_rates_fetcher import FX_PAIRS
        self.symbols = FX_PAIRS

    def fetch(self, from_date: date, to_date: date, *, source: str | None = None) -> list[dict[str, Any]]:
        try:
            from src.data_importer.fetchers.fx_rates_fetcher import fetch_fx_rates
            return fetch_fx_rates(from_date=from_date, to_date=to_date)
        except Exception as exc:
            log.warning("%s fetch failed: %s", self, exc)
            return []

    def insert(self, rows: list[dict], ch) -> int:
        n = ch.insert_fx_rates(rows)
        latest_by_sym: dict[str, dict] = {}
        for r in rows:
            if r["symbol"] not in latest_by_sym or r["trade_date"] > latest_by_sym[r["symbol"]]["trade_date"]:
                latest_by_sym[r["symbol"]] = r
        for sym, r in sorted(latest_by_sym.items()):
            log.info("%-8s %s close=%.4f", sym, r["trade_date"], r["close"])
        return n

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

    def fetch(self, from_date: date, to_date: date, *, source: str | None = None) -> list[dict[str, Any]]:
        try:
            from src.data_importer.fetchers.cot_fetcher import fetch_cot_gold
            # A fixed generous limit regardless of delta/first-run: delta runs
            # are already bounded by the from_date filter, and a bigger cap
            # never hurts on a first run (more history allowed, never less).
            return fetch_cot_gold(from_date=from_date, limit=2000)
        except Exception as exc:
            log.warning("%s fetch failed: %s", self, exc)
            return []

    def insert(self, rows: list[dict], ch) -> int:
        n = ch.insert_cot_gold(rows)
        latest = sorted(rows, key=lambda r: r["report_date"])[-1]
        log.info(
            "COT latest (%s): MM Net %s | OI %s | MM pct OI %+.1f%%",
            latest["report_date"], f"{latest['mm_net']:+,d}", f"{latest['open_interest']:,d}",
            latest["mm_net"] / max(latest["open_interest"], 1) * 100,
        )
        return n

    def max_date(self, rows: list[dict]) -> date:
        return max(r["report_date"] for r in rows)


# ── World Bank Macro Indicators ───────────────────────────────────────────────

class CbReservesFetcher(Fetcher):
    """
    Central bank gold reserves (9 countries, RAFAGOLD series), monthly,
    ~6-week publication lag. fetch() takes a year, not a date range —
    overlap_days=0 makes run_fetcher() pass the raw watermark through as
    from_date unmodified, so from_date.year matches today's cb_wm.year
    exactly whenever a watermark exists.
    """
    source_name  = "cb_reserves"
    symbol_key   = "ALL"
    description  = "IMF IFS — Central Bank Gold Reserves"
    overlap_days = 0

    def fetch(self, from_date: date, to_date: date, *, source: str | None = None) -> list[dict[str, Any]]:
        try:
            from src.data_importer.fetchers.imf_reserves_fetcher import fetch_cb_reserves
            return fetch_cb_reserves(from_year=from_date.year)
        except Exception as exc:
            log.warning("%s fetch failed: %s", self, exc)
            return []

    def insert(self, rows: list[dict], ch) -> int:
        return ch.insert_cb_reserves(rows)

    def max_date(self, rows: list[dict]) -> date:
        return max(r["ref_period"] for r in rows)


class EtfAumFetcher(Fetcher):
    """
    Daily AUM + implied gold tonnes for GLD/IAU/SGOL/PHYS via yfinance.
    No date-range parameter upstream — always today's snapshot. Adding a
    watermark here is purely additive bookkeeping (fetch() ignores
    from_date/to_date either way, so it can't change what's fetched).
    """
    source_name  = "etf_aum"
    symbol_key   = "ALL"
    description  = "Gold ETF AUM Snapshots (GLD · IAU · SGOL · PHYS)"

    def fetch(self, from_date: date, to_date: date, *, source: str | None = None) -> list[dict[str, Any]]:
        try:
            from src.data_importer.fetchers.etf_aum_fetcher import fetch_etf_aum
            return fetch_etf_aum()
        except Exception as exc:
            log.warning("%s fetch failed: %s", self, exc)
            return []

    def insert(self, rows: list[dict], ch) -> int:
        n = ch.insert_etf_aum(rows)
        for r in rows:
            log.info(
                "%-6s AUM $%.2fB price $%.2f ~%.0ft",
                r["symbol"], r["aum_usd"] / 1e9, r["price"], r["implied_tonnes"],
            )
        return n

    def max_date(self, rows: list[dict]) -> date:
        return max(r["trade_date"] for r in rows)


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
            from src.data_importer.fetchers.worldbank_macro_fetcher import fetch_worldbank_macro
            return fetch_worldbank_macro(from_year=from_year, to_year=to_year)
        except Exception as exc:
            log.warning("%s fetch failed: %s", self, exc)
            return []

    def insert(self, rows: list[dict], ch) -> int:
        n = ch.insert_macro_indicators(rows)
        india_gdp = [
            r for r in rows
            if r.get("country_code") == "IN" and r.get("indicator_code") == "NY.GDP.MKTP.KD.ZG"
        ]
        if india_gdp:
            latest = sorted(india_gdp, key=lambda r: r["ref_year"])[-1]
            log.info("India GDP growth (%s): %+.2f%%", latest["ref_year"], latest["value"])
        return n

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
            from src.data_importer.fetchers.imf_weo_fetcher import fetch_imf_weo
            return fetch_imf_weo(from_year=from_year, to_year=to_year)
        except Exception as exc:
            log.warning("%s fetch failed: %s", self, exc)
            return []

    def insert(self, rows: list[dict], ch) -> int:
        n = ch.insert_macro_indicators(rows)
        today_year = date.today().year
        india_gdp = [
            r for r in rows
            if r.get("country_code") == "IN"
            and r.get("indicator_code") == "NGDP_RPCH"
            and int(r.get("ref_year", 0)) >= today_year
        ]
        if india_gdp:
            forecasts = sorted(india_gdp, key=lambda r: r["ref_year"])[:3]
            fwd_str = "  ".join(f"{r['ref_year']}={r['value']:+.1f}%" for r in forecasts)
            log.info("India GDP forecasts: %s", fwd_str)
        return n

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


# ── nselib NSE indices (no Yahoo Finance ticker) ────────────────────────────

class NseIndexFetcher(Fetcher):
    """
    Daily OHLCV for NSE indices via nselib.capital_market.index_data().

    For indices not available on Yahoo Finance (midcap/smallcap variants,
    sectoral, thematic, strategy/factor indices).
    Stores data in daily_prices with category='indices'.
    """
    overlap_days = 3

    def __init__(self, symbols: list[tuple[str, str]]) -> None:
        self.category    = "indices"
        self.symbols     = symbols           # (internal_symbol, nse_api_index_name)
        self.source_name = "nselib_index"
        self.symbol_key  = "NSE_INDICES"
        self.description = f"nselib index data ({len(symbols)} indices)"

    def fetch(self, from_date: date, to_date: date) -> list[dict[str, Any]]:
        from src.data_importer.fetchers.nse_index_fetcher import fetch_nse_indices
        return fetch_nse_indices(self.symbols, from_date, to_date)

    def insert(self, rows: list[dict], ch) -> int:
        return ch.insert_prices(rows)

    def validate(self, rows: list[dict]) -> list[dict]:
        return [r for r in rows if r.get("close") and r["close"] > 0]

    def max_date(self, rows: list[dict]) -> date:
        return max(r["trade_date"] for r in rows)


# ── NSE EOD OHLCV (ETFs + stocks, direct NSE Quote API) ────────────────────

class NseEodFetcher(Fetcher):
    """
    Same-day OHLCV for ETFs + stocks via the NSE Quote API — available right
    after 3:30 PM IST market close, without Yahoo Finance's ~1h delay.

    fetch() ignores from_date/to_date — there's no date-range parameter on
    the underlying NSE Quote endpoint, it always returns *today's* bar.
    """
    overlap_days = 3
    supports_parallel = True  # per-symbol watermark write, not per-symbol fetch (workers stays 1)

    def __init__(self, etf_symbols: list[tuple[str, str]], stock_symbols: list[tuple[str, str]]) -> None:
        self._etf_symbols   = etf_symbols
        self._stock_symbols = stock_symbols
        self.symbols      = etf_symbols + stock_symbols
        self.source_name  = "nse_quote"
        self.symbol_key   = "NSE_EOD_GROUP"
        self.description  = f"NSE EOD OHLCV — ETFs + stocks ({len(self.symbols)} symbols)"

    def fetch(self, from_date: date, to_date: date, *, source: str | None = None) -> list[dict[str, Any]]:
        from src.data_importer.fetchers.nse_quote_fetcher import fetch_nse_eod

        rows: list[dict[str, Any]] = []
        for cat_name, sym_list in (("etfs", self._etf_symbols), ("stocks", self._stock_symbols)):
            rows.extend(fetch_nse_eod(sym_list, cat_name))
        return rows

    def insert(self, rows: list[dict], ch) -> int:
        return ch.insert_prices(rows)

    def validate(self, rows: list[dict]) -> list[dict]:
        return [r for r in rows if r.get("close") and r["close"] > 0]

    def max_date(self, rows: list[dict]) -> date:
        return max(r["trade_date"] for r in rows)


# ── MF Holdings (Morningstar) ───────────────────────────────────────────────

class MfHoldingsFetcher(Fetcher):
    """
    Monthly portfolio-holdings snapshot per fund in the watchlist.

    Constructed fresh per call (not cached in get_registry()) — as_of_month
    is a runtime value, not something fixed at registry-build time. The
    "already imported this month" idempotency guard lives in cli.py, not
    here — it's a presence check against real data, not delta-sync logic
    fetch()/watermarks can express.
    """
    source_name = "mf_holdings"
    symbol_key  = "ALL"
    description = "MF Holdings (Morningstar)"
    overlap_days = 0

    def __init__(self, watchlist: list[tuple[str, str, str]], as_of_month: date) -> None:
        self.watchlist   = watchlist
        self.as_of_month = as_of_month

    def fetch(self, from_date: date, to_date: date, *, source: str | None = None) -> list[dict[str, Any]]:
        from src.data_importer.fetchers.mf_holdings_fetcher import fetch_holdings
        return fetch_holdings(self.watchlist, self.as_of_month)

    def insert(self, rows: list[dict], ch) -> int:
        return ch.insert_mf_holdings(rows)

    def max_date(self, rows: list[dict]) -> date:
        return self.as_of_month


# ── AMFI Category-Wise Monthly Flows + AUM ──────────────────────────────────

class AmfiCategoryFlowsFetcher(Fetcher):
    """
    AMFI industry data — monthly net flows & AUM by fund category.

    fetch() has no date-range parameter upstream; it takes months_back
    instead. overlap_days=0 makes run_fetcher() pass the raw watermark
    through as from_date unmodified, so months_back can be derived from it
    with exact parity to the original cli.py month-arithmetic whenever a
    watermark exists.
    """
    source_name  = "amfi_category_flows"
    symbol_key   = "INDUSTRY"
    dataset      = "flows"
    description  = "AMFI Category-Wise Monthly Flows + AUM"
    overlap_days = 0
    first_run_lookback_days = 730  # ~24 months, matches the original first-run default

    def fetch(self, from_date: date, to_date: date, *, source: str | None = None) -> list[dict[str, Any]]:
        from src.data_importer.fetchers.amfi_flows_fetcher import fetch_amfi_category_flows

        months_back = max(
            2,
            (to_date.year - from_date.year) * 12 + (to_date.month - from_date.month) + 1,
        )
        return fetch_amfi_category_flows(months_back=months_back)

    def insert(self, rows: list[dict], ch) -> int:
        n = ch.insert_amfi_category_flows(rows)
        latest_m = max(r["report_month"] for r in rows)
        latest_rows = sorted(
            (r for r in rows if r["report_month"] == latest_m),
            key=lambda r: r["net_flow_cr"], reverse=True,
        )
        log.info("AMFI category flows — latest month: %s", latest_m.strftime("%b %Y"))
        for r in latest_rows[:3]:
            log.info(
                "  %-30s Net: ₹%s Cr  |  AUM: ₹%s Cr",
                r["category_name"][:30],
                f"{r['net_flow_cr']:+,.0f}",
                f"{r['closing_aum_cr']:,.0f}",
            )
        return n

    def max_date(self, rows: list[dict]) -> date:
        return max(r["report_month"] for r in rows)


# ── Registry ──────────────────────────────────────────────────────────────────
# Maps CLI category name → Fetcher instance.
# The orchestrator loops over this — adding a new source = one line here.

def _build_registry() -> dict[str, Fetcher]:
    from src.data_importer.registry import (
        get_symbols_for_categories,
        MF_SCHEME_CODES,
        NSE_ONLY_INDICES,
        ETFS,
        STOCKS,
    )
    from src.data_importer.fetchers.indian_macro_fetcher import IndianMacroFetcher

    sym_map = get_symbols_for_categories(["stocks", "us_stocks", "etfs", "commodities", "indices"])

    registry: dict[str, Fetcher] = {}
    for cat, sym_list in sym_map.items():
        if cat == "etfs":
            registry[cat] = ShoonyaFetcher(cat, sym_list)  # Shoonya → nselib (etfs) → yfinance
        elif cat in {"stocks", "us_stocks"}:
            registry[cat] = StocksFetcher(cat, sym_list)   # prices + earnings + insider + valuation
        else:
            registry[cat] = YFinanceFetcher(cat, sym_list) # global symbols

    registry["nse_indices"] = NseIndexFetcher(NSE_ONLY_INDICES)
    registry["nse_eod"]      = NseEodFetcher(ETFS, STOCKS)
    registry["indian_macro"] = IndianMacroFetcher()
    registry["amfi_flows"]   = AmfiCategoryFlowsFetcher()
    registry["mf"]      = MFNavFetcher(MF_SCHEME_CODES)
    registry["fii_dii"] = FIIDIIFetcher()
    registry["fx_rates"] = FXRatesFetcher()
    registry["cot"]     = COTGoldFetcher()
    registry["cb_reserves"] = CbReservesFetcher()
    registry["etf_aum"]     = EtfAumFetcher()
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
