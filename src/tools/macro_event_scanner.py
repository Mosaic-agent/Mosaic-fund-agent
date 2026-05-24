"""
src/tools/macro_event_scanner.py
──────────────────────────────────
Macro & Geopolitical Event Scanner — maps live news to ETF/asset impact.

Monitors 8 macro themes:
  1. Geopolitical / War        → Gold ↑, Oil ↑, Equities ↓, INR ↓
  2. Central Bank Policy       → Debt ETFs ↑/↓, Equities, Gold
  3. Currency / INR            → IT ETFs (revenue), International ETFs
  4. Crude Oil Shock           → Inflation → RBI rate → broad market
  5. Trade War / Tariffs       → IT, Pharma, Export-oriented
  6. India Macro               → Nifty, PSU, Banking ETFs
  7. Commodity (Gold/Silver)   → GOLDBEES, SILVERBEES directly
  8. Global Equity Risk-Off    → International ETFs, Safe-haven Gold

For each detected event, shows:
  • Event headline & source
  • Theme classification
  • Transmission mechanism (why it matters)
  • Affected ETFs + expected direction (↑ bullish / ↓ bearish / ~ neutral)
  • Conviction: HIGH / MEDIUM / LOW

Sources: Google News RSS (gnews) + Yahoo Finance — no API key required.

Public API
──────────
    scan_macro_events(max_per_theme=4)  → MacroReport
    print_macro_report(report)          → None  (Rich console)
    run_macro_scan()                    → None  (CLI entry point)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

log = logging.getLogger(__name__)


# ── Macro theme definitions ──────────────────────────────────────────────────
# Each theme has:
#   queries       : Google News search terms (free, gnews)
#   yf_symbols    : Yahoo Finance tickers whose news to pull
#   keywords      : words that confirm the event belongs to this theme
#   impact_map    : ETF symbol → expected direction (+1 bullish, -1 bearish, 0 neutral)
#   transmission  : plain-English explanation of *why* this moves these ETFs
#   conviction    : default conviction level

MACRO_THEMES: list[dict] = [
    {
        "theme": "Geopolitical / War",
        "icon": "⚔️",
        "queries": [
            "US Iran war conflict military",
            "Russia Ukraine war ceasefire",
            "Middle East conflict oil supply",
            "geopolitical risk global war",
            "India China border tension",
            "Pakistan India military",
        ],
        "yf_symbols": ["GC=F", "CL=F"],   # Gold + Crude futures
        "keywords": {
            "war", "conflict", "strike", "missile", "military", "sanction",
            "ceasefire", "invasion", "attack", "iran", "russia", "ukraine",
            "tension", "geopolitic", "nato", "nuke", "nuclear", "troops",
        },
        "impact_map": {
            "GOLDBEES":   +1,   # Safe-haven bid
            "SILVERBEES": +1,   # Safe-haven
            "NIFTYBEES":  -1,   # Risk-off
            "BANKBEES":   -1,   # Risk-off
            "JUNIORBEES": -1,   # Risk-off
            "ITBEES":     -1,   # Global demand uncertainty
            "LIQUIDBEES": +1,   # Flight to safety
            "LIQUIDCASE": +1,   # Flight to safety
            "MON100":     -1,   # Global risk-off
            "MAFANG":     -1,   # Tech risk-off
        },
        "transmission": (
            "War/conflict → investors flee to safe havens (Gold ↑) and sell risky "
            "assets (equities ↓). Oil supply disruption raises inflation → RBI may "
            "delay rate cuts → bond prices fall. INR weakens → imported inflation."
        ),
        "conviction": "HIGH",
    },
    {
        "theme": "Central Bank Policy (Fed / RBI)",
        "icon": "🏦",
        "queries": [
            "US Federal Reserve interest rate decision",
            "RBI repo rate India monetary policy",
            "Fed rate cut hike pause 2026",
            "RBI MPC inflation India rate",
            "Jerome Powell Fed speech",
        ],
        "yf_symbols": ["^TNX", "^IRX"],    # US 10Y + 3M yield
        "keywords": {
            "fed", "federal reserve", "rbi", "repo rate", "rate cut", "rate hike",
            "monetary policy", "mpc", "inflation", "powell", "shaktikanta",
            "interest rate", "yield", "hawkish", "dovish", "pause",
        },
        "impact_map": {
            "GILT5YBEES": +1,   # Rate cut → bond prices rise
            "LIQUIDBEES": 0,    # Stable but yield adjusts
            "GOLDBEES":   +1,   # Rate cut → real yield falls → Gold ↑
            "NIFTYBEES":  +1,   # Rate cut → cheaper credit → equities ↑
            "BANKBEES":   +1,   # Rate cut → NIM pressure but loan growth ↑
            "ITBEES":     0,    # Indirect (via USD/INR and US demand)
            "MON100":     -1,   # Rate hike → Nasdaq ↓ (duration risk)
        },
        "transmission": (
            "Rate CUT: bond prices ↑ (GILT5YBEES ↑), real yield falls → Gold ↑, "
            "cheaper credit → equities ↑. Rate HIKE: opposite. "
            "RBI pause → status quo; watch for language on inflation path."
        ),
        "conviction": "HIGH",
    },
    {
        "theme": "Crude Oil Shock",
        "icon": "🛢️",
        "queries": [
            "crude oil price spike drop OPEC",
            "Brent WTI oil supply cut",
            "India crude oil import inflation",
            "OPEC production cut output",
            "oil price geopolitical supply",
        ],
        "yf_symbols": ["CL=F", "BZ=F"],    # WTI + Brent
        "keywords": {
            "crude", "oil", "opec", "brent", "wti", "petroleum", "energy",
            "barrel", "supply cut", "refinery", "gasoline", "fuel",
        },
        "impact_map": {
            "NIFTYBEES":  -1,   # Input cost inflation → margin pressure
            "BANKBEES":   -1,   # Inflation → RBI holds/hikes
            "GOLDBEES":   +1,   # Oil inflation → stagflation hedge
            "LIQUIDBEES": 0,    # Short-term safe parking
            "AUTOBEES":   -1,   # Fuel cost → demand destruction
            "PSUBNKBEES": 0,    # Mixed: oil PSUs ↑ but broader pain
            "ITBEES":     0,    # Mainly USD/INR dependent
        },
        "transmission": (
            "Oil spike → India imports ~85% of crude → trade deficit widens → INR "
            "weakens → imported inflation → RBI delays rate cuts → equities under "
            "pressure. Stagflation scenario: Gold as hedge."
        ),
        "conviction": "HIGH",
    },
    {
        "theme": "Currency / INR Move",
        "icon": "💱",
        "queries": [
            "Indian rupee dollar USDINR depreciation",
            "India forex reserves RBI dollar intervention",
            "rupee weakens strengthens DXY dollar",
            "India current account deficit CAD",
        ],
        "yf_symbols": ["USDINR=X", "DX-Y.NYB"],
        "keywords": {
            "rupee", "usdinr", "inr", "forex", "dollar", "currency", "dxy",
            "depreciation", "appreciation", "rbi intervention", "fii outflow",
            "current account", "capital flows",
        },
        "impact_map": {
            "ITBEES":     +1,   # Rupee weak → IT exports more valuable in INR
            "GOLDBEES":   +1,   # Rupee weak → Gold in INR rises
            "SILVERBEES": +1,   # Same as Gold
            "NIFTYBEES":  -1,   # Rupee weak → FII outflow → equities ↓
            "BANKBEES":   -1,   # FII outflow pressure
            "MON100":     -1,   # INR depreciation erodes USD-denominated returns
            "HNGSNGBEES": -1,   # Same
            "MAFANG":     -1,   # Same
        },
        "transmission": (
            "Rupee WEAKENS: Gold & IT ETFs ↑ (export revenue, INR-priced gold). "
            "Domestic equities ↓ (FII outflow, imported inflation). "
            "International ETFs ↓ (INR depreciation erodes returns for Indian investor)."
        ),
        "conviction": "MEDIUM",
    },
    {
        "theme": "Trade War / Tariffs",
        "icon": "⚖️",
        "queries": [
            "US China trade war tariff 2026",
            "India US trade deal tariff",
            "WTO trade dispute export ban",
            "US tariff reciprocal India",
            "China export restriction rare earth",
        ],
        "yf_symbols": ["^NDX", "^GSPC"],
        "keywords": {
            "tariff", "trade war", "sanction", "export ban", "import duty",
            "wto", "trade deal", "reciprocal", "protectionism", "supply chain",
            "decoupling", "china", "trade deficit",
        },
        "impact_map": {
            "ITBEES":      -1,  # US visa/outsourcing restrictions
            "NIFTYBEES":   -1,  # Sentiment hit, FII outflow
            "MON100":      -1,  # US-China trade war → Nasdaq ↓
            "MAFANG":      -1,  # FANG exposed to China/trade
            "GOLDBEES":    +1,  # Uncertainty → safe haven
            "PHARMABEES":  -1,  # US drug pricing / export restrictions
            "AUTOBEES":    -1,  # Component supply chain disruption
            "LIQUIDBEES":  +1,  # Flight to safety / cash
        },
        "transmission": (
            "Trade war → supply chain disruption → inflation → growth slowdown. "
            "Indian IT at risk from US visa/outsourcing policy. Gold benefits from "
            "uncertainty. Tech/export sectors face earnings risk."
        ),
        "conviction": "MEDIUM",
    },
    {
        "theme": "India Macro (GDP / Budget / Policy)",
        "icon": "🇮🇳",
        "queries": [
            "India GDP growth forecast 2026",
            "India budget fiscal deficit capex",
            "India inflation CPI WPI data",
            "India industrial production IIP PMI",
            "SEBI regulation India market reform",
        ],
        "yf_symbols": ["^NSEI", "^NSEBANK"],
        "keywords": {
            "india gdp", "india budget", "fiscal deficit", "capex", "india cpi",
            "india wpi", "iip", "india pmi", "sebi", "india reform", "india growth",
            "divestment", "india infrastructure", "india consumption",
        },
        "impact_map": {
            "NIFTYBEES":  +1,   # Strong GDP → earnings growth
            "BANKBEES":   +1,   # Credit growth follows GDP
            "CPSEETF":    +1,   # Capex → PSU order books
            "JUNIORBEES": +1,   # Mid/small cap benefit from domestic growth
            "SMALL250":   +1,   # Same
            "GILT5YBEES": -1,   # Higher govt borrowing → yields rise → bond price ↓
            "AUTOBEES":   +1,   # Domestic consumption
            "FMCGIETF":   +1,   # Rural demand, consumption
        },
        "transmission": (
            "Strong India macro → corporate earnings ↑ → broad equity ETFs ↑. "
            "High fiscal deficit → more g-sec supply → gilt yields rise → GILT5YBEES ↓. "
            "Infrastructure capex → CPSE, PSU ETFs ↑."
        ),
        "conviction": "MEDIUM",
    },
    {
        "theme": "Gold / Commodity Specific",
        "icon": "🥇",
        "queries": [
            "gold price record high 2026",
            "central bank gold buying reserves",
            "gold ETF AUM inflow outflow",
            "India gold import duty smuggling",
            "gold silver ratio trend",
            "World Gold Council demand report",
        ],
        "yf_symbols": ["GC=F", "SI=F", "GLD", "IAU"],
        "keywords": {
            "gold", "silver", "bullion", "precious metal", "xau", "xag",
            "goldbees", "central bank", "gold reserve", "gold etf", "wgc",
            "gold demand", "gold import", "gold mine",
        },
        "impact_map": {
            "GOLDBEES":   +1,
            "SILVERBEES": +1,
        },
        "transmission": (
            "Direct gold price drivers: DXY direction, real US yields, central bank "
            "buying, geopolitical risk, India import duty changes, and ETF flow "
            "momentum (AUM inflows = buying pressure on the underlying)."
        ),
        "conviction": "HIGH",
    },
    {
        "theme": "Global Risk-Off / Equity Sell-Off",
        "icon": "📉",
        "queries": [
            "global stock market crash correction 2026",
            "FII DII India equity outflow selling",
            "Nasdaq S&P 500 bear market",
            "VIX volatility index spike fear",
            "emerging market selloff India FPI",
        ],
        "yf_symbols": ["^VIX", "^GSPC", "^NDX"],
        "keywords": {
            "crash", "correction", "selloff", "bear market", "vix", "fear",
            "volatility", "fii outflow", "fpi selling", "risk off", "panic",
            "recession", "slowdown", "stagflation",
        },
        "impact_map": {
            "GOLDBEES":   +1,   # Safe haven
            "SILVERBEES": +1,   # Safe haven (weaker)
            "LIQUIDBEES": +1,   # Cash parking
            "LIQUIDCASE": +1,   # Cash parking
            "NIFTYBEES":  -1,
            "BANKBEES":   -1,
            "JUNIORBEES": -1,
            "ITBEES":     -1,
            "MON100":     -1,
            "MAFANG":     -1,
            "HNGSNGBEES": -1,
            "SMALL250":   -1,
        },
        "transmission": (
            "Global risk-off → FII sells India equities (India is EM) → Nifty/Bank "
            "ETFs fall. Gold and liquid ETFs are the beneficiaries. Small/mid cap "
            "falls harder than large cap in risk-off."
        ),
        "conviction": "HIGH",
    },
]

# ── Sentiment keywords ────────────────────────────────────────────────────────
_POS_WORDS = {
    "ceasefire", "peace", "deal", "agreement", "cut rate", "easing", "recovery",
    "growth", "rally", "surge", "strong", "bullish", "inflow", "gain", "rise",
    "record high", "beat", "upgrade", "buy", "approval",
}
_NEG_WORDS = {
    "war", "conflict", "attack", "strike", "crash", "sell", "bearish", "outflow",
    "decline", "fall", "hike", "tighten", "sanction", "ban", "default", "plunge",
    "recession", "stagflation", "correction", "fear", "panic", "slowdown",
}


def _sentiment(text: str) -> str:
    t = text.lower()
    pos = sum(1 for w in _POS_WORDS if w in t)
    neg = sum(1 for w in _NEG_WORDS if w in t)
    if pos > neg:   return "POSITIVE"
    if neg > pos:   return "NEGATIVE"
    return "NEUTRAL"


def _theme_score(text: str, keywords: set[str]) -> int:
    """Count how many theme keywords appear in the text."""
    t = text.lower()
    return sum(1 for kw in keywords if kw in t)


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class MacroEvent:
    headline: str
    source: str
    published_at: str
    url: str
    theme: str
    icon: str
    sentiment: str
    transmission: str
    conviction: str
    fetch_source: str
    impact: dict[str, int]          # ETF → +1 / -1 / 0
    theme_score: int = 0            # keyword match count (relevance)


@dataclass
class MacroFundamentals:
    """
    Annual macro indicators for India from World Bank WDI / IMF WEO,
    read from market_data.macro_indicators in ClickHouse.
    All fields None if the table is not yet populated or DB is unreachable.
    Populate with: mosaic import --category world_bank,imf_weo
    """
    gdp_growth_pct:     float | None = None  # India real GDP growth % (latest WB actual)
    cpi_pct:            float | None = None  # India CPI inflation % (latest WB actual)
    ca_balance_pct:     float | None = None  # Current account balance (% of GDP)
    fiscal_balance_pct: float | None = None  # Net govt lending/borrowing (% of GDP)
    gdp_forecast_pct:   float | None = None  # IMF WEO nearest future-year GDP growth
    cpi_forecast_pct:   float | None = None  # IMF WEO nearest future-year CPI
    actual_year:        int | None   = None  # Year of the latest actual data
    forecast_year:      int | None   = None  # Year of the IMF forecast


@dataclass
class QuantOverlay:
    """Live quantitative context fetched from ClickHouse — validates / grounds news signals."""
    fii_net_5d_cr: float | None = None       # 5-day FII net flow (₹ Cr) — positive = buying
    dii_net_5d_cr: float | None = None       # 5-day DII net flow (₹ Cr)
    goldbees_vs_ema50: str | None = None     # 'above' | 'below'
    goldbees_close: float | None = None
    goldbees_5d_logret_pct: float | None = None
    garch_vol_pct: float | None = None       # annualised GARCH vol for GOLDBEES
    data_as_of: str | None = None
    macro_fundamentals: MacroFundamentals = field(default_factory=MacroFundamentals)


@dataclass
class MacroReport:
    as_of: str
    events: list[MacroEvent] = field(default_factory=list)
    themes_detected: list[str] = field(default_factory=list)
    etf_net_signal: dict[str, int] = field(default_factory=dict)   # aggregated across all events
    quant: QuantOverlay = field(default_factory=QuantOverlay)


# ── Macro fundamentals from ClickHouse ───────────────────────────────────────

def _is_macro_stale(mf: MacroFundamentals) -> bool:
    """
    Return True if the stored macro fundamentals need a refresh.

    Staleness criteria (any one triggers auto-import):
    - Table is empty (actual_year is None)
    - Latest actual is older than current_year − 2  (World Bank has ~12mo lag,
      so current_year − 1 is normal; current_year − 2 means genuinely stale)
    - No IMF WEO forecast exists for the current year or ahead
    """
    current_year = date.today().year
    if mf.actual_year is None:
        return True
    if mf.actual_year < current_year - 2:
        return True
    if mf.forecast_year is None or mf.forecast_year < current_year:
        return True
    return False


def _auto_import_macro() -> None:
    """
    Fetch fresh World Bank WDI + IMF WEO data and insert into ClickHouse.
    Called automatically by _fetch_macro_fundamentals() when data is stale.
    Bypasses the full CLI orchestrator — uses fetchers + ClickHouseImporter directly.
    """
    current_year = date.today().year
    log.info(
        "Macro fundamentals stale — auto-importing world_bank + imf_weo "
        "(this may take ~15s for first run)…"
    )
    try:
        from config.settings import settings
        from src.importer.clickhouse import ClickHouseImporter
        from src.importer.fetchers.worldbank_macro_fetcher import fetch_worldbank_macro
        from src.importer.fetchers.imf_weo_fetcher import fetch_imf_weo

        ch = ClickHouseImporter(
            host     = settings.clickhouse_host,
            port     = settings.clickhouse_port,
            database = settings.clickhouse_database,
            username = settings.clickhouse_user,
            password = settings.clickhouse_password,
        )
        ch.ensure_schema()

        wb_rows = fetch_worldbank_macro(from_year=2000, to_year=current_year)
        if wb_rows:
            ch.insert_macro_indicators(wb_rows)
            ch.set_watermark(
                "world_bank", "MACRO_GROUP",
                date(max(int(r["ref_year"]) for r in wb_rows), 12, 31),
            )
            log.info("Auto-imported %d World Bank macro rows", len(wb_rows))
        else:
            log.warning("World Bank API returned no rows during auto-import")

        imf_rows = fetch_imf_weo(from_year=2000, to_year=current_year + 3)
        if imf_rows:
            ch.insert_macro_indicators(imf_rows)
            actual_imf = [r for r in imf_rows if not r.get("is_forecast", 0)]
            if actual_imf:
                ch.set_watermark(
                    "imf_weo", "MACRO_GROUP",
                    date(max(int(r["ref_year"]) for r in actual_imf), 12, 31),
                )
            log.info("Auto-imported %d IMF WEO rows", len(imf_rows))
        else:
            log.warning("IMF WEO API returned no rows during auto-import")

    except Exception as exc:
        log.warning("Auto-import of macro fundamentals failed: %s", exc)


def _fetch_macro_fundamentals() -> MacroFundamentals:
    """
    Read latest India annual macro indicators from market_data.macro_indicators.
    If the data is stale (empty table, data too old, or no IMF forecasts for
    the current year), automatically triggers a World Bank + IMF WEO import
    before re-querying.
    Returns empty MacroFundamentals if DB is unreachable.
    """
    try:
        from src.db.pool import get_pool as _get_ch_pool
        _pool = _get_ch_pool()
        current_year = date.today().year

        def _query() -> MacroFundamentals:
            with _pool.acquire() as cl:
                df = cl.query_df("""
                    SELECT indicator_code, ref_year, value, source, is_forecast
                    FROM market_data.macro_indicators FINAL
                    WHERE country_code = 'IN'
                      AND indicator_code IN (
                          'NY.GDP.MKTP.KD.ZG', 'FP.CPI.TOTL.ZG',
                          'BN.CAB.XOKA.GD.ZS', 'GGXCNL_NGDP',
                          'NGDP_RPCH', 'PCPIPCH', 'BCA_NGDPDZ'
                      )
                    ORDER BY ref_year DESC
                    LIMIT 200
                """)

            if df.empty:
                return MacroFundamentals()

            actuals   = df[df["is_forecast"] == 0]
            forecasts = df[df["is_forecast"] == 1]

            def _latest_actual(codes: list[str]) -> tuple[float | None, int | None]:
                sub = actuals[actuals["indicator_code"].isin(codes)]
                if sub.empty:
                    return None, None
                row = sub.sort_values("ref_year", ascending=False).iloc[0]
                return float(row["value"]), int(row["ref_year"])

            def _nearest_forecast(codes: list[str]) -> tuple[float | None, int | None]:
                sub = forecasts[
                    forecasts["indicator_code"].isin(codes) &
                    (forecasts["ref_year"] >= current_year)
                ]
                if sub.empty:
                    return None, None
                row = sub.sort_values("ref_year").iloc[0]
                return float(row["value"]), int(row["ref_year"])

            gdp_val,   actual_yr = _latest_actual(["NY.GDP.MKTP.KD.ZG", "NGDP_RPCH"])
            cpi_val,   _         = _latest_actual(["FP.CPI.TOTL.ZG", "PCPIPCH"])
            ca_val,    _         = _latest_actual(["BN.CAB.XOKA.GD.ZS", "BCA_NGDPDZ"])
            fis_val,   _         = _latest_actual(["GGXCNL_NGDP"])
            gdp_fcast, fcast_yr  = _nearest_forecast(["NGDP_RPCH"])
            cpi_fcast, _         = _nearest_forecast(["PCPIPCH"])

            return MacroFundamentals(
                gdp_growth_pct     = round(gdp_val,   2) if gdp_val   is not None else None,
                cpi_pct            = round(cpi_val,   2) if cpi_val   is not None else None,
                ca_balance_pct     = round(ca_val,    2) if ca_val    is not None else None,
                fiscal_balance_pct = round(fis_val,   2) if fis_val   is not None else None,
                gdp_forecast_pct   = round(gdp_fcast, 2) if gdp_fcast is not None else None,
                cpi_forecast_pct   = round(cpi_fcast, 2) if cpi_fcast is not None else None,
                actual_year        = actual_yr,
                forecast_year      = fcast_yr,
            )

        # First query attempt — table may not exist yet on a fresh install.
        # Treat any DB error the same as an empty result (stale) so auto-import fires.
        try:
            mf = _query()
        except Exception as exc:
            log.info("macro_indicators table missing or unreadable (%s) — will auto-import", exc)
            mf = MacroFundamentals()

        if _is_macro_stale(mf):
            log.info(
                "Macro fundamentals stale (actual_year=%s, forecast_year=%s) — refreshing…",
                mf.actual_year, mf.forecast_year,
            )
            _auto_import_macro()
            try:
                mf = _query()
            except Exception as exc:
                log.warning("Second macro query failed after auto-import: %s", exc)

        return mf
    except Exception as exc:
        log.warning("MacroFundamentals fetch failed: %s", exc)
        return MacroFundamentals()


# ── Quant overlay from ClickHouse ─────────────────────────────────────────────

def _fetch_quant_overlay() -> QuantOverlay:
    """
    Pull live quantitative context from ClickHouse to ground news signals.
    Degrades gracefully — returns empty QuantOverlay if DB is unavailable.

    Perf note: uses two ClickHouse queries only. GARCH vol is read from the
    weight_checkpoints table (pre-computed by the risk pipeline) rather than
    fitting a GARCH model here — avoids a 2-3s computation on every macro scan.
    """
    try:
        import numpy as np
        from concurrent.futures import ThreadPoolExecutor

        # Each concurrent query gets its own pooled connection — the pool's
        # acquire() context manager guarantees exclusive per-thread checkout.
        from src.db.pool import get_pool as _get_ch_pool
        _pool = _get_ch_pool()

        def _fii():
            with _pool.acquire() as cl:
                return cl.query_df("""
                    SELECT
                        sum(fii_net_cr) AS fii_net_5d,
                        sum(dii_net_cr) AS dii_net_5d
                    FROM market_data.fii_dii_flows FINAL
                    WHERE trade_date >= today() - INTERVAL 5 DAY
                """)

        def _prices():
            with _pool.acquire() as cl:
                return cl.query_df("""
                    SELECT trade_date,
                           toFloat64(argMax(close, imported_at)) AS close
                    FROM market_data.daily_prices
                    WHERE symbol = 'GOLDBEES' AND category = 'etfs'
                    GROUP BY trade_date ORDER BY trade_date DESC LIMIT 55
                """)

        def _garch():
            # Read pre-computed GARCH vol from the last weight checkpoint row —
            # avoids a 2-3s GARCH fit on every macro scan.
            try:
                with _pool.acquire() as cl:
                    df = cl.query_df("""
                        SELECT garch_vol_pct FROM market_data.weight_checkpoints FINAL
                        WHERE symbol = 'GOLDBEES' AND garch_vol_pct > 0
                        ORDER BY as_of DESC LIMIT 1
                    """)
                return float(df["garch_vol_pct"].iloc[0]) if not df.empty else None
            except Exception:
                return None

        with ThreadPoolExecutor(max_workers=4) as ex:
            f_fii    = ex.submit(_fii)
            f_prices = ex.submit(_prices)
            f_garch  = ex.submit(_garch)
            f_macro  = ex.submit(_fetch_macro_fundamentals)
            fii_df        = f_fii.result()
            price_df      = f_prices.result()
            garch_vol_raw = f_garch.result()
            macro_funds   = f_macro.result()

        fii_net = float(fii_df["fii_net_5d"].iloc[0]) if not fii_df.empty else None
        dii_net = float(fii_df["dii_net_5d"].iloc[0]) if not fii_df.empty else None

        if price_df.empty:
            return QuantOverlay()

        price_df    = price_df.sort_values("trade_date").reset_index(drop=True)
        closes      = price_df["close"].astype(float)
        latest      = float(closes.iloc[-1])
        ema50       = float(closes.ewm(span=50, adjust=False).mean().iloc[-1])
        vs_ema50    = "above" if latest >= ema50 else "below"
        ret5d       = None
        if len(closes) >= 6:
            ret5d = round(float(np.log(latest / closes.iloc[-6])) * 100, 2)
        data_date   = str(price_df["trade_date"].iloc[-1].date()
                         if hasattr(price_df["trade_date"].iloc[-1], "date")
                         else price_df["trade_date"].iloc[-1])[:10]
        garch_vol   = round(garch_vol_raw, 1) if garch_vol_raw is not None else None

        return QuantOverlay(
            fii_net_5d_cr          = round(fii_net, 1) if fii_net is not None else None,
            dii_net_5d_cr          = round(dii_net, 1) if dii_net is not None else None,
            goldbees_vs_ema50      = vs_ema50,
            goldbees_close         = round(latest, 2),
            goldbees_5d_logret_pct = ret5d,
            garch_vol_pct          = garch_vol,
            data_as_of             = data_date,
            macro_fundamentals     = macro_funds,
        )
    except Exception as exc:
        log.debug("Quant overlay unavailable: %s", exc)
        return QuantOverlay()


# ── Fetchers ──────────────────────────────────────────────────────────────────

def _gnews_fetch(query: str, max_results: int = 5) -> list[dict]:
    try:
        from gnews import GNews
        client = GNews(language="en", country="US", max_results=max_results, period="2d")
        results = client.get_news(query) or []
        # Also try India-specific
        client_in = GNews(language="en", country="IN", max_results=max_results, period="2d")
        results_in = client_in.get_news(query) or []
        return results + results_in
    except Exception as exc:
        log.debug("gnews error for '%s': %s", query, exc)
        return []


def _yf_news_fetch(symbol: str, max_results: int = 5) -> list[dict]:
    try:
        import yfinance as yf
        raw = yf.Ticker(symbol).news or []
        out = []
        for item in raw[:max_results]:
            content  = item.get("content", {})
            title    = content.get("title", "") or item.get("title", "")
            if not title:
                continue
            pub = content.get("pubDate", "") or item.get("providerPublishTime", "")
            if isinstance(pub, int):
                pub = datetime.fromtimestamp(pub).strftime("%Y-%m-%d %H:%M")
            provider = content.get("provider", {})
            source   = provider.get("displayName", symbol) if isinstance(provider, dict) else symbol
            url_info = content.get("canonicalUrl", {})
            url      = url_info.get("url", "") if isinstance(url_info, dict) else ""
            out.append({"title": title, "source": source,
                        "published date": str(pub), "url": url})
        return out
    except Exception as exc:
        log.debug("yfinance news error for '%s': %s", symbol, exc)
        return []


# ── Main scanner ──────────────────────────────────────────────────────────────

def scan_macro_events(max_per_theme: int = 4, progress_cb=None) -> MacroReport:
    """
    Fetch and classify macro/geopolitical events, map to ETF impact.

    Parameters
    ----------
    max_per_theme : max articles to keep per theme
    progress_cb   : optional callable(step: str) called at each major stage
                    (fires from worker threads — must be thread-safe)

    Returns MacroReport with per-event impact maps, an aggregated
    net ETF signal (sum of all event directions per ETF), and a
    QuantOverlay with live ClickHouse data to ground the news signal.

    Performance
    -----------
    All HTTP fetches (GNews + Yahoo Finance) run concurrently in a
    ThreadPoolExecutor. Duplicate YF tickers across themes are de-duplicated
    before fetching and their results fanned out to all relevant themes.
    Expected wall-clock time: 5-10s vs 60-70s sequential.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _cb(msg: str) -> None:
        if progress_cb:
            try:
                progress_cb(msg)
            except Exception:
                pass

    report = MacroReport(as_of=datetime.now().strftime("%Y-%m-%d %H:%M IST"))

    # ── Build all fetch tasks up-front ────────────────────────────────────────
    gnews_tasks: list[tuple[str, str]] = []
    yf_sym_to_themes: dict[str, list[str]] = {}

    for theme_def in MACRO_THEMES:
        tname = theme_def["theme"]
        for q in theme_def["queries"]:
            gnews_tasks.append((tname, q))
        for sym in theme_def.get("yf_symbols", []):
            yf_sym_to_themes.setdefault(sym, []).append(tname)

    theme_raw: dict[str, list[dict]] = {t["theme"]: [] for t in MACRO_THEMES}
    total_tasks = len(gnews_tasks) + len(yf_sym_to_themes)
    completed   = 0

    def _gnews_task(theme_name: str, query: str):
        articles = _gnews_fetch(query, max_results=max_per_theme)
        _cb(f"📰 GNews — {theme_name[:30]}")
        return "gnews", theme_name, articles

    def _yf_task(sym: str):
        articles = _yf_news_fetch(sym, max_results=max_per_theme)
        _cb(f"📈 Yahoo Finance — {sym}")
        return "yf", sym, articles

    _cb(f"🔍 Scanning {len(MACRO_THEMES)} macro themes via GNews + Yahoo Finance…")

    max_workers = min(32, total_tasks + 1)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        _cb("📐 Fetching quant overlay (FII flows, GOLDBEES, macro fundamentals)…")
        f_quant = executor.submit(_fetch_quant_overlay)

        for tname, q in gnews_tasks:
            futures.append(executor.submit(_gnews_task, tname, q))
        for sym in yf_sym_to_themes:
            futures.append(executor.submit(_yf_task, sym))

        for fut in as_completed(futures):
            try:
                result = fut.result()
                completed += 1
                if result[0] == "gnews":
                    _, theme_name, articles = result
                    theme_raw[theme_name].extend(articles)
                else:  # "yf"
                    _, sym, articles = result
                    for tname in yf_sym_to_themes[sym]:
                        theme_raw[tname].extend(articles)
            except Exception as exc:
                log.debug("Fetch task failed: %s", exc)

        _cb("⚙️  Scoring articles and mapping ETF impact…")
        report.quant = f_quant.result()

    # ── Score and assemble per-theme ──────────────────────────────────────────
    seen_titles: set[str] = set()
    etf_net: dict[str, int] = {}

    for theme_def in MACRO_THEMES:
        theme_name   = theme_def["theme"]
        theme_events: list[MacroEvent] = []

        for art in theme_raw[theme_name]:
            title = art.get("title", "")
            if not title:
                continue
            key = title[:60].lower()
            if key in seen_titles:
                continue
            score = _theme_score(
                f"{title} {art.get('description', '')}",
                theme_def["keywords"],
            )
            if score == 0:
                continue

            seen_titles.add(key)
            publisher = art.get("publisher", {})
            source    = publisher.get("title", "") if isinstance(publisher, dict) else art.get("source", "")
            pub_date  = str(art.get("published date", ""))

            event = MacroEvent(
                headline=title,
                source=source,
                published_at=pub_date,
                url=art.get("url", ""),
                theme=theme_name,
                icon=theme_def["icon"],
                sentiment=_sentiment(title),
                transmission=theme_def["transmission"],
                conviction=theme_def["conviction"],
                fetch_source="gnews" if "publisher" in art else "yfinance",
                impact=theme_def["impact_map"].copy(),
                theme_score=score,
            )
            theme_events.append(event)

        theme_events.sort(key=lambda e: -e.theme_score)
        top = theme_events[:max_per_theme]

        if top:
            report.themes_detected.append(theme_name)
            report.events.extend(top)
            for ev in top:
                for etf, direction in ev.impact.items():
                    etf_net[etf] = etf_net.get(etf, 0) + direction

    report.etf_net_signal = etf_net
    log.info(
        "Macro scan: %d events across %d themes",
        len(report.events), len(report.themes_detected),
    )
    _cb(
        f"✅ Scan complete — {len(report.events)} events across "
        f"{len(report.themes_detected)} themes"
    )
    return report


# ── Rich console printer ──────────────────────────────────────────────────────

def print_macro_report(report: MacroReport, max_per_theme: int = 4) -> None:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich import box
    from collections import defaultdict

    console = Console()

    # ── Header ────────────────────────────────────────────────────────────────
    console.print(Panel(
        f"[bold magenta]🌍 Macro & Geopolitical Event Scanner[/bold magenta]\n"
        f"[dim]{report.as_of}  •  {len(report.events)} events  •  "
        f"{len(report.themes_detected)} themes active  •  "
        f"Google News RSS + Yahoo Finance (no key)[/dim]",
        border_style="magenta",
    ))

    # ── Per-theme events ──────────────────────────────────────────────────────
    by_theme: dict[str, list[MacroEvent]] = defaultdict(list)
    for ev in report.events:
        by_theme[ev.theme].append(ev)

    _SENT = {"POSITIVE": ("🟢", "green"), "NEGATIVE": ("🔴", "red"), "NEUTRAL": ("⚪", "dim")}
    _DIR  = {+1: "[green]↑ BULLISH[/green]", -1: "[red]↓ BEARISH[/red]", 0: "[dim]~ NEUTRAL[/dim]"}
    _CONV = {"HIGH": "[bold red]HIGH[/bold red]", "MEDIUM": "[yellow]MEDIUM[/yellow]", "LOW": "[dim]LOW[/dim]"}

    for theme_name, events in by_theme.items():
        icon = events[0].icon
        conviction = events[0].conviction
        console.print(
            f"\n{icon}  [bold white]{theme_name}[/bold white]  "
            f"Conviction: {_CONV[conviction]}"
        )

        # Headlines table
        tbl = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
        tbl.add_column("s",    width=2)
        tbl.add_column("headline", style="white", no_wrap=False, ratio=7)
        tbl.add_column("source",   style="dim", ratio=2)
        tbl.add_column("date",     style="dim", ratio=2)

        for ev in events:
            icon_s, _ = _SENT[ev.sentiment]
            tbl.add_row(icon_s, ev.headline, ev.source, ev.published_at[:16])
        console.print(tbl)

        # Transmission mechanism
        console.print(f"  [dim]💡 Why it matters:[/dim] [italic dim]{events[0].transmission}[/italic dim]")

        # ETF impact
        impact_map = events[0].impact
        bullish = [etf for etf, d in impact_map.items() if d == +1]
        bearish = [etf for etf, d in impact_map.items() if d == -1]
        if bullish:
            console.print(f"  [green]↑ Bullish:[/green] {' '.join(bullish)}")
        if bearish:
            console.print(f"  [red]↓ Bearish:[/red]  {' '.join(bearish)}")

    # ── Aggregated ETF net signal ─────────────────────────────────────────────
    console.print("\n")
    console.print(Panel(
        "[bold]📊 Aggregated ETF Signal (all active macro themes)[/bold]",
        border_style="white",
        expand=False,
    ))

    net = report.etf_net_signal
    if net:
        sorted_etfs = sorted(net.items(), key=lambda x: -x[1])
        tbl2 = Table(box=box.SIMPLE, show_header=True, padding=(0, 1))
        tbl2.add_column("ETF",    style="cyan",  width=16)
        tbl2.add_column("Net Score", justify="right", width=10)
        tbl2.add_column("Signal",  width=16)
        tbl2.add_column("Bar", no_wrap=True)

        for etf, score in sorted_etfs:
            if score > 0:
                signal = "[green]BULLISH[/green]"
                bar    = "[green]" + "█" * min(score, 8) + "[/green]"
            elif score < 0:
                signal = "[red]BEARISH[/red]"
                bar    = "[red]" + "▼" * min(abs(score), 8) + "[/red]"
            else:
                signal = "[dim]NEUTRAL[/dim]"
                bar    = "[dim]─[/dim]"
            tbl2.add_row(etf, str(score), signal, bar)
        console.print(tbl2)

    # Scale note: max_per_theme × n_themes × max_weight ≈ 4 × 8 × 1 = 32 per side
    n_themes = len(report.themes_detected)
    max_score = max_per_theme * n_themes if n_themes else 32
    strong_thresh = max(4, max_score // 2)
    mod_thresh    = max(2, max_score // 4)
    console.print(
        f"\n[dim]Net score = article-count × impact-weight, summed across all themes "
        f"(max ≈ ±{max_score} with {n_themes} active themes, max_per_theme={max_per_theme}).  "
        f"≥+{strong_thresh} = strong bullish  |  ≥+{mod_thresh} = moderate  |  "
        f"≤-{strong_thresh} = strong bearish[/dim]"
    )

    # ── Quant overlay (ClickHouse ground truth) ───────────────────────────────
    q = report.quant
    has_quant = any([
        q.fii_net_5d_cr is not None,
        q.goldbees_close is not None,
        q.garch_vol_pct is not None,
    ])
    if has_quant:
        console.print()
        lines: list[str] = []

        if q.goldbees_close is not None:
            ret_str = ""
            if q.goldbees_5d_logret_pct is not None:
                col = "green" if q.goldbees_5d_logret_pct >= 0 else "red"
                ret_str = f"  [{col}]{q.goldbees_5d_logret_pct:+.2f}% (5d)[/{col}]"
            ema_col = "green" if q.goldbees_vs_ema50 == "above" else "red"
            lines.append(
                f"  GOLDBEES   ₹{q.goldbees_close:.2f}{ret_str}  "
                f"EMA50: [{ema_col}]{q.goldbees_vs_ema50}[/{ema_col}]"
                + (f"  GARCH vol: {q.garch_vol_pct:.1f}%" if q.garch_vol_pct else "")
            )

        if q.fii_net_5d_cr is not None:
            fii_col = "green" if q.fii_net_5d_cr >= 0 else "red"
            dii_str = ""
            if q.dii_net_5d_cr is not None:
                dii_col = "green" if q.dii_net_5d_cr >= 0 else "red"
                dii_str = f"  DII: [{dii_col}]{q.dii_net_5d_cr:+,.0f} Cr[/{dii_col}]"
            lines.append(
                f"  FII 5d net [{fii_col}]{q.fii_net_5d_cr:+,.0f} Cr[/{fii_col}]{dii_str}"
            )

        # ── Macro Fundamentals (World Bank / IMF WEO) ─────────────────────────
        mf = q.macro_fundamentals
        if mf.gdp_growth_pct is not None or mf.gdp_forecast_pct is not None:
            yr_label = f" ({mf.actual_year})" if mf.actual_year else ""
            gdp_col  = "green" if (mf.gdp_growth_pct or 0) >= 6.0 else "red"
            cpi_col  = "green" if (mf.cpi_pct or 99) <= 5.0 else "yellow"
            ca_col   = "green" if (mf.ca_balance_pct or 0) >= -2.0 else "red"

            parts: list[str] = []
            if mf.gdp_growth_pct is not None:
                parts.append(f"GDP [{gdp_col}]{mf.gdp_growth_pct:+.1f}%{yr_label}[/{gdp_col}]")
            if mf.cpi_pct is not None:
                parts.append(f"CPI [{cpi_col}]{mf.cpi_pct:+.1f}%[/{cpi_col}]")
            if mf.ca_balance_pct is not None:
                parts.append(f"CA [{ca_col}]{mf.ca_balance_pct:+.1f}% GDP[/{ca_col}]")
            if mf.fiscal_balance_pct is not None:
                fis_col = "green" if mf.fiscal_balance_pct >= -4.5 else "red"
                parts.append(f"Fiscal [{fis_col}]{mf.fiscal_balance_pct:+.1f}% GDP[/{fis_col}]")
            lines.append("  India Macro  " + "  ".join(parts))

            fcast_parts: list[str] = []
            if mf.gdp_forecast_pct is not None:
                fcast_parts.append(f"GDP {mf.gdp_forecast_pct:+.1f}%")
            if mf.cpi_forecast_pct is not None:
                fcast_parts.append(f"CPI {mf.cpi_forecast_pct:+.1f}%")
            if fcast_parts and mf.forecast_year:
                lines.append(
                    f"  IMF {mf.forecast_year} forecast  " + "  ".join(fcast_parts)
                )

        if lines:
            console.print(Panel(
                "[bold]📐 Quant Overlay — ClickHouse ground truth[/bold]  "
                + f"[dim](data as of {q.data_as_of})[/dim]\n"
                + "\n".join(lines),
                border_style="dim",
                expand=False,
            ))


# ── CLI entry point ───────────────────────────────────────────────────────────

def run_macro_scan(max_per_theme: int = 4) -> None:
    logging.basicConfig(level=logging.INFO)
    report = scan_macro_events(max_per_theme=max_per_theme)
    print_macro_report(report, max_per_theme=max_per_theme)


def save_macro_events_to_db(report: MacroReport, ch_client) -> int:
    """
    Persist a MacroReport to market_data.news_articles in ClickHouse.

    Parameters
    ----------
    report    : MacroReport returned by scan_macro_events()
    ch_client : clickhouse_connect client (already connected)

    Returns number of rows inserted.
    """
    from datetime import datetime
    fetched_at = datetime.now()
    rows = [
        {
            "fetched_at":    fetched_at,
            "published_at":  ev.published_at,
            "source_type":   "macro_event",
            "category":      ev.theme,
            "etfs_impacted": ",".join(ev.impact.keys()),
            "sentiment":     ev.sentiment,
            "impact_tier":   ev.conviction,
            "title":         ev.headline,
            "source":        ev.source,
            "url":           ev.url,
        }
        for ev in report.events
    ]
    if not rows:
        return 0
    n = ch_client.insert_news_articles(rows)
    log.info("Saved %d macro events to ClickHouse", n)
    return n


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
    run_macro_scan()
