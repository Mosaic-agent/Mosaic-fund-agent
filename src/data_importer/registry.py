"""
src/importer/registry.py
────────────────────────
Symbol registry for the historical data importer.

Each category holds a list of (nse_symbol, yahoo_ticker) tuples.
The importer iterates over categories, fetches OHLCV data from yfinance,
and stores it in ClickHouse.

MF NAV is handled separately via MFAPI.in using AMFI scheme codes.
"""

from __future__ import annotations

# ── Category definitions ──────────────────────────────────────────────────────
# Each entry: (nse_symbol, yahoo_ticker)
# yahoo_ticker is what yfinance.download() expects.

STOCKS: list[tuple[str, str]] = [
    ("RELIANCE",    "RELIANCE.NS"),
    ("TCS",         "TCS.NS"),
    ("HDFCBANK",    "HDFCBANK.NS"),
    ("INFY",        "INFY.NS"),
    ("ICICIBANK",   "ICICIBANK.NS"),
    ("HINDUNILVR",  "HINDUNILVR.NS"),
    ("ITC",         "ITC.NS"),
    ("SBIN",        "SBIN.NS"),
    ("BHARTIARTL",  "BHARTIARTL.NS"),
    ("KOTAKBANK",   "KOTAKBANK.NS"),
    ("LT",          "LT.NS"),
    ("AXISBANK",    "AXISBANK.NS"),
    ("ASIANPAINT",  "ASIANPAINT.NS"),
    ("MARUTI",      "MARUTI.NS"),
    ("BAJFINANCE",  "BAJFINANCE.NS"),
    ("BAJAJFINSV",  "BAJAJFINSV.NS"),
    ("WIPRO",       "WIPRO.NS"),
    ("HCLTECH",     "HCLTECH.NS"),
    ("SUNPHARMA",   "SUNPHARMA.NS"),
    ("TECHM",       "TECHM.NS"),
    ("TMPV",        "TMPV.NS"),          # Tata Motors PV (demerged Nov 2025; was TATAMOTORS)
    ("TATASTEEL",   "TATASTEEL.NS"),
    ("NESTLEIND",   "NESTLEIND.NS"),
    ("ULTRACEMCO",  "ULTRACEMCO.NS"),
    ("TITAN",       "TITAN.NS"),
    ("POWERGRID",   "POWERGRID.NS"),
    ("NTPC",        "NTPC.NS"),
    ("ONGC",        "ONGC.NS"),
    ("COALINDIA",   "COALINDIA.NS"),
    ("JSWSTEEL",    "JSWSTEEL.NS"),
    ("ADANIPORTS",  "ADANIPORTS.NS"),
    ("ADANIENT",    "ADANIENT.NS"),
    ("GRASIM",      "GRASIM.NS"),
    ("EICHERMOT",   "EICHERMOT.NS"),
    ("HEROMOTOCO",  "HEROMOTOCO.NS"),
    ("APOLLOHOSP",  "APOLLOHOSP.NS"),
    ("CIPLA",       "CIPLA.NS"),
    ("DRREDDY",     "DRREDDY.NS"),
    ("DIVISLAB",    "DIVISLAB.NS"),
    ("BRITANNIA",   "BRITANNIA.NS"),
    ("HINDALCO",    "HINDALCO.NS"),
    ("INDUSINDBK",  "INDUSINDBK.NS"),
    ("TATACONSUM",  "TATACONSUM.NS"),
    ("BAJAJ-AUTO",  "BAJAJ-AUTO.NS"),
    ("BPCL",        "BPCL.NS"),
    ("IOC",         "IOC.NS"),
    ("M&M",         "M&M.NS"),
    ("MPHASIS",     "MPHASIS.NS"),
    ("PERSISTENT",  "PERSISTENT.NS"),
    ("LTIM",        "540005.BO"),        # LTIMindtree — LTIM.NS broken on Yahoo, use BSE code
    ("GARFIBRES",   "GARFIBRES.NS"),     # Garware Technical Fibres — quality smallcap, beta≈0
    ("GRWRHITECH",  "GRWRHITECH.NS"),    # Garware Hi-Tech Films — polyester / sun-control films
    ("WELCORP",     "WELCORP.NS"),       # Welspun Corp — line pipes / steel
    ("TEJASNET",    "TEJASNET.NS"),      # Tejas Networks — telecom equipment (Tata-backed)
    ("ADVENZYMES",  "ADVENZYMES.NS"),    # Advanced Enzyme Technologies — specialty enzymes
    ("ZENTEC",      "ZENTEC.NS"),        # Zen Technologies — defence simulators
    ("VAIBHAVGBL",  "VAIBHAVGBL.NS"),    # Vaibhav Global — jewellery / TV retail (US/UK)
    ("NUVAMA",      "NUVAMA.NS"),        # Nuvama Wealth Mgmt (ex-Edelweiss)
    ("NEWGEN",      "NEWGEN.NS"),        # Newgen Software — BPM / ECM platforms
    ("CHOLAFIN",    "CHOLAFIN.NS"),      # Cholamandalam Investment & Finance (Murugappa)
    ("ALKYLAMINE",  "ALKYLAMINE.NS"),    # Alkyl Amines Chemicals — specialty amines
    ("BECTORFOOD",  "BECTORFOOD.NS"),    # Mrs. Bectors Food Specialities Limited
]

ETFS: list[tuple[str, str]] = [
    ("NIFTYBEES",   "NIFTYBEES.NS"),
    ("JUNIORBEES",  "JUNIORBEES.NS"),
    ("GOLDBEES",    "GOLDBEES.NS"),
    ("LIQUIDBEES",  "LIQUIDBEES.NS"),
    ("BANKBEES",    "BANKBEES.NS"),
    ("PSUBNKBEES",  "PSUBNKBEES.NS"),
    ("SILVERBEES",  "SILVERBEES.NS"),
    ("HNGSNGBEES",  "HNGSNGBEES.NS"),
    ("MAFANG",      "MAFANG.NS"),
    ("HDFCNIFTY",   "HDFCNIFTY.NS"),
    ("SETFNIF50",   "SETFNIF50.NS"),
    ("ICICIB22",    "ICICIB22.NS"),
    ("MON100",      "MON100.NS"),
    ("MAHKTECH",    "MAHKTECH.NS"),
    ("MASPTOP50",   "MASPTOP50.NS"),
    ("MONQ50",      "MONQ50.NS"),
    ("GOLDCASE",    "GOLDCASE.NS"),
    ("SILVERCASE",  "SILVERCASE.NS"),
    ("TOP100CASE",  "TOP100CASE.NS"),
    ("MID150CASE",  "MID150CASE.NS"),
    ("SMALLCAP",    "SMALLCAP.NS"),
    ("LTGILTCASE",  "LTGILTCASE.NS"),
]

# Commodities via Yahoo Finance (XAU/USD, XAG/USD, etc.)
COMMODITIES: list[tuple[str, str]] = [
    ("GOLD",     "GC=F"),      # Gold futures (USD/troy oz)
    ("SILVER",   "SI=F"),      # Silver futures
    ("COPPER",   "HG=F"),      # Copper futures
    ("PLATINUM", "PL=F"),      # Platinum futures
    ("PALLADIUM","PA=F"),      # Palladium futures
    ("CRUDEOIL", "CL=F"),      # WTI Crude Oil futures
    ("NGAS",     "NG=F"),      # Natural Gas futures
]

# USD FX rates (for gold fund local-currency return analysis)
FX_PAIRS: list[tuple[str, str]] = [
    ("USDINR", "USDINR=X"),   # USD / Indian Rupee      — primary local currency
    ("USDCNY", "USDCNY=X"),   # USD / Chinese Yuan      — demand-side pressure
    ("USDAED", "USDAED=X"),   # USD / UAE Dirham        — Gulf peg
    ("USDSAR", "USDSAR=X"),   # USD / Saudi Riyal       — Gulf peg
    ("USDKWD", "USDKWD=X"),   # USD / Kuwaiti Dinar     — strongest Gulf currency
]

# US-listed stocks (NASDAQ / NYSE)
US_STOCKS: list[tuple[str, str]] = [
    ("ADSK",  "ADSK"),   # Autodesk Inc. — NASDAQ
    ("SLV",   "SLV"),    # iShares Silver Trust ETF — NYSE Arca
]

# ── Indices (Yahoo Finance) ────────────────────────────────────────────────────
# Only indices verified to return data from Yahoo Finance.
# For NSE indices NOT on Yahoo Finance, see NSE_ONLY_INDICES below.
INDICES: list[tuple[str, str]] = [
    # ── Indian — Broad Market ──────────────────────────────────────────────
    ("NIFTY50",        "^NSEI"),
    ("NIFTYNEXT50",    "^NSMIDCP"),           # Nifty Next 50
    ("NIFTY100",       "^CNX100"),
    ("NIFTY200",       "^CNX200"),
    ("NIFTY500",       "^CRSLDX"),            # Nifty 500
    ("NIFTYMID",       "^NSEMDCP50"),         # Nifty Midcap 50
    ("NIFTYSC",        "^CNXSC"),             # Nifty Smallcap composite

    # ── Indian — Sectoral ──────────────────────────────────────────────────
    ("BANKNIFTY",      "^NSEBANK"),
    ("NIFTYPSUBANK",   "^CNXPSUBANK"),
    ("NIFTYIT",        "^CNXIT"),
    ("NIFTYPHARMA",    "^CNXPHARMA"),
    ("NIFTYAUTO",      "^CNXAUTO"),
    ("NIFTYFMCG",      "^CNXFMCG"),
    ("NIFTYFINSRV",    "^CNXFIN"),            # Nifty Financial Services
    ("NIFTYMETAL",     "^CNXMETAL"),
    ("NIFTYMEDIA",     "^CNXMEDIA"),
    ("NIFTYREALTY",    "^CNXREALTY"),

    # ── Indian — Thematic ──────────────────────────────────────────────────
    ("NIFTYENERGY",    "^CNXENERGY"),
    ("NIFTYINFRA",     "^CNXINFRA"),
    ("NIFTYCMDTY",     "^CNXCMDT"),           # Nifty Commodities
    ("NIFTYSERVICE",   "^CNXSERVICE"),        # Nifty Services Sector
    ("NIFTYCONSUMP",   "^CNXCONSUM"),         # Nifty India Consumption
    ("NIFTYPSE",       "^CNXPSE"),
    ("NIFTYMNC",       "^CNXMNC"),

    # ── Volatility ─────────────────────────────────────────────────────────
    ("INDIAVIX",       "^INDIAVIX"),
    ("VIX",            "^VIX"),

    # ── Global ─────────────────────────────────────────────────────────────
    ("SENSEX",         "^BSESN"),
    ("SP500",          "^GSPC"),
    ("NASDAQ",         "^IXIC"),
    ("DOWJONES",       "^DJI"),
    ("US10Y",          "^TNX"),               # US 10-year Treasury yield
    ("US13W",          "^IRX"),               # US 13-week T-bill yield
    ("DXY",            "DX-Y.NYB"),           # ICE US Dollar Index (DXY spot)
]

# ── NSE-only indices (no Yahoo Finance ticker) ────────────────────────────────
# Fetched via nselib.capital_market.index_data().
# Format: (internal_symbol, nse_api_index_name)
NSE_ONLY_INDICES: list[tuple[str, str]] = [
    # ── Broad Market (cap-based) ───────────────────────────────────────────
    ("NIFTYMIDCAP100",   "NIFTY MIDCAP 100"),
    ("NIFTYMIDCAP150",   "NIFTY MIDCAP 150"),
    ("NIFTYSC50",        "NIFTY SMALLCAP 50"),
    ("NIFTYSC100",       "NIFTY SMALLCAP 100"),
    ("NIFTYSC250",       "NIFTY SMALLCAP 250"),
    ("NIFTYMICRO250",    "NIFTY MICROCAP 250"),
    ("NIFTYTOTALMARKET", "NIFTY TOTAL MARKET"),
    ("NIFTYLARGEMID250", "NIFTY LARGEMIDCAP 250"),
    ("NIFTYMIDSMALL400", "NIFTY MIDSMALLCAP 400"),

    # ── Sectoral ───────────────────────────────────────────────────────────
    ("NIFTYPVTBANK",     "NIFTY PRIVATE BANK"),
    ("NIFTYFINSRVEXBK",  "NIFTY FINANCIAL SERVICES EX-BANK"),
    ("NIFTYCONSDUR",     "NIFTY CONSUMER DURABLES"),
    ("NIFTYHEALTHCARE",  "NIFTY HEALTHCARE INDEX"),
    ("NIFTYOILGAS",      "NIFTY OIL & GAS"),
    ("NIFTYCEMENT",      "NIFTY CEMENT"),
    ("NIFTYCHEMICALS",   "NIFTY CHEMICALS"),

    # ── Thematic ───────────────────────────────────────────────────────────
    ("NIFTYEV",          "NIFTY EV & NEW AGE AUTOMOTIVE"),
    ("NIFTYDEFENCE",     "NIFTY INDIA DEFENCE"),
    ("NIFTYRAILWAY",     "NIFTY INDIA RAILWAYS PSU"),
    ("NIFTYCPSE",        "NIFTY CPSE"),
    ("NIFTYHOUSING",     "NIFTY HOUSING"),
    ("NIFTYDIGITAL",     "NIFTY INDIA DIGITAL"),
    ("NIFTYIPO",         "NIFTY IPO"),
    ("NIFTYREITS",       "NIFTY REITS & REALTY"),

    # ── Strategy / Factor ──────────────────────────────────────────────────
    ("NIFTY100ESG",       "NIFTY100 ESG"),
    ("NIFTY100ESGENH",    "NIFTY100 ENHANCED ESG"),
    ("NIFTY100ESGSECLDR", "NIFTY100 ESG SECTOR LEADERS"),
    ("NIFTY50SHARIAH",    "NIFTY50 SHARIAH"),
    ("NIFTY500SHARIAH",   "NIFTY500 SHARIAH"),
    ("NIFTYLOWVOL50",     "NIFTY LOW VOLATILITY 50"),
    ("NIFTYALPHA50",      "NIFTY ALPHA 50"),
    ("NIFTY100QUALITY30", "NIFTY100 QUALITY 30"),
]

# MFAPI.in scheme codes for MF NAV import
# Maps nse_symbol → AMFI scheme code
MF_SCHEME_CODES: dict[str, str] = {
    "GOLDBEES":   "140088",
    "NIFTYBEES":  "140084",
    "BANKBEES":   "140087",
    "JUNIORBEES": "140085",
    "LIQUIDBEES": "140086",
    "SILVERBEES": "149758",
    "HNGSNGBEES": "140095",
    "PSUBNKBEES": "140089",
    "MAFANG":     "148927",
    "HDFCNIFTY":  "135853",
    "SETFNIF50":  "135106",
    "MON100":     "114984",
    "MAHKTECH":   "149379",
    "MASPTOP50":  "149169",
    "MONQ50":     "149438",
}

# Watchlist for MF portfolio holdings via Morningstar sal-service API.
# Only include AMCs that do NOT have a dedicated importer in src/scripts/fund_imports/.
# Excluded (have own importers):
#   DSP_*   → src/scripts/dsp/import_all_dsp_equity.py + import_latest_dsp.py
#   ICICI_* → src/scripts/fund_imports/importers/icici_mf.py  (IciciMFImporter)
#   NIPPON_*/RELIANCE_*/NIMF_* → src/scripts/fund_imports/importers/nippon.py
#   BAJAJ_* → src/scripts/fund_imports/importers/bajaj.py (BajajImporter)
# Each entry: (amfi_scheme_code, short_name, isin_growth)
MF_HOLDINGS_WATCHLIST: list[tuple[str, str, str]] = [
    ("120821", "QUANT_MULTI_ASSET", "INF966L01580"),
    ("119843", "SBI_MULTI_ASSET", "INF200K01TZ3"),
    ("152064", "KOTAK_MULTI_ASSET", "INF174KA1PD4"),
    ("120334", "ICICI_MULTI_ASSET", "INF109K015K4"),
]

# ── Registry lookup ────────────────────────────────────────────────────────────

CATEGORY_MAP: dict[str, list[tuple[str, str]]] = {
    "stocks":       STOCKS,
    "etfs":         ETFS,
    "commodities":  COMMODITIES,
    "indices":      INDICES,
    "nse_indices":  NSE_ONLY_INDICES,
    "fx_rates":     FX_PAIRS,
    "us_stocks":    US_STOCKS,
}

# Symbols for which NSE live iNAV snapshots are captured
INAV_SYMBOLS: list[str] = [
    # ── Domestic ETFs (scanned by Domestic ETF Premium/Discount Scanner) ──────
    "GOLDBEES",   "NIFTYBEES",  "BANKBEES",   "JUNIORBEES", "LIQUIDBEES",
    "SILVERBEES", "PSUBNKBEES", "HDFCNIFTY",  "SETFNIF50",  "ICICIB22",
    "LIQUIDCASE", "CPSEETF",    "ITBEES",     "MID150BEES", "MONIFTY500",
    "GILT5YBEES", "PHARMABEES", "AUTOBEES",   "FMCGIETF",   "SMALL250",
    "GOLDCASE",
    "SILVERCASE", "TOP100CASE", "MID150CASE", "SMALLCAP", "LTGILTCASE",
    # ── International ETFs (scanned by Intl ETF Scarcity Premium Alerts) ─────
    "HNGSNGBEES", "MAFANG",     "MON100",     "MAHKTECH",   "MASPTOP50",  "MONQ50",
]

ALL_CATEGORIES = list(CATEGORY_MAP.keys()) + [
    "mf", "inav", "nse_eod", "cot", "cb_reserves", "etf_aum", "mf_holdings",
    "fii_dii", "amfi_flows", "earnings", "insider", "valuation",
    # Macro fundamentals (annual, free public APIs — no auth required)
    "world_bank", "imf_weo", "indian_macro", "indian_macro_indicators",
    # AMC fund-holdings importers (factory pattern in src/scripts/fund_imports/)
    "icici", "nippon", "icici-index", "dsp", "bajaj", "quant",
]


def get_symbols_for_categories(categories: list[str]) -> dict[str, list[tuple[str, str]]]:
    """
    Return {category: [(nse_symbol, yahoo_ticker), ...]} for the given categories.
    'mf' is excluded here — MF NAV uses a separate fetcher.
    """
    result: dict[str, list[tuple[str, str]]] = {}
    for cat in categories:
        if cat in CATEGORY_MAP:
            result[cat] = CATEGORY_MAP[cat]
    return result
