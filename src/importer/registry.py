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
    ("TATAMOTORS",  "TATAMOTORS.NS"),
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
    ("LTIM",        "LTIM.NS"),
]

ETFS: list[tuple[str, str]] = [
    # ── Broad market ──────────────────────────────────────────────────────────
    ("NIFTYBEES",   "NIFTYBEES.NS"),    # Nifty 50 (Nippon)
    ("JUNIORBEES",  "JUNIORBEES.NS"),   # Nifty Next 50
    ("BANKBEES",    "BANKBEES.NS"),     # Nifty Bank
    ("PSUBNKBEES",  "PSUBNKBEES.NS"),  # Nifty PSU Bank
    ("HDFCNIFTY",   "HDFCNIFTY.NS"),   # Nifty 50 (HDFC)
    ("SETFNIF50",   "SETFNIF50.NS"),   # Nifty 50 (SBI)
    ("MID150BEES",  "MID150BEES.NS"),  # Nifty MidSmallcap 150
    ("SMALL250",    "SMALL250.NS"),    # Nifty Smallcap 250
    ("MONIFTY500",  "MONIFTY500.NS"),  # Nifty 500 (Motilal)
    # ── Sectoral ──────────────────────────────────────────────────────────────
    ("CPSEETF",     "CPSEETF.NS"),     # Nifty CPSE (PSU / Govt)
    ("ITBEES",      "ITBEES.NS"),      # Nifty IT
    ("PHARMABEES",  "PHARMABEES.NS"),  # Nifty Pharma
    ("AUTOBEES",    "AUTOBEES.NS"),    # Nifty Auto
    ("FMCGIETF",    "FMCGIETF.NS"),   # Nifty FMCG
    ("ICICIB22",    "ICICIB22.NS"),    # S&P BSE Bharat 22
    # ── Commodities ───────────────────────────────────────────────────────────
    ("GOLDBEES",    "GOLDBEES.NS"),    # Physical Gold
    ("SILVERBEES",  "SILVERBEES.NS"),  # Physical Silver
    # ── Debt ──────────────────────────────────────────────────────────────────
    ("LIQUIDBEES",  "LIQUIDBEES.NS"),  # Liquid / Cash
    ("LIQUIDCASE",  "LIQUIDCASE.NS"),  # 1D Rate Liquid
    ("GILT5YBEES",  "GILT5YBEES.NS"),  # Govt Securities 5Y
    # ── International ─────────────────────────────────────────────────────────
    ("HNGSNGBEES",  "HNGSNGBEES.NS"),  # Hang Seng
    ("MAFANG",      "MAFANG.NS"),      # NYSE FANG+
    ("MAHKTECH",    "MAHKTECH.NS"),    # Hang Seng TECH
    ("MON100",      "MON100.NS"),      # NASDAQ 100
    ("MASPTOP50",   "MASPTOP50.NS"),   # S&P 500 Top 50
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

# Broad indices
INDICES: list[tuple[str, str]] = [
    ("NIFTY50",    "^NSEI"),
    ("SENSEX",     "^BSESN"),
    ("BANKNIFTY",  "^NSEBANK"),
    ("NIFTYMID",   "^NSEMDCP50"),
    ("SP500",      "^GSPC"),
    ("NASDAQ",     "^IXIC"),
    ("DOWJONES",   "^DJI"),
    ("US10Y",      "^TNX"),      # US 10-year Treasury yield — real rate / gold headwind signal
    ("US13W",      "^IRX"),      # US 13-week T-bill yield  — yield-curve spread (10Y − 13W)
    ("DXY",        "DX-Y.NYB"),  # US Dollar Index
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
    # New domestic ETFs
    "LIQUIDCASE":  "153725",   # Nippon India Nifty 1D Rate Liquid ETF
    "CPSEETF":     "140107",   # CPSE ETF (Govt)
    "ITBEES":      "148408",   # Nippon India ETF Nifty IT
    "MID150BEES":  "146271",   # Nippon India ETF Nifty Midcap 150
    "MONIFTY500":  "152106",   # Motilal Oswal Nifty 500 ETF
    "GILT5YBEES":  "148800",   # Nippon India ETF Nifty 5yr Benchmark G-Sec
    "PHARMABEES":  "149008",   # Nippon India Nifty Pharma ETF
    "AUTOBEES":    "149465",   # Nippon India Nifty Auto ETF
    "FMCGIETF":    "149072",   # ICICI Prudential Nifty FMCG ETF
    "SMALL250":    "152455",   # Mirae Asset Nifty Smallcap 250 ETF
}

# Watchlist for MF portfolio holdings tracker
# Each entry: (amfi_scheme_code, short_name, isin_growth)
MF_HOLDINGS_WATCHLIST: list[tuple[str, str, str]] = [
    ("152056", "DSP_MULTI_ASSET",   "INF740KA1TE9"),
    ("120821", "QUANT_MULTI_ASSET", "INF966L01580"),
    ("120334", "ICICI_MULTI_ASSET", "INF109K015K4"),
]

# ── Registry lookup ────────────────────────────────────────────────────────────

CATEGORY_MAP: dict[str, list[tuple[str, str]]] = {
    "stocks":      STOCKS,
    "etfs":        ETFS,
    "commodities": COMMODITIES,
    "indices":     INDICES,
    "fx_rates":    FX_PAIRS,
}

# Symbols for which NSE live iNAV snapshots are captured
INAV_SYMBOLS: list[str] = [
    # Broad market
    "NIFTYBEES", "JUNIORBEES", "BANKBEES", "PSUBNKBEES",
    "HDFCNIFTY", "SETFNIF50", "MID150BEES", "SMALL250", "MONIFTY500",
    # Sectoral
    "CPSEETF", "ITBEES", "PHARMABEES", "AUTOBEES", "FMCGIETF", "ICICIB22",
    # Commodities
    "GOLDBEES", "SILVERBEES",
    # Debt
    "LIQUIDBEES", "LIQUIDCASE", "GILT5YBEES",
    # International
    "HNGSNGBEES", "MAFANG", "MAHKTECH", "MON100", "MASPTOP50",
]

ALL_CATEGORIES = list(CATEGORY_MAP.keys()) + ["mf", "inav", "nse_eod", "cot", "cb_reserves", "etf_aum", "mf_holdings", "fii_dii"]


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
