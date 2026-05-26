"""
src/tools/company_resolver.py
──────────────────────────────
Resolves a company name, partial name, or ticker symbol to its canonical
trading symbol and determines whether it's listed on Indian (NSE/BSE) or
US (NYSE/NASDAQ) exchanges.

Strategy
--------
1. Fast local lookup — checks the known Indian symbol alias map (no network).
2. Yahoo Finance search  — ``https://query1.finance.yahoo.com/v1/finance/search``
   (free, no API key).
3. Fallback — treats the input as an NSE symbol.

Returns a dict:
  symbol         — canonical clean symbol (e.g. "ADANIENT", "AAPL")
  nse_symbol     — NSE symbol if Indian, else None
  yf_symbol      — Yahoo Finance ticker (e.g. "ADANIENT.NS", "AAPL")
  exchange       — "NSE" | "BSE" | exchange code for US
  market         — "India" | "US"
  company_name   — human-readable name
  currency       — "INR" | "USD"
  source         — "local_map" | "yahoo_search" | "fallback"
"""
from __future__ import annotations

import logging
import re
from typing import Optional

import requests
from langchain_core.tools import tool

from src.utils.symbol_mapper import SYMBOL_TO_COMPANY

log = logging.getLogger(__name__)

_YAHOO_SEARCH_URL = "https://query1.finance.yahoo.com/v1/finance/search"

# ── LLM-powered symbol resolver ───────────────────────────────────────────────

_resolver_llm: "Any" = None          # lazy singleton — built on first use
_llm_symbol_cache: dict[str, str | None] = {}   # session-level cache


def _get_resolver_llm() -> "Any":
    """Return (building once) a minimal LLM for single-token symbol lookups."""
    global _resolver_llm
    if _resolver_llm is not None:
        return _resolver_llm
    try:
        from config.settings import settings
        kwargs = dict(temperature=0, max_tokens=20)
        if settings.llm_base_url:
            from langchain_openai import ChatOpenAI
            _resolver_llm = ChatOpenAI(
                model=settings.llm_model,
                base_url=settings.llm_base_url,
                api_key=settings.openai_api_key or "local",
                **kwargs,
            )
        elif settings.llm_provider == "anthropic":
            from langchain_anthropic import ChatAnthropic
            _resolver_llm = ChatAnthropic(
                model=settings.llm_model,
                api_key=settings.anthropic_api_key,
                **kwargs,
            )
        elif settings.llm_provider == "google":
            from langchain_google_genai import ChatGoogleGenerativeAI
            _resolver_llm = ChatGoogleGenerativeAI(
                model=settings.llm_model,
                google_api_key=settings.google_api_key,
                temperature=0,
                max_output_tokens=20,
            )
        else:
            from langchain_openai import ChatOpenAI
            _resolver_llm = ChatOpenAI(
                model=settings.llm_model,
                api_key=settings.openai_api_key,
                **kwargs,
            )
    except Exception as exc:
        log.warning("_get_resolver_llm: could not build LLM: %s", exc)
    return _resolver_llm


def _llm_resolve(query: str) -> str | None:
    """
    Ask the LLM for the canonical NSE/NYSE ticker of *query*.

    Returns the raw symbol string (e.g. "ASIANPAINT", "AAPL") or None.
    Results are cached in ``_llm_symbol_cache`` for the session lifetime so
    repeated lookups for the same query never hit the LLM twice.
    """
    key = query.strip().lower()
    if key in _llm_symbol_cache:
        return _llm_symbol_cache[key]

    llm = _get_resolver_llm()
    if llm is None:
        _llm_symbol_cache[key] = None
        return None

    try:
        from langchain_core.messages import HumanMessage
        prompt = (
            "You are a stock market expert covering Indian (NSE/BSE) and US markets.\n"
            f"What is the exact exchange ticker symbol for: \"{query}\"?\n"
            "Rules:\n"
            "- For Indian stocks reply with the NSE symbol in UPPERCASE (e.g. RELIANCE, INFY, ASIANPAINT).\n"
            "- For US stocks reply with the NYSE/NASDAQ symbol (e.g. AAPL, MSFT).\n"
            "- Reply with ONLY the ticker symbol — no explanation, no punctuation.\n"
            "- If you are not sure, reply UNKNOWN."
        )
        raw = str(llm.invoke([HumanMessage(content=prompt)]).content).strip()
        # Take the first token, strip non-alphanumeric (except & for some Indian symbols)
        first = raw.split()[0] if raw.split() else ""
        sym = re.sub(r"[^A-Z0-9&]", "", first.upper())
        result = sym if sym and sym != "UNKNOWN" and 1 < len(sym) <= 20 else None
        _llm_symbol_cache[key] = result
        if result:
            log.info("_llm_resolve: %r → %s (LLM memory)", query, result)
        else:
            log.info("_llm_resolve: %r → unresolved (LLM returned %r)", query, raw[:40])
        return result
    except Exception as exc:
        log.warning("_llm_resolve: LLM call failed for %r: %s", query, exc)
        _llm_symbol_cache[key] = None
        return None
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Accept": "application/json",
}
_TIMEOUT = 10

# Yahoo Finance exchange codes for Indian exchanges
_INDIAN_CODES = {"NSI", "BSI", "BOM", "CSE"}
# Yahoo Finance exchange codes for US exchanges
_US_CODES = {"NMS", "NYQ", "NGM", "NCM", "ASE", "PCX", "PNK", "OBB", "NAS"}

# Reverse index: lowercase company name → NSE symbol
_NAME_TO_NSE: dict[str, str] = {v.lower(): k for k, v in SYMBOL_TO_COMPANY.items()}

# Alias map: common informal names / abbreviations → NSE symbol
_ALIAS: dict[str, str] = {
    # Adani group
    "adani enterprises": "ADANIENT",
    "adani enterprise": "ADANIENT",
    "adani ports": "ADANIPORTS",
    "adani green": "ADANIGREEN",
    "adani power": "ADANIPOWER",
    "adani total gas": "ADANITOTGAS",
    "adani transmission": "ADANITRANS",
    "adani wilmar": "AWL",
    # HDFC / ICICI
    "hdfc bank": "HDFCBANK",
    "hdfc life": "HDFCLIFE",
    "icici bank": "ICICIBANK",
    "icici prudential": "ICICIPRULI",
    # SBI family
    "state bank": "SBIN",
    "sbi": "SBIN",
    "sbi life": "SBILIFE",
    # IT giants
    "tcs": "TCS",
    "tata consultancy services": "TCS",
    "tata consultancy": "TCS",
    "infosys": "INFY",
    "info edge": "NAUKRI",
    "hcl technologies": "HCLTECH",
    "hcl tech": "HCLTECH",
    "tech mahindra": "TECHM",
    "wipro": "WIPRO",
    "ltimindtree": "LTIM",
    "mphasis": "MPHASIS",
    "persistent systems": "PERSISTENT",
    "persistent": "PERSISTENT",
    "coforge": "COFORGE",
    "kpit technologies": "KPITTECH",
    "tata elxsi": "TATAELXSI",
    # Reliance
    "reliance industries": "RELIANCE",
    "reliance": "RELIANCE",
    "ril": "RELIANCE",
    # Tata group
    "tata motors": "TATAMOTORS",
    "tata steel": "TATASTEEL",
    "tata consumer": "TATACONSUM",
    "tata chemicals": "TATACHEM",
    "tata power": "TATAPOWER",
    "titan": "TITAN",
    "titan company": "TITAN",
    # Kotak / Axis
    "kotak bank": "KOTAKBANK",
    "kotak mahindra": "KOTAKBANK",
    "kotak mahindra bank": "KOTAKBANK",
    "axis bank": "AXISBANK",
    # Bajaj group
    "bajaj finance": "BAJFINANCE",
    "bajaj finserv": "BAJAJFINSV",
    "bajaj auto": "BAJAJ-AUTO",
    # Pharma
    "sun pharma": "SUNPHARMA",
    "sun pharmaceutical": "SUNPHARMA",
    "dr reddy": "DRREDDY",
    "dr reddys": "DRREDDY",
    "dr. reddy's": "DRREDDY",
    "cipla": "CIPLA",
    "lupin": "LUPIN",
    "biocon": "BIOCON",
    "divi's laboratories": "DIVISLAB",
    "divis laboratories": "DIVISLAB",
    "divis lab": "DIVISLAB",
    "torrent pharma": "TORNTPHARM",
    "aurobindo": "AUROPHARMA",
    "alkem": "ALKEM",
    # L&T
    "larsen and toubro": "LT",
    "larsen & toubro": "LT",
    "l&t": "LT",
    "lt": "LT",
    # Infrastructure / energy
    "ntpc": "NTPC",
    "power grid": "POWERGRID",
    "ongc": "ONGC",
    "coal india": "COALINDIA",
    "jsw steel": "JSWSTEEL",
    "tata steel": "TATASTEEL",
    "hindalco": "HINDALCO",
    "grasim": "GRASIM",
    "ultratech cement": "ULTRACEMCO",
    "ultra tech": "ULTRACEMCO",
    "gail india": "GAIL",
    "gail": "GAIL",
    "bpcl": "BPCL",
    "bharat petroleum": "BPCL",
    "ioc": "IOC",
    "indian oil": "IOC",
    "hindustan petroleum": "HINDPETRO",
    "hpcl": "HINDPETRO",
    "siemens india": "SIEMENS",
    # Consumer
    "hindustan unilever": "HINDUNILVR",
    "hul": "HINDUNILVR",
    "itc": "ITC",
    "nestle india": "NESTLEIND",
    "nestle": "NESTLEIND",
    "asian paints": "ASIANPAINT",
    "pidilite": "PIDILITIND",
    "berger paints": "BERGEPAINT",
    "dabur": "DABUR",
    "marico": "MARICO",
    "godrej consumer": "GODREJCP",
    "colgate": "COLPAL",
    "colgate palmolive": "COLPAL",
    "britannia": "BRITANNIA",
    "varun beverages": "VBL",
    # Auto
    "maruti suzuki": "MARUTI",
    "maruti": "MARUTI",
    "tata motors": "TATAMOTORS",
    "mahindra and mahindra": "M&M",
    "mahindra": "M&M",
    "m&m": "M&M",
    "hero motocorp": "HEROMOTOCO",
    "hero moto": "HEROMOTOCO",
    "tvs motor": "TVSMOTOR",
    "bajaj auto": "BAJAJ-AUTO",
    "eicher motors": "EICHERMOT",
    "ashok leyland": "ASHOKLEY",
    "mrf": "MRF",
    "apollo tyres": "APOLLOTYRE",
    # Hospitals / Insurance
    "apollo hospitals": "APOLLOHOSP",
    "max healthcare": "MAXHEALTH",
    "fortis": "FORTIS",
    # Banks (private small)
    "indusind bank": "INDUSINDBK",
    "federal bank": "FEDERALBNK",
    "bandhan bank": "BANDHANBNK",
    "yes bank": "YESBANK",
    "rbl bank": "RBLBANK",
    "idfc first": "IDFCFIRSTB",
    "idfc first bank": "IDFCFIRSTB",
    # PSU banks
    "pnb": "PNB",
    "punjab national bank": "PNB",
    "bank of baroda": "BANKBARODA",
    "canara bank": "CANBK",
    "union bank": "UNIONBANK",
    # Finance
    "muthoot finance": "MUTHOOTFIN",
    "cholamandalam": "CHOLAFIN",
    # Telecom
    "bharti airtel": "BHARTIARTL",
    "airtel": "BHARTIARTL",
    # Nippon India ETF "BeES" family — both concatenated and spaced forms
    "goldbees": "GOLDBEES",
    "gold bees": "GOLDBEES",
    "gold bee": "GOLDBEES",
    "nippon gold": "GOLDBEES",
    "nippon india gold": "GOLDBEES",
    "niftybees": "NIFTYBEES",
    "nifty bees": "NIFTYBEES",
    "nifty50bees": "NIFTYBEES",
    "nifty 50 bees": "NIFTYBEES",
    "bankbees": "BANKBEES",
    "bank bees": "BANKBEES",
    "silverbees": "SILVERBEES",
    "silver bees": "SILVERBEES",
    "silver bee": "SILVERBEES",
    "nippon silver": "SILVERBEES",
    "juniorbees": "JUNIORBEES",
    "junior bees": "JUNIORBEES",
    "liquidbees": "LIQUIDBEES",
    "liquid bees": "LIQUIDBEES",
    # Garware group — two distinct listed companies
    "garware hi-tech films": "GRWRHITECH",
    "garware hi tech films": "GRWRHITECH",
    "garware hitech films": "GRWRHITECH",
    "garware hitech": "GRWRHITECH",
    "garware hi tech": "GRWRHITECH",
    "grwrhitech": "GRWRHITECH",
    "garware technical fibres": "GARFIBRES",
    "garware technical fibre": "GARFIBRES",
    "garware fibres": "GARFIBRES",
    "garware fiber": "GARFIBRES",
    "garfibres": "GARFIBRES",
}


def _local_indian_lookup(query: str) -> Optional[str]:
    """
    Fast local lookup: returns NSE symbol if the query matches a known
    Indian company without hitting the network.  Returns None if not found.
    """
    q = query.strip().lower()

    # 1. Direct alias match
    if q in _ALIAS:
        return _ALIAS[q]

    # 2. Exact company-name match
    if q in _NAME_TO_NSE:
        return _NAME_TO_NSE[q]

    # 3. Looks like a known NSE symbol (passed directly by caller)
    upper = query.strip().upper().replace(" ", "")
    if upper in SYMBOL_TO_COMPANY:
        return upper

    # 4. Partial alias match — longest suffix match wins
    best: tuple[int, str] = (0, "")
    for alias, sym in _ALIAS.items():
        if alias in q or q in alias:
            if len(alias) > best[0]:
                best = (len(alias), sym)
    if best[1]:
        return best[1]

    # 5. Fuzzy match — catches typos like "adanai" → "adani"
    import difflib
    all_keys = list(_ALIAS.keys()) + list(_NAME_TO_NSE.keys())
    matches = difflib.get_close_matches(q, all_keys, n=1, cutoff=0.75)
    if matches:
        matched_key = matches[0]
        sym = _ALIAS.get(matched_key) or _NAME_TO_NSE.get(matched_key)
        if sym:
            log.info("_local_indian_lookup: fuzzy %r → %r → %s", q, matched_key, sym)
            return sym

    return None


def resolve_company_info(query: str) -> dict:
    """
    Core resolver — not a LangChain tool, call from Python directly.

    Resolves ``query`` (company name, partial name, or ticker) to a
    canonical trading symbol with market classification.
    """
    # ── 1. Fast local lookup (no network) ─────────────────────────────────
    local_sym = _local_indian_lookup(query)
    if local_sym:
        name = SYMBOL_TO_COMPANY.get(local_sym, local_sym)
        return {
            "symbol":       local_sym,
            "nse_symbol":   local_sym,
            "yf_symbol":    f"{local_sym}.NS",
            "exchange":     "NSE",
            "market":       "India",
            "company_name": name,
            "currency":     "INR",
            "source":       "local_map",
        }

    # ── 1b. LLM memory lookup ─────────────────────────────────────────────
    # Ask the LLM for the ticker from its training knowledge, then:
    #   a) if the symbol is already in the local map → return immediately
    #   b) otherwise use it as the Yahoo Finance search query (more precise than raw input)
    llm_sym = _llm_resolve(query)
    if llm_sym:
        local_from_llm = _local_indian_lookup(llm_sym)
        if local_from_llm:
            name = SYMBOL_TO_COMPANY.get(local_from_llm, local_from_llm)
            return {
                "symbol":       local_from_llm,
                "nse_symbol":   local_from_llm,
                "yf_symbol":    f"{local_from_llm}.NS",
                "exchange":     "NSE",
                "market":       "India",
                "company_name": name,
                "currency":     "INR",
                "source":       "llm_memory",
            }
        # LLM gave a symbol not in local map — use it for a tighter Yahoo search
        log.info("resolve_company: LLM suggested %r for %r — querying Yahoo", llm_sym, query)

    # ── 2. Yahoo Finance search ────────────────────────────────────────────
    # Use LLM-suggested symbol when available (more precise than free text)
    _yahoo_query = llm_sym or query
    try:
        resp = requests.get(
            _YAHOO_SEARCH_URL,
            params={"q": _yahoo_query, "lang": "en-US", "region": "US", "quotesCount": 6},
            headers=_HEADERS,
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        quotes = [
            q for q in resp.json().get("quotes", [])
            if q.get("quoteType") in ("EQUITY", "STOCK")
            and not q.get("symbol", "").startswith("^")
        ]

        for quote in quotes:
            sym          = quote.get("symbol", "")
            exch_code    = quote.get("exchange", "")
            display_name = quote.get("shortname") or quote.get("longname") or sym

            is_indian = (
                exch_code in _INDIAN_CODES
                or sym.endswith(".NS")
                or sym.endswith(".BO")
            )
            is_us = exch_code in _US_CODES and not is_indian

            clean = re.sub(r"\.(NS|BO|BSE)$", "", sym, flags=re.I)

            if is_indian:
                exch    = "BSE" if sym.endswith(".BO") else "NSE"
                yf_sym  = sym if sym.endswith((".NS", ".BO")) else f"{clean}.NS"
                return {
                    "symbol":       clean,
                    "nse_symbol":   clean,
                    "yf_symbol":    yf_sym,
                    "exchange":     exch,
                    "market":       "India",
                    "company_name": display_name,
                    "currency":     "INR",
                    "source":       "yahoo_search",
                }
            elif is_us:
                return {
                    "symbol":       sym,
                    "nse_symbol":   None,
                    "yf_symbol":    sym,
                    "exchange":     exch_code,
                    "market":       "US",
                    "company_name": display_name,
                    "currency":     "USD",
                    "source":       "yahoo_search",
                }
    except Exception as exc:
        log.warning("Yahoo Finance search failed for %r: %s", query, exc)

    # ── 3. Fallback — treat as NSE symbol ──────────────────────────────────
    upper = query.strip().upper().split()[0]    # first word as ticker
    log.info("resolve_company: falling back to NSE for %r → %s", query, upper)
    return {
        "symbol":       upper,
        "nse_symbol":   upper,
        "yf_symbol":    f"{upper}.NS",
        "exchange":     "NSE",
        "market":       "India",
        "company_name": upper,
        "currency":     "INR",
        "source":       "fallback",
    }


@tool
def resolve_company(query: str) -> dict:
    """
    Resolve a company name, partial name, or ticker to its canonical trading
    symbol and determine whether it is listed in India (NSE/BSE) or the US
    (NYSE/NASDAQ).

    ALWAYS call this tool first before any stock research to get the correct
    symbol, exchange, and market context.

    Examples
    --------
      "adani enterprise"  → {symbol: "ADANIENT", exchange: "NSE",  market: "India"}
      "AAPL"              → {symbol: "AAPL",      exchange: "NMS",  market: "US"}
      "reliance"          → {symbol: "RELIANCE",  exchange: "NSE",  market: "India"}
      "autodesk"          → {symbol: "ADSK",      exchange: "NMS",  market: "US"}
      "HDFC Bank"         → {symbol: "HDFCBANK",  exchange: "NSE",  market: "India"}

    Returns a dict with:
      symbol, nse_symbol, yf_symbol, exchange, market, company_name, currency, source
    """
    return resolve_company_info(query)
