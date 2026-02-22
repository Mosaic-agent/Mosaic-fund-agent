"""
src/utils/symbol_mapper.py
───────────────────────────
Utility for mapping between Zerodha trading symbols, Yahoo Finance tickers,
and common company names for Indian equity markets.

Why this is needed:
  • Zerodha returns symbols like: RELIANCE, TCS, HDFCBANK
  • Yahoo Finance needs:          RELIANCE.NS, TCS.NS, HDFCBANK.NS
  • NewsAPI searches work best with full company names

A curated mapping of the most common Indian large/mid-cap stocks is
included. Unknown symbols fall back to a generic NSE suffix mapping.
"""

from __future__ import annotations

# ── Symbol → Company Name Map ─────────────────────────────────────────────────
# Format: ZERODHA_SYMBOL → Full company name (used for better news search)

SYMBOL_TO_COMPANY: dict[str, str] = {
    # Nifty 50 & large caps
    "RELIANCE": "Reliance Industries",
    "TCS": "Tata Consultancy Services",
    "HDFCBANK": "HDFC Bank",
    "INFY": "Infosys",
    "ICICIBANK": "ICICI Bank",
    "HINDUNILVR": "Hindustan Unilever",
    "ITC": "ITC Limited",
    "SBIN": "State Bank of India",
    "BHARTIARTL": "Bharti Airtel",
    "KOTAKBANK": "Kotak Mahindra Bank",
    "LT": "Larsen and Toubro",
    "AXISBANK": "Axis Bank",
    "ASIANPAINT": "Asian Paints",
    "MARUTI": "Maruti Suzuki",
    "BAJFINANCE": "Bajaj Finance",
    "BAJAJFINSV": "Bajaj Finserv",
    "WIPRO": "Wipro",
    "HCLTECH": "HCL Technologies",
    "SUNPHARMA": "Sun Pharmaceutical",
    "TECHM": "Tech Mahindra",
    "TATAMOTORS": "Tata Motors",
    "TATASTEEL": "Tata Steel",
    "NESTLEIND": "Nestle India",
    "ULTRACEMCO": "UltraTech Cement",
    "TITAN": "Titan Company",
    "POWERGRID": "Power Grid Corporation",
    "NTPC": "NTPC Limited",
    "ONGC": "Oil and Natural Gas Corporation",
    "COALINDIA": "Coal India",
    "JSWSTEEL": "JSW Steel",
    "ADANIPORTS": "Adani Ports",
    "ADANIENT": "Adani Enterprises",
    "ADANIGREEN": "Adani Green Energy",
    "ADANIPOWER": "Adani Power",
    "GRASIM": "Grasim Industries",
    "EICHERMOT": "Eicher Motors",
    "HEROMOTOCO": "Hero MotoCorp",
    "APOLLOHOSP": "Apollo Hospitals",
    "CIPLA": "Cipla",
    "DRREDDY": "Dr Reddys Laboratories",
    "DIVISLAB": "Divi's Laboratories",
    "BRITANNIA": "Britannia Industries",
    "HINDALCO": "Hindalco Industries",
    "INDUSINDBK": "IndusInd Bank",
    "TATACONSUM": "Tata Consumer Products",
    "BAJAJ-AUTO": "Bajaj Auto",
    "BPCL": "Bharat Petroleum",
    "IOC": "Indian Oil Corporation",
    "M&M": "Mahindra and Mahindra",
    "SBILIFE": "SBI Life Insurance",
    "HDFCLIFE": "HDFC Life Insurance",
    "ICICIPRULI": "ICICI Prudential Life",
    "PIDILITIND": "Pidilite Industries",
    "BERGEPAINT": "Berger Paints",
    "HAVELLS": "Havells India",
    "MUTHOOTFIN": "Muthoot Finance",
    "CHOLAFIN": "Cholamandalam Finance",
    "BANDHANBNK": "Bandhan Bank",
    "FEDERALBNK": "Federal Bank",
    "PNB": "Punjab National Bank",
    "BANKBARODA": "Bank of Baroda",
    "CANBK": "Canara Bank",
    "UNIONBANK": "Union Bank of India",
    "IDFCFIRSTB": "IDFC First Bank",
    "YESBANK": "Yes Bank",
    "RBLBANK": "RBL Bank",
    # IT Sector
    "MPHASIS": "Mphasis",
    "PERSISTENT": "Persistent Systems",
    "LTIM": "LTIMindtree",
    "COFORGE": "Coforge",
    "KPITTECH": "KPIT Technologies",
    "TATAELXSI": "Tata Elxsi",
    # Pharma
    "BIOCON": "Biocon",
    "LUPIN": "Lupin",
    "TORNTPHARM": "Torrent Pharmaceuticals",
    "AUROPHARMA": "Aurobindo Pharma",
    "ALKEM": "Alkem Laboratories",
    # Consumer
    "MARICO": "Marico",
    "DABUR": "Dabur India",
    "GODREJCP": "Godrej Consumer Products",
    "EMAMILTD": "Emami",
    "COLPAL": "Colgate Palmolive India",
    "VBL": "Varun Beverages",
    # Auto
    "TVSMOTOR": "TVS Motor Company",
    "ASHOKLEY": "Ashok Leyland",
    "MRF": "MRF",
    "APOLLOTYRE": "Apollo Tyres",
    # Infrastructure / Energy
    "GAIL": "GAIL India",
    "PETRONET": "Petronet LNG",
    "SIEMENS": "Siemens India",
    "ABB": "ABB India",
    "CUMMINSIND": "Cummins India",
    # Common Indian ETFs
    "NIFTYBEES": "Nippon India Nifty 50 ETF",
    "JUNIORBEES": "Nippon India Junior BeES",
    "GOLDBEES": "Nippon India Gold ETF",
    "LIQUIDBEES": "Nippon India Liquid BeES",
    "ICICIB22": "ICICI Prudential Bharat 22 ETF",
    "MON100": "Mirae Asset NYSE FANG+ ETF",
    "NETFIT": "Nippon India ETF Nifty IT",
    "BANKBEES": "Nippon India Banking ETF",
    "PSUBNKBEES": "Nippon India ETF PSU Bank",
}

# ── Lookup helpers ─────────────────────────────────────────────────────────────

def get_company_name(symbol: str) -> str:
    """
    Return the full company name for a Zerodha symbol.
    Falls back to the symbol itself if not found in the map.
    """
    return SYMBOL_TO_COMPANY.get(symbol.upper(), symbol)


def to_nse_yahoo(symbol: str) -> str:
    """Convert a Zerodha symbol to NSE Yahoo Finance ticker e.g. RELIANCE → RELIANCE.NS"""
    return f"{symbol.upper()}.NS"


def to_bse_yahoo(symbol: str) -> str:
    """Convert a Zerodha symbol to BSE Yahoo Finance ticker e.g. RELIANCE → RELIANCE.BO"""
    return f"{symbol.upper()}.BO"


def from_yahoo(yf_symbol: str) -> str:
    """Strip .NS or .BO suffix to get plain Zerodha symbol."""
    return yf_symbol.upper().replace(".NS", "").replace(".BO", "")
