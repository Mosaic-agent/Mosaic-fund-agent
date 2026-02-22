"""
tests/_test_nse_parse.py
Test the fixed _fetch_inav_nse to verify it correctly filters by symbol
and returns the right nav + ltP pair.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import httpx, time

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/market-data/exchange-traded-funds-etf",
    "Connection": "keep-alive",
}

test_syms = ["NIFTYBEES", "GOLDBEES", "BANKBEES", "SILVERBEES",
             "JUNIORBEES", "HNGSNGBEES", "MAFANG", "MAHKTECH"]

print("\nFetching full NSE ETF list...")
with httpx.Client(headers=HEADERS, follow_redirects=True, timeout=15) as client:
    client.get("https://www.nseindia.com/market-data/exchange-traded-funds-etf", timeout=10)
    time.sleep(1.5)
    resp = client.get("https://www.nseindia.com/api/etf", timeout=10)
    data = resp.json()

rows = data.get("data", []) if isinstance(data, dict) else data
print(f"Total ETFs in NSE list: {len(rows)}")
print(f"First 5 symbols: {[r.get('symbol') for r in rows[:5]]}")
print()

# Show all symbols in list for reference
all_syms = {r.get("symbol", "").upper() for r in rows}
print(f"\n{'Symbol':<14} {'In NSE list':>12} {'nav':>10} {'ltP (mkt)':>12}")
print("-" * 52)
for sym in test_syms:
    matched = next((r for r in rows if r.get("symbol", "").upper() == sym), None)
    in_list = "YES" if matched else "NO"
    nav = matched.get("nav", "N/A") if matched else "N/A"
    ltp = matched.get("ltP", "N/A") if matched else "N/A"
    print(f"  {sym:<12} {in_list:>12} {str(nav):>10} {str(ltp):>12}")

print()
# Now test via the actual fixed function
print("Testing via _fetch_inav_nse():")
from src.tools.inav_fetcher import _fetch_inav_nse, get_etf_inav

for sym in ["NIFTYBEES", "GOLDBEES", "MAFANG"]:
    result = _fetch_inav_nse(sym)
    print(f"  {sym:<12}: {result}")
