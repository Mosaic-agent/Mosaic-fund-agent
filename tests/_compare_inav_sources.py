"""
tests/_compare_inav_sources.py
──────────────────────────────
Compares NSE API iNAV vs Yahoo Finance navPrice vs market price
for all ETFs to find which source gives the true iNAV.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import httpx
import time
import yfinance as yf

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

etfs = [
    "NIFTYBEES", "GOLDBEES", "BANKBEES", "JUNIORBEES",
    "SILVERBEES", "HNGSNGBEES", "MAFANG", "MAHKTECH",
]


def fetch_nse_full(client, symbol):
    """Return the full NSE API JSON response for an ETF."""
    try:
        r = client.get(
            "https://www.nseindia.com/api/etf",
            params={"symbol": symbol},
            timeout=10,
        )
        return r.json()
    except Exception as e:
        return {"error": str(e)}


def main():
    print(f"\n  {'Symbol':<14} {'NSE_iNAV':>12} {'NSE_raw_fields':<40} {'YF_navPrice':>12} {'YF_mktPrice':>12}")
    print("  " + "-" * 92)

    with httpx.Client(headers=HEADERS, follow_redirects=True, timeout=15) as client:
        # Warm-up to obtain session cookies
        client.get("https://www.nseindia.com/market-data/exchange-traded-funds-etf", timeout=10)
        time.sleep(1.5)

        for i, sym in enumerate(etfs):
            # ── NSE API ──────────────────────────────────────────────────────
            data = fetch_nse_full(client, sym)
            if i == 0:
                print(f"\n  [DEBUG] Raw NSE response for {sym}:")
                import json as _json
                print("  " + _json.dumps(data, indent=2)[:600])
                print()
            # ── NSE API ──────────────────────────────────────────────────────
            data = fetch_nse_full(client, sym)
            nse_inav = "N/A"
            nse_fields = ""
            if isinstance(data, dict):
                # NSE sometimes nests under "data" key as a list
                inner = data.get("data")
                if isinstance(inner, list) and inner:
                    row = inner[0]
                elif isinstance(inner, dict):
                    row = inner
                else:
                    row = data
                nav_keys = {k: v for k, v in row.items() if any(x in k.lower() for x in ("nav", "price", "inav"))}
                nse_inav = row.get("iNavValue") or data.get("iNavValue") or "N/A"
                nse_fields = str(nav_keys)[:40]
            elif isinstance(data, list) and data:
                row = data[0]
                nav_keys = {k: v for k, v in row.items() if any(x in k.lower() for x in ("nav", "price", "inav"))}
                nse_inav = row.get("iNavValue", "N/A")
                nse_fields = str(nav_keys)[:40]

            # ── Yahoo Finance ────────────────────────────────────────────────
            info = yf.Ticker(f"{sym}.NS").info
            yf_nav = info.get("navPrice") or "N/A"
            yf_mkt = info.get("regularMarketPrice") or "N/A"

            print(f"  {sym:<14} {str(nse_inav):>12}  {nse_fields:<40} {str(yf_nav):>12} {str(yf_mkt):>12}")
            time.sleep(0.4)

    print()
    print("  Legend:")
    print("  NSE_iNAV   = data from NSE API iNavValue field (true live iNAV, every 15s)")
    print("  YF_navPrice = Yahoo Finance navPrice (may be end-of-day, often == mktPrice)")
    print("  YF_mktPrice = Yahoo Finance regularMarketPrice (last traded price)")


if __name__ == "__main__":
    main()
