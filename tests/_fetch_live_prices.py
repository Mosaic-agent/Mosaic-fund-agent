"""Fetch live iNAV / market prices for all test ETFs — run once to update mocks."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import yfinance as yf

etfs = [
    ("NIFTYBEES",  "NIFTYBEES.NS"),
    ("GOLDBEES",   "GOLDBEES.NS"),
    ("BANKBEES",   "BANKBEES.NS"),
    ("JUNIORBEES", "JUNIORBEES.NS"),
    ("SILVERBEES", "SILVERBEES.NS"),
    ("LIQUIDBEES", "LIQUIDBEES.NS"),
    ("HNGSNGBEES", "HNGSNGBEES.NS"),
    ("MAFANG",     "MAFANG.NS"),
    ("MIAEHTECH",  "MIAEHTECH.NS"),
]

print(f"\n  {'Symbol':<14} {'navPrice':>12} {'mktPrice':>12} {'prevClose':>12} {'52W_H':>10} {'52W_L':>10}")
print("  " + "-"*72)
for sym, ticker in etfs:
    info  = yf.Ticker(ticker).info
    nav   = info.get("navPrice") or info.get("regularMarketPrice") or "N/A"
    mkt   = info.get("regularMarketPrice") or "N/A"
    prev  = info.get("regularMarketPreviousClose") or "N/A"
    h52   = info.get("fiftyTwoWeekHigh") or "N/A"
    l52   = info.get("fiftyTwoWeekLow") or "N/A"
    print(f"  {sym:<14} {str(nav):>12} {str(mkt):>12} {str(prev):>12} {str(h52):>10} {str(l52):>10}")
