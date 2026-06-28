"""
src/tools/etf_setup_scanner.py
───────────────────────────────
Scans all 18 tracked ETFs for volume-volatility opportunities (breakouts, exhaustion, squeezes).
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from langchain_core.tools import tool

log = logging.getLogger(__name__)

# List of tracked ETFs in the Mosaic platform
TRACKED_ETFS = [
    "NIFTYBEES", "BANKBEES", "JUNIORBEES", "PSUBNKBEES", "CPSEETF",
    "MID150BEES", "PHARMABEES", "AUTOBEES", "ITBEES", "SMALL250",
    "LIQUIDBEES", "LIQUIDCASE", "GILT5YBEES", "GOLDBEES", "SILVERBEES",
    "MON100", "MAFANG", "HNGSNGBEES", "HDFCNIFTY", "SETFNIF50",
    "GOLDCASE", "SILVERCASE", "MAHKTECH", "MONQ50", "MASPTOP50",
    "TOP100CASE", "MID150CASE", "LTGILTCASE",
]

def run_etf_setup_scan(lookback_days: int = 90) -> List[Dict[str, Any]]:
    """
    Scans ClickHouse ETF data and calculates volume-volatility setups.
    """
    from src.db.pool import query_df
    
    symbols_str = ", ".join(f"'{s}'" for s in TRACKED_ETFS)
    try:
        df = query_df(
            f"""
            SELECT symbol, trade_date, close, volume 
            FROM market_data.daily_prices FINAL 
            WHERE category = 'etfs' AND symbol IN ({symbols_str}) 
              AND trade_date >= today() - {lookback_days}
            ORDER BY symbol, trade_date ASC
            """
        )
    except Exception as e:
        log.error("Failed to query ClickHouse daily prices for setup scan: %s", e)
        return []

    if df.empty:
        log.warning("No daily prices data retrieved from ClickHouse.")
        return []

    # Parse and compute
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df_sorted = df.sort_values(["symbol", "trade_date"]).copy()
    
    results = []
    
    for sym, sub in df_sorted.groupby("symbol"):
        sub = sub.reset_index(drop=True)
        if len(sub) < 20:
            continue
            
        sub["daily_return"] = sub["close"].pct_change() * 100
        sub["vol_20d"] = sub["daily_return"].rolling(20, min_periods=10).std()
        sub["vol_ma20"] = sub["volume"].rolling(20, min_periods=10).mean()
        
        latest = sub.iloc[-1]
        
        # Calculate ratios
        daily_ret = float(latest["daily_return"])
        vol_20d = float(latest["vol_20d"]) if not pd.isna(latest["vol_20d"]) else 1.0
        if vol_20d == 0.0:
            vol_20d = 1.0
            
        vol_ratio = abs(daily_ret) / vol_20d
        vol_ma = float(latest["vol_ma20"]) if not pd.isna(latest["vol_ma20"]) else 1.0
        if vol_ma == 0.0:
            vol_ma = 1.0
            
        vol_ratio_val = float(latest["volume"]) / vol_ma
        
        # Get historical volatilities for squeeze check
        recent_vols = sub["vol_20d"].tail(60).dropna()
        is_squeeze = False
        if not recent_vols.empty and len(recent_vols) >= 15:
            is_squeeze = vol_20d < recent_vols.quantile(0.25)
            
        # Classify setups
        if vol_ratio_val > 1.5 and vol_ratio > 1.5:
            pattern = "🚀 Volatile Breakout"
            details = f"Volume: {vol_ratio_val:.2f}x MA, Return: {daily_ret:+.2f}% (Vol: {vol_20d:.2f}%)"
        elif vol_ratio_val < 0.7 and vol_ratio > 1.8:
            pattern = "⚠️ Volume Exhaustion"
            details = f"Volume: {vol_ratio_val:.2f}x MA, Return: {daily_ret:+.2f}% (Vol: {vol_20d:.2f}%)"
        elif vol_ratio_val < 0.8 and is_squeeze:
            pattern = "📦 Volatility Squeeze"
            details = f"Low Vol: {vol_20d:.2f}%, Volume: {vol_ratio_val:.2f}x MA"
        else:
            pattern = "Normal"
            details = "-"
            
        results.append({
            "symbol": sym,
            "close": float(latest["close"]),
            "daily_return": round(daily_ret, 4),
            "volatility_20d": round(vol_20d, 4),
            "volume_vs_ma": round(vol_ratio_val, 4),
            "pattern": pattern,
            "details": details
        })
        
    results.sort(key=lambda x: x["volume_vs_ma"], reverse=True)
    return results

@tool
def scan_etf_setups() -> str:
    """
    Scans all tracked ETFs for volume-volatility setups (breakouts, squeezes, exhaustion)
    using the last 90 business days of ClickHouse daily prices data.
    
    Returns:
        A Markdown-formatted table summarizing the volume-volatility setups.
    """
    results = run_etf_setup_scan()
    if not results:
        return "No ETF setup scan results available. Ensure ClickHouse has ETF EOD data."
        
    md = []
    md.append("### 🔬 ETF Volume-Volatility Setup Scan\n")
    md.append("| ETF | Close | Daily Return | 20d Volatility | Volume vs 20d MA | Pattern Setup | Details |")
    md.append("|---|---|---|---|---|---|---|")
    
    for r in results:
        md.append(
            f"| {r['symbol']} | {r['close']:.2f} | {r['daily_return']:+.2f}% | "
            f"{r['volatility_20d']:.2f}% | {r['volume_vs_ma']:.2f}x | "
            f"{r['pattern']} | {r['details']} |"
        )
        
    return "\n".join(md)
