"""
tests/_backtest_etf_arbitrage.py
─────────────────────────────────
Backtest the ETF premium/discount arbitrage strategy against
historical iNAV and price data stored in ClickHouse.

Strategy:
  • Buy when Z-score of premium/discount is ≤ -1.5 (GOOD DISCOUNT)
  • Sell/Avoid when Z-score of premium/discount is ≥ +1.5 (HIGH PREMIUM)

Metrics:
  • Hit rate (positive forward return for Buy, negative for Sell)
  • Mean forward return at 1, 3, 5, and 10 trading days.

Usage:
  python tests/_backtest_etf_arbitrage.py --symbol GOLDBEES
  python tests/_backtest_etf_arbitrage.py --symbol NIFTYBEES --lookback 30
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta

import pandas as pd
import numpy as np

# Ensure project root is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.settings import settings

PASS  = "\033[92m✓\033[0m"
FAIL  = "\033[91m✗\033[0m"
WARN  = "\033[93m⚠\033[0m"
RESET = "\033[0m"

def run_backtest(
    symbol: str = "GOLDBEES",
    lookback: int = 30,
    z_threshold: float = 1.5,
    ch_host: str = "localhost",
    ch_port: int = 8123,
    ch_user: str = "default",
    ch_pass: str = "",
):
    try:
        import clickhouse_connect
    except ImportError:
        print("ERROR: Missing clickhouse-connect. Run: pip install clickhouse-connect")
        sys.exit(1)

    print("=" * 68)
    print(f"ETF Arbitrage Backtest — {symbol}")
    print(f"  lookback window : {lookback} days")
    print(f"  z_threshold     : {z_threshold}")
    print("=" * 68)

    client = clickhouse_connect.get_client(
        host=ch_host, port=ch_port,
        username=ch_user, password=ch_pass,
        connect_timeout=10,
    )

    # 1. Load Daily iNAV Snapshots (resampled to daily if needed)
    print(f"\nFetching {symbol} iNAV snapshots…")
    # We take the last snapshot per day to represent the EOD premium
    query_inav = f"""
        SELECT
            toDate(snapshot_at) AS trade_date,
            argMax(premium_discount_pct, snapshot_at) AS premium
        FROM market_data.inav_snapshots
        WHERE symbol = '{symbol}'
        GROUP BY trade_date
        ORDER BY trade_date ASC
    """
    inav_rows = client.query(query_inav).result_rows
    if not inav_rows:
        print(f"ERROR: No iNAV data for {symbol}")
        client.close()
        return

    df_inav = pd.DataFrame(inav_rows, columns=["trade_date", "premium"])
    df_inav["trade_date"] = pd.to_datetime(df_inav["trade_date"])
    
    # 2. Load Daily Prices for Forward Returns
    print(f"Fetching {symbol} daily prices…")
    query_prices = f"""
        SELECT
            trade_date,
            argMax(close, imported_at) AS close
        FROM market_data.daily_prices
        WHERE symbol = '{symbol}'
        GROUP BY trade_date
        ORDER BY trade_date ASC
    """
    price_rows = client.query(query_prices).result_rows
    client.close()

    if not price_rows:
        print(f"ERROR: No price data for {symbol}")
        return

    df_prices = pd.DataFrame(price_rows, columns=["trade_date", "close"])
    df_prices["trade_date"] = pd.to_datetime(df_prices["trade_date"])

    # Merge
    df = pd.merge(df_inav, df_prices, on="trade_date", how="inner")
    df = df.sort_values("trade_date").reset_index(drop=True)
    
    # ── Data Quality Filter ──────────────────────────────────────────────────
    # Domestic ETFs rarely trade at >10% premium/discount.
    # Values like -99.99% indicate missing NAV data.
    initial_len = len(df)
    df = df[(df["premium"] > -10.0) & (df["premium"] < 10.0)].copy()
    filtered_len = len(df)
    if initial_len != filtered_len:
        print(f"  [Filtered {initial_len - filtered_len} outlier rows (premium > ±10%)]")
    
    if len(df) < lookback + 10:
        print(f"ERROR: Insufficient merged data ({len(df)} rows)")
        return

    # 3. Compute Rolling Z-score
    print(f"Computing Z-scores (window={lookback})…")
    df["mean_p"] = df["premium"].rolling(window=lookback).mean()
    df["std_p"] = df["premium"].rolling(window=lookback).std()
    df["z_score"] = (df["premium"] - df["mean_p"]) / df["std_p"]

    # 4. Signal Generation
    df["signal"] = "Neutral"
    df.loc[df["z_score"] <= -z_threshold, "signal"] = "Buy (Discount)"
    df.loc[df["z_score"] >= z_threshold, "signal"] = "Sell (Premium)"

    # 5. Forward Returns
    for h in [1, 3, 5, 10]:
        df[f"ret_{h}d"] = (df["close"].shift(-h) / df["close"] - 1) * 100

    # 6. Analysis
    print("\n" + "─" * 68)
    print(f"{'Signal':<20} | {'Count':>6} | {'1d Ret':>8} | {'3d Ret':>8} | {'5d Ret':>8} | {'10d Ret':>8}")
    print("─" * 68)

    for sig in ["Buy (Discount)", "Sell (Premium)"]:
        subset = df[df["signal"] == sig].dropna(subset=["ret_10d"])
        count = len(subset)
        if count == 0:
            print(f"{sig:<20} | {0:>6} | {'N/A':>8} | {'N/A':>8} | {'N/A':>8} | {'N/A':>8}")
            continue

        rets = [subset[f"ret_{h}d"].mean() for h in [1, 3, 5, 10]]
        
        # Hit rate for Buy: ret > 0; for Sell: ret < 0
        if "Buy" in sig:
            hit_rate = (subset["ret_5d"] > 0).mean() * 100
        else:
            hit_rate = (subset["ret_5d"] < 0).mean() * 100

        print(f"{sig:<20} | {count:>6} | {rets[0]:>7.2f}% | {rets[1]:>7.2f}% | {rets[2]:>7.2f}% | {rets[3]:>7.2f}%")
        print(f"{'':<20} | {'HitRate':>6} | {'5d':>8} | {hit_rate:>7.1f}% | {'':>8} | {'':>8}")

    print("─" * 68)
    
    # Summary of best and worst signals
    flagged = df[df["signal"] != "Neutral"].dropna(subset=["ret_10d"])
    if not flagged.empty:
        best_buy = flagged[flagged["signal"] == "Buy (Discount)"].sort_values("ret_5d", ascending=False).head(3)
        if not best_buy.empty:
            print("\nTop 3 Buy Signals (by 5d return):")
            for _, r in best_buy.iterrows():
                print(f"  {r.trade_date.date()} : Prem {r.premium:>6.2f}% (Z={r.z_score:>5.2f}) → 5d return: {r.ret_5d:>6.2f}%")

    print("\nBacktest Complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="GOLDBEES")
    parser.add_argument("--lookback", type=int, default=30)
    parser.add_argument("--z-threshold", type=float, default=1.5)
    args = parser.parse_args()

    run_backtest(
        symbol=args.symbol,
        lookback=args.lookback,
        z_threshold=args.z_threshold,
        ch_host=settings.clickhouse_host,
        ch_port=settings.clickhouse_port,
        ch_user=settings.clickhouse_user,
        ch_pass=settings.clickhouse_password,
    )
