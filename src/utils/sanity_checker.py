"""
src/utils/sanity_checker.py
─────────────────────────
Rules engine for market data anomaly detection.
Identifies economically unrealistic returns or outliers.
"""

from typing import List, Dict, Any
import clickhouse_connect
from pathlib import Path
import sys

# Constants for anomaly detection
MAX_YOY_RETURN_SAFE_ASSET = 0.40  # 40% (e.g., Gold, Nifty 50)
MAX_DAILY_RETURN = 0.07          # 7% (NSE circuit limit)

# Assets considered "safe/stable" for lower thresholds
SAFE_ASSETS = ["GOLDBEES", "NIFTYBEES", "BANKBEES", "LIQUIDBEES", "MON100"]

def detect_yoy_anomalies(client: Any, symbols: List[str] = None) -> List[Dict[str, Any]]:
    """
    Scans ClickHouse for symbols where YoY returns exceed economic reality.
    """
    symbol_filter = ""
    if symbols:
        symbols_tuple = str(tuple(symbols)) if len(symbols) > 1 else f"('{symbols[0]}')"
        symbol_filter = f"WHERE symbol IN {symbols_tuple}"

    query = f"""
    WITH daily_data AS (
        SELECT 
            symbol, 
            toYear(trade_date) as year,
            trade_date,
            argMax(close, imported_at) as close_price
        FROM market_data.daily_prices
        {symbol_filter}
        GROUP BY symbol, trade_date
    ),
    year_end_prices AS (
        SELECT 
            symbol,
            year,
            argMax(close_price, trade_date) as end_price
        FROM daily_data
        GROUP BY symbol, year
    ),
    yoy_calc AS (
        SELECT 
            year,
            symbol,
            end_price,
            any(end_price) OVER (PARTITION BY symbol ORDER BY year ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as prev_year_price
        FROM year_end_prices
    )
    SELECT 
        year, 
        symbol, 
        end_price, 
        prev_year_price,
        (end_price / prev_year_price) - 1 as return_yoy
    FROM yoy_calc
    WHERE prev_year_price > 0 
      AND (
          (symbol IN {tuple(SAFE_ASSETS)} AND abs(return_yoy) > {MAX_YOY_RETURN_SAFE_ASSET})
          OR abs(return_yoy) > 1.0  # 100% for anything else
      )
    ORDER BY year DESC, symbol ASC
    """
    
    result = client.query(query)
    anomalies = []
    for row in result.result_rows:
        anomalies.append({
            "year": row[0],
            "symbol": row[1],
            "end_price": row[2],
            "prev_price": row[3],
            "return_pct": row[4] * 100
        })
    return anomalies

def detect_daily_anomalies(client: Any, symbols: List[str] = None) -> List[Dict[str, Any]]:
    """
    Scans ClickHouse for sudden daily price spikes exceeding standard circuit limits.
    """
    symbol_filter = ""
    if symbols:
        symbols_tuple = str(tuple(symbols)) if len(symbols) > 1 else f"('{symbols[0]}')"
        symbol_filter = f"WHERE symbol IN {symbols_tuple}"

    query = f"""
    WITH daily_data AS (
        SELECT 
            symbol, 
            trade_date, 
            argMax(close, imported_at) as close_price,
            any(close_price) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as prev_close
        FROM market_data.daily_prices
        {symbol_filter}
        GROUP BY symbol, trade_date
    )
    SELECT 
        trade_date,
        symbol,
        close_price,
        prev_close,
        abs((close_price / prev_close) - 1) as day_move
    FROM daily_data
    WHERE prev_close > 0 AND day_move > {MAX_DAILY_RETURN}
    ORDER BY trade_date DESC
    LIMIT 100
    """
    
    result = client.query(query)
    anomalies = []
    for row in result.result_rows:
        anomalies.append({
            "date": row[0],
            "symbol": row[1],
            "price": row[2],
            "prev_price": row[3],
            "move_pct": row[4] * 100
        })
    return anomalies
