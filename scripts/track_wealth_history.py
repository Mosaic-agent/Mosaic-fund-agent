"""
scripts/track_wealth_history.py
───────────────────────────────
Calculates total portfolio value and P&L for the current day
and stores it in market_data.wealth_history for time-series analysis.
"""

import sys
import os
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.importer.clickhouse import ClickHouseImporter
from config.settings import settings

def ensure_wealth_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS market_data.wealth_history (
        record_date     Date,
        total_invested  Float64,
        total_value     Float64,
        total_pnl       Float64,
        pnl_pct         Float64,
        imported_at     DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(imported_at)
    ORDER BY (record_date)
    """
    with ClickHouseImporter(host=settings.clickhouse_host, port=settings.clickhouse_port) as ch:
        ch._client.command(ddl)

def record_daily_wealth():
    query = """
    SELECT 
        today() as record_date,
        SUM(quantity * average_price) as total_invested,
        SUM(quantity * last_price) as total_value,
        SUM(pnl) as total_pnl,
        (SUM(pnl) / SUM(quantity * average_price)) * 100 as pnl_pct
    FROM market_data.user_holdings FINAL
    """
    with ClickHouseImporter(host=settings.clickhouse_host, port=settings.clickhouse_port) as ch:
        res = ch._client.query_df(query)
        if res.empty or res.iloc[0]['total_invested'] is None:
            print("No holdings found to record.")
            return
        
        row = res.iloc[0].to_dict()
        ch._client.insert(
            "market_data.wealth_history",
            [[row['record_date'], row['total_invested'], row['total_value'], row['total_pnl'], row['pnl_pct']]],
            column_names=['record_date', 'total_invested', 'total_value', 'total_pnl', 'pnl_pct']
        )
        print(f"Recorded wealth snapshot for {row['record_date']}: ₹{row['total_value']:,.2f}")

if __name__ == "__main__":
    ensure_wealth_table()
    record_daily_wealth()
