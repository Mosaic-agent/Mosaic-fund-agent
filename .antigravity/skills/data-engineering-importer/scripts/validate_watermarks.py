#!/usr/bin/env python3
import sys
import os

# Add project root to sys.path
sys.path.append(os.getcwd())

from config.settings import settings
import clickhouse_connect

def main():
    try:
        client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
        )
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}")
        sys.exit(1)

    print("Checking daily_prices watermarks...")
    query = """
    SELECT symbol, max(trade_date) as actual_max 
    FROM market_data.daily_prices 
    GROUP BY symbol
    """
    actual_max_dates = {row[0]: row[1] for row in client.query(query).result_rows}

    query = "SELECT source, symbol, last_date FROM market_data.import_watermarks WHERE source = 'yfinance'"
    watermarks = {row[1]: row[2] for row in client.query(query).result_rows}

    mismatch = False
    for symbol, actual in actual_max_dates.items():
        wm = watermarks.get(symbol)
        if wm != actual:
            mismatch = True
            print(f"Mismatch for {symbol}: Watermark={wm}, Actual Max={actual}")

    if not mismatch:
        print("✅ All daily_prices watermarks match!")
    
    # Similarly for mf_nav if source=mfapi
    print("\nChecking mf_nav watermarks...")
    query = """
    SELECT symbol, max(nav_date) as actual_max 
    FROM market_data.mf_nav 
    GROUP BY symbol
    """
    actual_max_dates = {row[0]: row[1] for row in client.query(query).result_rows}

    query = "SELECT source, symbol, last_date FROM market_data.import_watermarks WHERE source = 'mfapi'"
    watermarks = {row[1]: row[2] for row in client.query(query).result_rows}

    mismatch = False
    for symbol, actual in actual_max_dates.items():
        wm = watermarks.get(symbol)
        if wm != actual:
            mismatch = True
            print(f"Mismatch for {symbol}: Watermark={wm}, Actual Max={actual}")

    if not mismatch:
        print("✅ All mf_nav watermarks match!")

    client.close()

if __name__ == "__main__":
    main()
