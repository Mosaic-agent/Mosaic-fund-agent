import os
import sys
import argparse
from pathlib import Path
import clickhouse_connect
from rich.console import Console
from rich.table import Table

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from config.settings import settings
except ImportError as e:
    print(f"Error importing project modules: {e}")
    sys.exit(1)

def verify_gemini_table():
    console = Console()
    
    try:
        client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database
        )
    except Exception as e:
        console.print(f"[red]Error connecting to ClickHouse: {e}[/red]")
        sys.exit(1)

    # Years to check
    years = range(2017, 2027)
    symbols = ['NIFTYBEES', 'GOLDBEES']
    
    # Structure to hold results: {year: {symbol: price}}
    actual_data = {year: {} for year in years}

    for year in years:
        # We look for the last trading day on or before March 31st of that year
        target_date = f"{year}-03-31"
        
        query = f"""
        WITH daily_data AS (
            SELECT symbol, trade_date, argMax(close, imported_at) as close_price
            FROM market_data.daily_prices
            WHERE symbol IN ('NIFTYBEES', 'GOLDBEES') AND trade_date <= '{target_date}'
            GROUP BY symbol, trade_date
        )
        SELECT symbol, argMax(close_price, trade_date) as march_close, max(trade_date) as actual_date
        FROM daily_data
        GROUP BY symbol
        """
        
        try:
            result = client.query(query)
            for row in result.result_rows:
                symbol, price, actual_date = row
                actual_data[year][symbol] = (price, actual_date)
        except Exception as e:
            console.print(f"[red]Error for year {year}: {e}[/red]")

    # Create Comparison Table
    table = Table(title="Verification: Gemini Results vs. ClickHouse (As of March 31)")
    table.add_column("Year", justify="center")
    table.add_column("Symbol", justify="left")
    table.add_column("Gemini Price", justify="right")
    table.add_column("ClickHouse Price", justify="right")
    table.add_column("Diff (%)", justify="right")
    table.add_column("Actual DB Date", justify="center")

    # Provided Gemini Data for mapping
    gemini_prices = {
        2017: {"NIFTYBEES": 93.13, "GOLDBEES": 27.43},
        2018: {"NIFTYBEES": 103.25, "GOLDBEES": 29.22},
        2019: {"NIFTYBEES": 118.44, "GOLDBEES": 30.87},
        2020: {"NIFTYBEES": 87.21, "GOLDBEES": 38.85},
        2021: {"NIFTYBEES": 149.47, "GOLDBEES": 41.02},
        2022: {"NIFTYBEES": 176.71, "GOLDBEES": 46.53},
        2023: {"NIFTYBEES": 175.95, "GOLDBEES": 53.21},
        2024: {"NIFTYBEES": 226.25, "GOLDBEES": 62.84},
        2025: {"NIFTYBEES": 252.04, "GOLDBEES": 74.52},
        2026: {"NIFTYBEES": 275.50, "GOLDBEES": 82.40},
    }

    for year in sorted(actual_data.keys()):
        for symbol in ['NIFTYBEES', 'GOLDBEES']:
            gem_price = gemini_prices.get(year, {}).get(symbol)
            db_entry = actual_data[year].get(symbol)
            
            if db_entry:
                db_price, db_date = db_entry
                diff = ((db_price - gem_price) / gem_price) * 100 if gem_price else 0
                diff_style = "green" if abs(diff) < 1 else "red"
                
                table.add_row(
                    str(year),
                    symbol,
                    f"₹{gem_price:.2f}" if gem_price else "N/A",
                    f"₹{db_price:.2f}",
                    f"[{diff_style}]{diff:+.2f}%[/{diff_style}]",
                    str(db_date)
                )
            else:
                table.add_row(str(year), symbol, f"₹{gem_price:.2f}" if gem_price else "N/A", "N/A", "N/A", "N/A")

    console.print(table)
    client.close()

if __name__ == "__main__":
    verify_gemini_table()
