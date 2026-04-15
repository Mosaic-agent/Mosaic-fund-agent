import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
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

def compare_returns(symbols, start_date):
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

    # Convert symbols to tuple string for SQL
    symbol_filter = str(tuple(symbols)) if len(symbols) > 1 else f"('{symbols[0]}')"

    query = f"""
    WITH daily_data AS (
        SELECT symbol, trade_date, argMax(close, imported_at) as close_price
        FROM market_data.daily_prices
        WHERE symbol IN {symbol_filter} AND trade_date >= '{start_date}'
        GROUP BY symbol, trade_date
    )
    SELECT 
        symbol,
        min(trade_date) as start_date,
        argMin(close_price, trade_date) as start_price,
        max(trade_date) as end_date,
        argMax(close_price, trade_date) as end_price
    FROM daily_data
    GROUP BY symbol
    """
    
    try:
        result = client.query(query)
        if not result.result_rows:
            console.print(f"[yellow]No data found for symbols {symbols} in ClickHouse since {start_date}.[/yellow]")
            return
            
        table = Table(title=f"ETF Performance Comparison (Since {start_date})")
        table.add_column("Symbol", style="cyan", justify="left")
        table.add_column("Start Date", justify="center")
        table.add_column("End Date", justify="center")
        table.add_column("Start Price", justify="right")
        table.add_column("End Price", justify="right")
        table.add_column("Cumulative Return", style="bold green", justify="right")
        table.add_column("CAGR", style="bold magenta", justify="right")
        
        for row in result.result_rows:
            symbol, s_date, start_price, e_date, end_price = row
            
            # Calculate years
            days = (e_date - s_date).days
            years = days / 365.25
            
            # Calculate returns
            cumulative_return = (end_price / start_price) - 1
            cagr = (end_price / start_price) ** (1 / years) - 1 if years > 0 else 0
            
            table.add_row(
                symbol,
                str(s_date),
                str(e_date),
                f"₹{start_price:,.2f}",
                f"₹{end_price:,.2f}",
                f"{cumulative_return * 100:,.2f}%",
                f"{cagr * 100:,.2f}%"
            )
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[red]Error executing query or calculating returns: {e}[/red]")
    finally:
        client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETF Return Comparison")
    parser.add_argument("--symbols", nargs="+", default=["GOLDBEES", "NIFTYBEES"], help="ETF symbols to compare")
    parser.add_argument("--start-date", type=str, default="2018-01-01", help="Start date (YYYY-MM-DD)")
    
    args = parser.parse_args()
    compare_returns(args.symbols, args.start_date)
