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

def calculate_returns(symbol, start_date):
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

    query = f"""
    WITH daily_data AS (
        SELECT trade_date, argMax(close, imported_at) as close_price
        FROM market_data.daily_prices
        WHERE symbol = '{symbol}' AND trade_date >= '{start_date}'
        GROUP BY trade_date
    )
    SELECT 
        min(trade_date) as start_date,
        argMin(close_price, trade_date) as start_price,
        max(trade_date) as end_date,
        argMax(close_price, trade_date) as end_price
    FROM daily_data
    """
    
    try:
        result = client.query(query)
        if not result.result_rows or result.result_rows[0][0] is None:
            console.print(f"[yellow]No data found for {symbol} in ClickHouse since {start_date}.[/yellow]")
            return
            
        row = result.result_rows[0]
        s_date, start_price, e_date, end_price = row
        
        # Calculate years
        days = (e_date - s_date).days
        years = days / 365.25
        
        # Calculate returns
        cumulative_return = (end_price / start_price) - 1
        cagr = (end_price / start_price) ** (1 / years) - 1 if years > 0 else 0
        
        # Display Table
        table = Table(title=f"{symbol} Performance Validation (Since {start_date})")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="magenta")
        
        table.add_row("Symbol", symbol)
        table.add_row("Start Date", str(s_date))
        table.add_row("Start Price", f"₹{start_price:,.2f}")
        table.add_row("End Date", str(e_date))
        table.add_row("End Price", f"₹{end_price:,.2f}")
        table.add_row("Total Days", str(days))
        table.add_row("Total Years", f"{years:.2f}")
        table.add_section()
        table.add_row("Cumulative Return", f"{cumulative_return * 100:.2f}%")
        table.add_row("CAGR", f"{cagr * 100:.2f}%")
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[red]Error executing query or calculating returns: {e}[/red]")
    finally:
        client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETF CAGR Validation")
    parser.add_argument("--symbol", type=str, default="GOLDBEES", help="ETF symbol to validate")
    parser.add_argument("--start-date", type=str, default="2018-01-01", help="Start date (YYYY-MM-DD)")
    
    args = parser.parse_args()
    calculate_returns(args.symbol, args.start_date)
