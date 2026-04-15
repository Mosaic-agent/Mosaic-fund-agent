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

def get_yoy_comparison(symbols, start_year):
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
        SELECT 
            symbol, 
            toYear(trade_date) as year,
            trade_date,
            argMax(close, imported_at) as close_price
        FROM market_data.daily_prices
        WHERE symbol IN {symbol_filter} AND trade_date >= '{start_year - 1}-01-01'
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
    SELECT * FROM yoy_calc
    WHERE year >= {start_year}
    ORDER BY year DESC, symbol ASC
    """
    
    try:
        result = client.query(query)
        if not result.result_rows:
            console.print(f"[yellow]No data found for symbols {symbols} in ClickHouse for year >= {start_year}.[/yellow]")
            return
            
        # Structure data for the table: {year: {symbol: return}}
        yoy_data = {}
        found_symbols = set()
        for row in result.result_rows:
            year, symbol, end_price, prev_price = row
            found_symbols.add(symbol)
            if year not in yoy_data:
                yoy_data[year] = {}
            
            if prev_price:
                ret = (end_price / prev_price) - 1
                yoy_data[year][symbol] = f"{ret * 100:+.2f}%"
            else:
                yoy_data[year][symbol] = "N/A"

        sorted_symbols = sorted(list(found_symbols))
        table = Table(title=f"Year-on-Year (YoY) Returns Comparison (Since {start_year})")
        table.add_column("Year", style="cyan", justify="center")
        for symbol in sorted_symbols:
            table.add_column(symbol, justify="right")
        
        # Sort years descending
        for year in sorted(yoy_data.keys(), reverse=True):
            row_vals = [str(year)]
            for symbol in sorted_symbols:
                val = yoy_data[year].get(symbol, "N/A")
                style = "bold green" if "+" in val and val != "N/A" else "bold red" if "-" in val else ""
                row_vals.append(f"[{style}]{val}[/{style}]" if style else val)
            table.add_row(*row_vals)
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[red]Error executing query or calculating returns: {e}[/red]")
    finally:
        client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETF Year-on-Year Return Comparison")
    parser.add_argument("--symbols", nargs="+", default=["GOLDBEES", "NIFTYBEES"], help="ETF symbols to compare")
    parser.add_argument("--start-year", type=int, default=2018, help="Starting year for comparison")
    
    args = parser.parse_args()
    get_yoy_comparison(args.symbols, args.start_year)
