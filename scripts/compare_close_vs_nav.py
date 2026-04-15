import os
import sys
import argparse
from pathlib import Path
import clickhouse_connect
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from config.settings import settings
except ImportError as e:
    print(f"Error importing project modules: {e}")
    sys.exit(1)

def compare_close_vs_nav(symbol, start_year):
    console = Console()
    console.print(Panel(f"[bold cyan]🔍 Close Price vs. Official NAV Anomaly Check[/bold cyan]", 
                        subtitle=f"Checking if {symbol} traded at a massive premium in 2025"))
    
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

    # Query to fetch both the Last Close Price and the Last NAV for each year
    query = f"""
    WITH close_data AS (
        SELECT 
            toYear(trade_date) as year,
            argMax(close, trade_date) as end_close
        FROM market_data.daily_prices
        WHERE symbol = '{symbol}' AND trade_date >= '{start_year - 1}-01-01'
        GROUP BY year
    ),
    nav_data AS (
        SELECT 
            toYear(nav_date) as year,
            argMax(nav, nav_date) as end_nav
        FROM market_data.mf_nav
        WHERE symbol = '{symbol}' AND nav_date >= '{start_year - 1}-01-01'
        GROUP BY year
    )
    SELECT 
        c.year,
        c.end_close,
        n.end_nav,
        any(c.end_close) OVER (ORDER BY c.year ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as prev_close,
        any(n.end_nav) OVER (ORDER BY n.year ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as prev_nav
    FROM close_data c
    LEFT JOIN nav_data n ON c.year = n.year
    WHERE c.year >= {start_year}
    ORDER BY c.year DESC
    """
    
    try:
        result = client.query(query)
        if not result.result_rows:
            console.print(f"[yellow]No data found for {symbol} since {start_year}.[/yellow]")
            return
            
        table = Table(title=f"{symbol} YoY Performance: Market Close vs. Fund NAV", show_header=True, header_style="bold magenta")
        table.add_column("Year", style="cyan", justify="center")
        table.add_column("Market Close (₹)", justify="right")
        table.add_column("Fund NAV (₹)", justify="right")
        table.add_column("Premium/Disc.", justify="right")
        table.add_column("Close Return", style="bold green", justify="right")
        table.add_column("NAV Return", style="bold yellow", justify="right")
        
        for row in result.result_rows:
            year, end_close, end_nav, prev_close, prev_nav = row
            
            # Premium/Discount calculation for the end of the year
            premium_disc = ((end_close / end_nav) - 1) * 100 if end_nav else 0
            pd_style = "green" if premium_disc > 0 else "red"
            
            # YoY Returns
            close_ret = ((end_close / prev_close) - 1) * 100 if prev_close else 0
            nav_ret = ((end_nav / prev_nav) - 1) * 100 if prev_nav and end_nav else 0
            
            table.add_row(
                str(year),
                f"₹{end_close:.2f}",
                f"₹{end_nav:.2f}" if end_nav else "N/A",
                f"[{pd_style}]{premium_disc:+.2f}%[/{pd_style}]" if end_nav else "N/A",
                f"{close_ret:+.2f}%" if prev_close else "N/A",
                f"{nav_ret:+.2f}%" if prev_nav else "N/A"
            )
            
        console.print(table)
        
    except Exception as e:
        console.print(f"[red]Error executing query: {e}[/red]")
    finally:
        client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare Close vs NAV Returns")
    parser.add_argument("--symbol", type=str, default="GOLDBEES", help="ETF symbol to check (e.g., GOLDBEES)")
    parser.add_argument("--start-year", type=int, default=2018, help="Starting year")
    
    args = parser.parse_args()
    compare_close_vs_nav(args.symbol, args.start_year)
