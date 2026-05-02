import argparse
import yfinance as yf
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from datetime import datetime

def calculate_yoy_returns(symbols, start_year):
    console = Console()
    console.print(Panel(f"[bold cyan]🌍 Yahoo Finance API YoY Comparison (Since {start_year})[/bold cyan]", 
                        subtitle="Fetching live market data directly from Yahoo Finance"))

    all_yoy_data = {}
    
    for symbol in symbols:
        # Normalize symbol for Yahoo Finance
        if "." in symbol:
            yahoo_symbol = symbol
        elif symbol in ["GLD", "GC=F", "SPY", "QQQ"]:
            yahoo_symbol = symbol
        else:
            yahoo_symbol = f"{symbol}.NS"
        
        try:
            with console.status(f"[cyan]Fetching data for {yahoo_symbol}...[/cyan]"):
                # Fetch data starting from the previous year's end to get the baseline for the first YoY return
                data = yf.download(yahoo_symbol, start=f"{start_year - 1}-01-01", end=datetime.now().strftime("%Y-%m-%d"), progress=False)
            
            if data.empty:
                console.print(f"[yellow]No data found for {yahoo_symbol} in Yahoo Finance.[/yellow]")
                continue

            # Ensure we have closing prices
            # Yahoo Finance returns a multi-index columns for multiple tickers or a single index for one
            if isinstance(data.columns, pd.MultiIndex):
                close_prices = data['Close'][yahoo_symbol]
            else:
                close_prices = data['Close']

            # Extract last trading day per year
            # 'YE' replaces 'A' or 'Y' in newer pandas versions for Year-End resampling
            try:
                year_end_prices = close_prices.resample('YE').last()
            except ValueError:
                year_end_prices = close_prices.resample('Y').last()
            
            # Calculate YoY Returns
            # Using shift(1) to get previous year's end price
            yoy_returns = (year_end_prices / year_end_prices.shift(1)) - 1
            
            # Filter for requested start year and later
            yoy_returns = yoy_returns[yoy_returns.index.year >= start_year]
            
            for date, ret in yoy_returns.items():
                year = date.year
                if year not in all_yoy_data:
                    all_yoy_data[year] = {}
                
                # Check for NaN (if no baseline available for the first year)
                if pd.isna(ret):
                    all_yoy_data[year][symbol] = "N/A"
                else:
                    all_yoy_data[year][symbol] = f"{ret * 100:+.2f}%"

        except Exception as e:
            console.print(f"[red]Error fetching {yahoo_symbol}: {e}[/red]")

    if not all_yoy_data:
        console.print("[red]No performance data could be calculated for any provided symbol.[/red]")
        return

    # Build the table
    table = Table(title=f"Year-on-Year (YoY) Performance — Yahoo Finance API", show_header=True, header_style="bold magenta")
    table.add_column("Year", style="cyan", justify="center")
    
    sorted_symbols = sorted(symbols)
    for symbol in sorted_symbols:
        table.add_column(symbol, justify="right")
    
    # Sort years descending
    for year in sorted(all_yoy_data.keys(), reverse=True):
        row_vals = [str(year)]
        for symbol in sorted_symbols:
            val = all_yoy_data[year].get(symbol, "N/A")
            style = "bold green" if "+" in val and val != "N/A" else "bold red" if "-" in val else ""
            row_vals.append(f"[{style}]{val}[/{style}]" if style else val)
        table.add_row(*row_vals)

    console.print(table)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETF Year-on-Year Return Comparison via Yahoo Finance API")
    parser.add_argument("--symbols", nargs="+", default=["GOLDBEES", "NIFTYBEES"], help="ETF symbols to compare (e.g., GOLDBEES)")
    parser.add_argument("--start-year", type=int, default=2018, help="Starting year for YoY comparison (e.g., 2018)")
    
    args = parser.parse_args()
    calculate_yoy_returns(args.symbols, args.start_year)
