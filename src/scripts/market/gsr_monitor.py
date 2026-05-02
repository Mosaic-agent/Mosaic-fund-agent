"""
scripts/gsr_monitor.py
───────────────────────
Monitors the Gold-Silver Ratio (GSR) based on Ritesh Jain's "80/50 Rule":
  - GSR > 80: Buy Silver (Silver is undervalued relative to Gold)
  - GSR < 50: Rotate to Gold (Gold is undervalued relative to Silver)

Uses International Futures (GC=F / SI=F) for the standard global ratio.
"""

import sys
import os
from typing import Optional

# Add project root to sys.path
sys.path.append(os.getcwd())

from config.settings import settings
import clickhouse_connect
from src.tools.yahoo_finance import fetch_yahoo_data
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

def get_latest_futures_price(symbol: str) -> float:
    """Fetch latest price from yfinance directly for international futures."""
    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        # Try fast_info first, then history
        price = ticker.fast_info.get('last_price')
        if not price:
            hist = ticker.history(period='1d')
            price = hist['Close'].iloc[-1]
        return float(price)
    except:
        return 0.0

def run_gsr_monitor():
    console = Console()
    
    # Ritesh Jain Thresholds
    BUY_SILVER_THRESHOLD = 80
    ROTATE_GOLD_THRESHOLD = 50
    
    gold_price = get_latest_futures_price("GC=F") 
    silver_price = get_latest_futures_price("SI=F") 
    
    if gold_price <= 0 or silver_price <= 0:
        console.print("[bold red]❌ Error: Could not fetch Gold or Silver futures prices.[/bold red]")
        return

    gsr = gold_price / silver_price
    
    console.print(Panel(
        f"[bold magenta]🥇 Gold-Silver Ratio (GSR) Monitor[/bold magenta]\n"
        f"[dim]Ritesh Jain's '80/50 Rule' Tracker[/dim]",
        border_style="magenta"
    ))

    table = Table(show_header=False, box=None)
    table.add_column("Key", style="cyan")
    table.add_column("Value", style="white")

    table.add_row("Gold Futures (GC=F)", f"${gold_price:,.2f}")
    table.add_row("Silver Futures (SI=F)", f"${silver_price:,.2f}")
    table.add_row("Current GSR", f"[bold yellow]{gsr:.2f}[/bold yellow]")
    
    console.print(table)

    # Signal Logic
    if gsr >= BUY_SILVER_THRESHOLD:
        status = "🟢 SCREAMING BUY: SILVER"
        reason = f"GSR ({gsr:.2f}) is above {BUY_SILVER_THRESHOLD}. Silver is historically undervalued. Overweight SILVERBEES."
        style = "bold green"
    elif gsr <= ROTATE_GOLD_THRESHOLD:
        status = "🔴 ROTATE TO GOLD"
        reason = f"GSR ({gsr:.2f}) is below {ROTATE_GOLD_THRESHOLD}. Silver euphoria reached. Lock silver profits, rotate to GOLDBEES."
        style = "bold red"
    else:
        status = "🟡 NEUTRAL / SILVER MOMENTUM"
        reason = "GSR is in the middle zone. Ride the Silver 'catch-up' rally toward the 50 target."
        style = "bold yellow"

    console.print(Panel(
        f"[{style}]SIGNAL: {status}[/{style}]\n"
        f"[dim]{reason}[/dim]",
        border_style=style.split()[-1]
    ))

    # Calculate distance to target
    if gsr > ROTATE_GOLD_THRESHOLD:
        distance = ((gsr - ROTATE_GOLD_THRESHOLD) / gsr) * 100
        console.print(f"\n[dim]Distance to Ritesh's 'Rotate to Gold' Target (50): [bold]{distance:.1f}%[/bold] drop in GSR required.[/dim]")

if __name__ == "__main__":
    run_gsr_monitor()
