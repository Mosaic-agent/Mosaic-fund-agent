"""
src/tools/valuation_alerts.py
──────────────────────────────
Valuation Re-rating Alert engine for core CPSE stocks.

Tracks current P/E ratios against 5-year historical averages to identify 
reversion-to-mean opportunities or structural re-rating signals.
"""
from __future__ import annotations

import logging
from typing import Any
from dataclasses import dataclass

from src.tools.yahoo_finance import fetch_yahoo_data

log = logging.getLogger(__name__)

# 5-Year Average P/E Targets (Research-based as of April 2026)
HISTORICAL_PE_TARGETS = {
    "NTPC":      12.5,
    "POWERGRID": 11.9,
    "ONGC":      7.7,
    "COALINDIA": 7.8,
    "IOC":       6.9,
}

@dataclass
class ValuationAlert:
    symbol: str
    current_pe: float
    avg_pe: float
    distance_pct: float
    action: str
    style: str

def check_valuation_alerts() -> list[ValuationAlert]:
    """
    Compare current P/E ratios with historical averages and generate signals.
    """
    alerts = []
    
    for symbol, avg_pe in HISTORICAL_PE_TARGETS.items():
        try:
            data = fetch_yahoo_data(symbol)
            current_pe = data.pe_ratio
            
            if current_pe <= 0:
                continue
                
            distance = ((current_pe - avg_pe) / avg_pe) * 100
            
            if current_pe <= avg_pe:
                action = "🟢 HISTORICAL VALUE (BUY)"
                style = "bold green"
            elif distance <= 15:
                action = "🟡 NEAR AVG (WATCH)"
                style = "yellow"
            elif distance > 50:
                action = "🔴 RE-RATED / EXPENSIVE"
                style = "red"
            else:
                action = "⚪ FAIR VALUE"
                style = "dim"
                
            alerts.append(ValuationAlert(
                symbol=symbol,
                current_pe=round(current_pe, 2),
                avg_pe=avg_pe,
                distance_pct=round(distance, 2),
                action=action,
                style=style
            ))
        except Exception as e:
            log.warning(f"Failed to check valuation for {symbol}: {e}")
            
    # Sort by distance (closest to avg first)
    alerts.sort(key=lambda x: x.distance_pct)
    return alerts

def print_valuation_report(alerts: list[ValuationAlert]):
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel

    console = Console()
    
    console.print(Panel(
        "[bold cyan]⚖️ CPSE Valuation Re-rating Alerts[/bold cyan]\n"
        "[dim]Comparing current P/E vs 5-Year Historical Averages[/dim]",
        border_style="cyan"
    ))

    table = Table(show_header=True, header_style="bold white")
    table.add_column("Symbol", style="cyan")
    table.add_column("Current P/E", justify="right")
    table.add_column("5-Yr Avg P/E", justify="right", style="dim")
    table.add_column("Distance (%)", justify="right")
    table.add_column("Signal", justify="left")

    for alert in alerts:
        dist_str = f"{alert.distance_pct:+.1f}%"
        table.add_row(
            alert.symbol,
            str(alert.current_pe),
            str(alert.avg_pe),
            dist_str,
            f"[{alert.style}]{alert.action}[/{alert.style}]"
        )

    console.print(table)
    console.print("\n[dim]Note: Positive distance means the stock is trading at a premium to its 5-year average valuation.[/dim]")

if __name__ == "__main__":
    alerts = check_valuation_alerts()
    print_valuation_report(alerts)
