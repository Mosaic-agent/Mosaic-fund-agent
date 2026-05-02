import os
import sys
from pathlib import Path
import clickhouse_connect
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from config.settings import settings
    from src.utils.sanity_checker import detect_yoy_anomalies, detect_daily_anomalies
except ImportError as e:
    print(f"Error importing project modules: {e}")
    sys.exit(1)

def main():
    console = Console()
    console.print(Panel("[bold red]🔍 Market Data Sanity Validator[/bold red]", subtitle="Cross-checking against economic reality"))

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

    # 1. Scan for YoY Anomalies
    with console.status("[cyan]Scanning for YoY anomalies (>40% for stable assets)...[/cyan]"):
        yoy_anomalies = detect_yoy_anomalies(client)

    if yoy_anomalies:
        table = Table(title="YoY Performance Anomalies", show_header=True, header_style="bold yellow")
        table.add_column("Year", justify="center")
        table.add_column("Symbol", style="cyan")
        table.add_column("Prev Price", justify="right")
        table.add_column("End Price", justify="right")
        table.add_column("Return (%)", style="bold red", justify="right")
        
        for a in yoy_anomalies:
            table.add_row(
                str(a["year"]),
                a["symbol"],
                f"₹{a['prev_price']:.2f}",
                f"₹{a['end_price']:.2f}",
                f"{a['return_pct']:+.2f}%"
            )
        console.print(table)
    else:
        console.print("[green]✅ No YoY anomalies detected in safe assets.[/green]")

    # 2. Scan for Daily Outliers
    with console.status("[cyan]Scanning for daily outliers (>7% movement)...[/cyan]"):
        daily_anomalies = detect_daily_anomalies(client)

    if daily_anomalies:
        table = Table(title="Daily Outliers (Exchange Circuit Thresholds)", show_header=True, header_style="bold yellow")
        table.add_column("Date", justify="center")
        table.add_column("Symbol", style="cyan")
        table.add_column("Prev Price", justify="right")
        table.add_column("Price", justify="right")
        table.add_column("Move (%)", style="bold red", justify="right")
        
        # Limit display to top 20 most recent
        for a in daily_anomalies[:20]:
            table.add_row(
                str(a["date"]),
                a["symbol"],
                f"₹{a['prev_price']:.2f}",
                f"₹{a['price']:.2f}",
                f"{a['move_pct']:+.2f}%"
            )
        console.print(table)
    else:
        console.print("[green]✅ No daily circuit-limit outliers detected.[/green]")

    client.close()

if __name__ == "__main__":
    main()
