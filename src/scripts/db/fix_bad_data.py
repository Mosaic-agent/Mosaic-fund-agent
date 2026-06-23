#!/usr/bin/env python3
"""
src/scripts/db/fix_bad_data.py
─────────────────────────────
Database repair and data quality automation utility.
Forces table deduplication, removes negative equity prices,
aligns watermarks, and runs sanity check validation.
"""

import os
import sys
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

try:
    from config.settings import settings
    from src.utils.sanity_checker import detect_yoy_anomalies, detect_daily_anomalies
    from src.events.bus import get_event_bus, DataImportedEvent
    from src.events.observers import setup_observers
except ImportError as e:
    print(f"Error importing project modules: {e}")
    sys.exit(1)

def main():
    console = Console()
    console.print(Panel("[bold green]🔧 Market Data Quality Repair & Maintenance[/bold green]", subtitle="Cleaning database and aligning watermarks"))

    try:
        from src.db.pool import get_client
        client = get_client()
    except Exception as e:
        console.print(f"[red]Error connecting to ClickHouse: {e}[/red]")
        sys.exit(1)

    # 1. Physical Deduplication
    with console.status("[cyan]Force optimizing tables for physical deduplication...[/cyan]"):
        try:
            tables = [row[0] for row in client.query('SHOW TABLES').result_rows]
            for t in tables:
                client.command(f"OPTIMIZE TABLE {t} FINAL")
            console.print("✅ All ClickHouse tables physically optimized and deduplicated.")
        except Exception as e:
            console.print(f"[red]Error optimizing tables: {e}[/red]")

    # 2. Clean Invalid Negative Prices
    with console.status("[cyan]Cleaning up invalid negative equity prices...[/cyan]"):
        try:
            # We preserve CRUDEOIL and US13W because negative yields and futures prices are real economic anomalies.
            # Equity shares cannot trade at or below 0.
            check_neg = client.command("SELECT count() FROM market_data.daily_prices WHERE close <= 0 AND symbol NOT IN ('CRUDEOIL', 'US13W')")
            if int(check_neg) > 0:
                client.command("ALTER TABLE market_data.daily_prices DELETE WHERE close <= 0 AND symbol NOT IN ('CRUDEOIL', 'US13W')")
                console.print(f"✅ Cleaned {check_neg} invalid negative equity price rows from daily_prices.")
            else:
                console.print("✅ No invalid negative equity prices found.")
        except Exception as e:
            console.print(f"[red]Error cleaning negative prices: {e}[/red]")

    # 3. Align Watermarks
    with console.status("[cyan]Checking and aligning delta-sync watermarks...[/cyan]"):
        try:
            # yfinance max date sync
            query_max = "SELECT symbol, max(trade_date) FROM market_data.daily_prices GROUP BY symbol"
            actual_max_dates = {row[0]: row[1] for row in client.query(query_max).result_rows}

            query_wm = "SELECT symbol, last_date FROM market_data.import_watermarks WHERE source = 'yfinance'"
            watermarks = {row[0]: row[1] for row in client.query(query_wm).result_rows}

            mismatches = 0
            for symbol, actual in actual_max_dates.items():
                wm = watermarks.get(symbol)
                if wm != actual:
                    client.command(f"ALTER TABLE market_data.import_watermarks UPDATE last_date = '{actual.strftime('%Y-%m-%d')}' WHERE source = 'yfinance' AND symbol = '{symbol}'")
                    mismatches += 1
            
            if mismatches > 0:
                console.print(f"✅ Aligned {mismatches} mismatched watermarks with actual max price dates.")
            else:
                console.print("✅ All delta-sync watermarks are perfectly aligned.")
        except Exception as e:
            console.print(f"[red]Error aligning watermarks: {e}[/red]")

    # 4. Trigger Sanity Check Observer
    console.print("\n[bold cyan]▶ Triggering SanityCheckObserver via EventBus…[/bold cyan]")
    try:
        setup_observers()
        bus = get_event_bus()
        event = DataImportedEvent(source="repair_pipeline", category="system", symbol_key="repair")
        bus.publish(event)
        console.print("✅ SanityCheckObserver triggered successfully.")
    except Exception as e:
        console.print(f"[red]Failed to trigger observer: {e}[/red]")

    client.close()
    console.print("\n[bold green]✓ Data Quality repairs complete.[/bold green]")

if __name__ == "__main__":
    main()
