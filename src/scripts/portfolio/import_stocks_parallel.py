import os
import sys
from pathlib import Path
import click

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from src.importer.registry import STOCKS, US_STOCKS
from src.importer.parallel_importer import run_parallel_stock_import

@click.command()
@click.option("--workers", default=5, help="Number of parallel worker threads.")
@click.option("--lookback", default=365, help="Lookback days for historical prices.")
@click.option("--full", is_flag=True, help="Full reimport (ignore watermarks).")
@click.option("--symbol", default=None, help="Import a specific stock symbol only.")
@click.option("--dry-run", is_flag=True, help="Fetch data but do NOT write to ClickHouse.")
def main(workers, lookback, full, symbol, dry_run):
    print(f"🚀 Initializing Parallel Stock Importer (workers limit={workers}, dry_run={dry_run})...")
    
    # Check if a single symbol was requested
    if symbol:
        sym_upper = symbol.strip().upper()
        # Find in registry
        match = [s for s in STOCKS if s[0] == sym_upper]
        if match:
            targets = [(match[0][0], match[0][1])]
            category = "stocks"
        else:
            match_us = [s for s in US_STOCKS if s[0] == sym_upper]
            if match_us:
                targets = [(match_us[0][0], match_us[0][1])]
                category = "us_stocks"
            else:
                # Direct fallback
                targets = [(sym_upper, sym_upper)]
                category = "stocks"
        
        print(f"Found 1 target stock to import: {sym_upper}")
        res = run_parallel_stock_import(targets, category, lookback, full, workers, dry_run=dry_run)
        print_summary([res], dry_run)
    else:
        # Run domestic Indian stocks and US stocks separately
        print(f"Importing {len(STOCKS)} domestic stocks and {len(US_STOCKS)} US stocks...")
        
        print("\n--- Importing Domestic Stocks ---")
        res_domestic = run_parallel_stock_import(STOCKS, "stocks", lookback, full, workers, dry_run=dry_run)
        
        print("\n--- Importing US Stocks ---")
        res_us = run_parallel_stock_import(US_STOCKS, "us_stocks", lookback, full, workers, dry_run=dry_run)
        
        print_summary([res_domestic, res_us], dry_run)

def print_summary(results, dry_run):
    total_processed = sum(r["processed"] for r in results)
    total_prices = sum(r["prices"] for r in results)
    total_earnings = sum(r["earnings"] for r in results)
    total_insider = sum(r["insider"] for r in results)
    total_valuation = sum(r["valuation"] for r in results)
    total_failures = sum(r["failures"] for r in results)
    
    print("\n================ IMPORT SUMMARY ================")
    print(f"Total Stocks Processed: {total_processed}")
    print(f"Total Prices Row Count: {total_prices}")
    print(f"Total Earnings Row Count: {total_earnings}")
    print(f"Total Insider Trades Row Count: {total_insider}")
    print(f"Total Valuation Snapshots: {total_valuation}")
    print(f"Total Failures: {total_failures}")
    if dry_run:
        print("NOTE: This was a DRY-RUN. No data was written to ClickHouse.")
    print("================================================")

if __name__ == "__main__":
    main()
