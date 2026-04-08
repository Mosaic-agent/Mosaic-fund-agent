#!/usr/bin/env python3
import os
import sys

TEMPLATE = '''"""
src/importer/fetchers/{name}_fetcher.py
──────────────────────────────────────
Data fetcher for {name} data.
"""

import logging
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)

def fetch_{name}(symbol: str, from_date: date, to_date: date) -> list[dict[str, Any]]:
    """
    Fetch {name} data for a given symbol and date range.
    
    Returns a list of dicts suitable for ClickHouse insertion.
    """
    logger.info(f"Fetching {name} for {{symbol}} from {{from_date}} to {{to_date}}")
    
    # [IMPL] Implement API call here
    rows = []
    
    # Example row structure for daily_prices:
    # rows.append({{
    #     "symbol": symbol,
    #     "category": "stocks",
    #     "trade_date": trade_date,
    #     "open": 0.0,
    #     "high": 0.0,
    #     "low": 0.0,
    #     "close": 0.0,
    #     "volume": 0.0
    # }})
    
    return rows
'''

def main():
    if len(sys.argv) < 2:
        print("Usage: python add_new_fetcher.py <name>")
        sys.exit(1)
    
    name = sys.argv[1].lower().replace("-", "_")
    filename = f"src/importer/fetchers/{name}_fetcher.py"
    
    if os.path.exists(filename):
        print(f"Error: {filename} already exists.")
        sys.exit(1)
    
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as f:
        f.write(TEMPLATE.format(name=name))
    
    print(f"✅ Created {filename}")
    print("\nNext steps:")
    print(f"1. Implement fetch_{name}() in {filename}")
    print("2. Add DDL to src/importer/clickhouse.py if a new table is needed")
    print("3. Register the source in src/importer/registry.py")
    print("4. Add the fetch logic to run_import() in src/importer/cli.py")

if __name__ == "__main__":
    main()
