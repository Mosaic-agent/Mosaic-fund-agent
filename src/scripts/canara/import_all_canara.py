"""
src/scripts/canara/import_all_canara.py
───────────────────────────────────────
CLI shortcut to fetch and import Canara Robeco Mutual Fund portfolio holdings into ClickHouse.

Usage:
    python src/scripts/canara/import_all_canara.py
    python src/scripts/canara/import_all_canara.py --full
    python src/scripts/canara/import_all_canara.py --dry-run
"""

from __future__ import annotations

import os
import sys

# Ensure project root is on sys.path
sys.path.insert(0, os.getcwd())

from src.data_importer.amc_downloaders.canara_holdings.import_all_canara import main

if __name__ == "__main__":
    main()
