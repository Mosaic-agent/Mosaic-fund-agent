"""
src/scripts/invesco/import_all_invesco.py
─────────────────────────────────────────
CLI shortcut to fetch and import Invesco Mutual Fund portfolio holdings into ClickHouse.

Usage:
    python src/scripts/invesco/import_all_invesco.py
    python src/scripts/invesco/import_all_invesco.py --full
    python src/scripts/invesco/import_all_invesco.py --dry-run
"""

from __future__ import annotations

import os
import sys

# Ensure project root is on sys.path
sys.path.insert(0, os.getcwd())

from src.data_importer.amc_downloaders.invesco_holdings.import_all_invesco import main

if __name__ == "__main__":
    main()
