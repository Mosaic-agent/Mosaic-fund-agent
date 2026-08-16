"""
src/scripts/helios/import_all_helios.py
────────────────────────────────────────
CLI shortcut to fetch and import Helios Mutual Fund portfolio holdings into ClickHouse.

Usage:
    python src/scripts/helios/import_all_helios.py
    python src/scripts/helios/import_all_helios.py --full
    python src/scripts/helios/import_all_helios.py --dry-run
"""

from __future__ import annotations

import os
import sys

# Ensure project root is on sys.path
sys.path.insert(0, os.getcwd())

from src.data_importer.helios_holdings.import_all_helios import main

if __name__ == "__main__":
    main()
