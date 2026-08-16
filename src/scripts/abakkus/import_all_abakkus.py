"""
src/scripts/abakkus/import_all_abakkus.py
──────────────────────────────────────────
CLI shortcut to fetch and import Abakkus Mutual Fund portfolio holdings into ClickHouse.

Usage:
    python src/scripts/abakkus/import_all_abakkus.py
    python src/scripts/abakkus/import_all_abakkus.py --full
    python src/scripts/abakkus/import_all_abakkus.py --dry-run
"""

from __future__ import annotations

import os
import sys

# Ensure project root is on sys.path
sys.path.insert(0, os.getcwd())

from src.data_importer.abakkus_holdings.import_all_abakkus import main

if __name__ == "__main__":
    main()
