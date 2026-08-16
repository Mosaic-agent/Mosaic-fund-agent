#!/usr/bin/env python3
"""
src/scripts/axis/import_all_axis.py
───────────────────────────────────
Shortcut CLI to import Axis Mutual Fund monthly portfolio disclosures.

Usage:
    python src/scripts/axis/import_all_axis.py
    python src/scripts/axis/import_all_axis.py --dry-run
    python src/scripts/axis/import_all_axis.py --month 2026-07
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root is on sys.path
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data_importer.amc_downloaders.axis_holdings.import_all_axis import main

if __name__ == "__main__":
    main()
