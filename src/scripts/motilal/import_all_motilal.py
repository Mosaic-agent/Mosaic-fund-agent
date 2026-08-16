#!/usr/bin/env python3
"""
src/scripts/motilal/import_all_motilal.py
──────────────────────────────────────────
CLI shortcut to import Motilal Oswal Mutual Fund portfolio holdings into ClickHouse.

Usage:
    python src/scripts/motilal/import_all_motilal.py [--dry-run] [--full] [--month YYYY-MM]
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add project root to sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.data_importer.amc_downloaders.motilal_holdings.import_all_motilal import main

if __name__ == "__main__":
    main()
