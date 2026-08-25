"""
src/scripts/edelweiss/backfill_6m_edelweiss_holdings.py
──────────────────────────────────────────────────────
Compat shim — redirected to unified Edelweiss Excel importer with historical discovery:
src.data_importer.amc_downloaders.edelweiss_holdings.import_all_edelweiss
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from src.data_importer.amc_downloaders.edelweiss_holdings.import_all_edelweiss import main

if __name__ == "__main__":
    main()
