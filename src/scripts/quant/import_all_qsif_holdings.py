"""
src/scripts/quant/import_all_qsif_holdings.py
─────────────────────────────────────────────
Compat shim — redirected to unified Quant SIF Excel importer:
src.data_importer.amc_downloaders.qsif_holdings.import_all_qsif
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from src.data_importer.amc_downloaders.qsif_holdings.import_all_qsif import main

if __name__ == "__main__":
    main()