"""
src/scripts/mirae/import_all_mirae.py
─────────────────────────────────────
Shortcut script to run Mirae Asset Mutual Fund portfolio imports.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root is on sys.path
_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.data_importer.mirae_holdings.import_all_mirae import main

if __name__ == "__main__":
    main()
