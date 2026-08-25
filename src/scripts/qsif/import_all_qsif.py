"""Compat shim — real module is src.data_importer.amc_downloaders.qsif_holdings.import_all_qsif."""
import sys as _sys
from pathlib import Path as _Path

_sys.path.insert(0, str(_Path(__file__).resolve().parents[3]))

import src.data_importer.amc_downloaders.qsif_holdings.import_all_qsif as _real
_sys.modules[__name__] = _real

if __name__ == "__main__":
    _real.main()
