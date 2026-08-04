"""Compat shim — real module is src.data_importer.amc_holdings.importers.hdfc."""
import sys as _sys
import src.data_importer.amc_holdings.importers.hdfc as _real
_sys.modules[__name__] = _real
