"""Compat shim — real module is src.data_importer.amc_holdings.importers.abakkus."""
import sys as _sys
import src.data_importer.amc_holdings.importers.abakkus as _real
_sys.modules[__name__] = _real
