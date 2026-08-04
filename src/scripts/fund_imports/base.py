"""Compat shim — real module is src.data_importer.amc_holdings.base."""
import sys as _sys
import src.data_importer.amc_holdings.base as _real
_sys.modules[__name__] = _real
