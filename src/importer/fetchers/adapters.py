"""Compat shim — real module is src.data_importer.fetchers.adapters."""
import sys as _sys
import src.data_importer.fetchers.adapters as _real
_sys.modules[__name__] = _real
