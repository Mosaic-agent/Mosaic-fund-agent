"""Compat shim — real module is src.data_importer.freshness."""
import sys as _sys
import src.data_importer.freshness as _real
_sys.modules[__name__] = _real
