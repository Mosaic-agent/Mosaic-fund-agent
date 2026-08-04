"""Compat shim — real module is src.data_importer.cli."""
import sys as _sys
import src.data_importer.cli as _real
_sys.modules[__name__] = _real
