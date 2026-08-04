"""Compat shim — real module is src.data_importer.fetchers."""
import sys as _sys
import src.data_importer.fetchers as _real
_sys.modules[__name__] = _real
