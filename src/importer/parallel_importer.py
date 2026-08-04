"""Compat shim — real module is src.data_importer.parallel_importer."""
import sys as _sys
import src.data_importer.parallel_importer as _real
_sys.modules[__name__] = _real
