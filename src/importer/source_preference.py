"""Compat shim — real module is src.data_importer.source_preference."""
import sys as _sys
import src.data_importer.source_preference as _real
_sys.modules[__name__] = _real
