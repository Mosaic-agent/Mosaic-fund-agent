"""Compat shim — real module is src.data_importer.registry."""
import sys as _sys
import src.data_importer.registry as _real
_sys.modules[__name__] = _real
