"""Compat shim — real module is src.data_importer.clickhouse."""
import sys as _sys
import src.data_importer.clickhouse as _real
_sys.modules[__name__] = _real
