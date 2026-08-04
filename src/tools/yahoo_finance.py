"""Compat shim — real module is src.data_importer.tool_fetchers.yahoo_finance."""
import sys as _sys
import src.data_importer.tool_fetchers.yahoo_finance as _real
_sys.modules[__name__] = _real
