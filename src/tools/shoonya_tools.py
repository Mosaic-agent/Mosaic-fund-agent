"""Compat shim — real module is src.data_importer.tool_fetchers.shoonya_tools."""
import sys as _sys
import src.data_importer.tool_fetchers.shoonya_tools as _real
_sys.modules[__name__] = _real
