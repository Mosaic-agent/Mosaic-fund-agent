"""Compat shim — real module is src.data_importer.tool_fetchers.news_search."""
import sys as _sys
import src.data_importer.tool_fetchers.news_search as _real
_sys.modules[__name__] = _real
