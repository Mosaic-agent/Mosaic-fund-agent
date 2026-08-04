"""Compat shim — real module is src.data_importer.tool_fetchers.earnings_scraper."""
import sys as _sys
import src.data_importer.tool_fetchers.earnings_scraper as _real
_sys.modules[__name__] = _real
