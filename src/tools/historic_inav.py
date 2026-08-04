"""Compat shim — real module is src.data_importer.tool_fetchers.historic_inav."""
import sys as _sys
import src.data_importer.tool_fetchers.historic_inav as _real
_sys.modules[__name__] = _real
