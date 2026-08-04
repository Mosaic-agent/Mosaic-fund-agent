"""Compat shim — real module is src.data_importer.tool_fetchers.zerodha_mcp_tools."""
import sys as _sys
import src.data_importer.tool_fetchers.zerodha_mcp_tools as _real
_sys.modules[__name__] = _real
