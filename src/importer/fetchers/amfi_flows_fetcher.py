"""Compat shim — real module is src.data_importer.fetchers.amfi_flows_fetcher."""
import sys as _sys
import src.data_importer.fetchers.amfi_flows_fetcher as _real
_sys.modules[__name__] = _real
