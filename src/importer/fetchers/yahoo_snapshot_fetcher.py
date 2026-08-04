"""Compat shim — real module is src.data_importer.fetchers.yahoo_snapshot_fetcher."""
import sys as _sys
import src.data_importer.fetchers.yahoo_snapshot_fetcher as _real
_sys.modules[__name__] = _real
