"""Compat shim — real module is src.data_importer.fetchers.etf_aum_fetcher."""
import sys as _sys
import src.data_importer.fetchers.etf_aum_fetcher as _real
_sys.modules[__name__] = _real
