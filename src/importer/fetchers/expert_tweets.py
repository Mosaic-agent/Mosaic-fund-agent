"""Compat shim — real module is src.data_importer.fetchers.expert_tweets."""
import sys as _sys
import src.data_importer.fetchers.expert_tweets as _real
_sys.modules[__name__] = _real
