"""Compat shim — real module is src.data_importer.backfillers.backfill_ml_predictions."""
import sys as _sys
import src.data_importer.backfillers.backfill_ml_predictions as _real
_sys.modules[__name__] = _real
