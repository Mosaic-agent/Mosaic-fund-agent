"""Compatibility shim forwarding to src.data_importer.amc_holdings.importers.motilal."""
from src.data_importer.amc_holdings.importers.motilal import (
    MotilalOswalImporter,
    _parse_disclosure_date,
    _parse_sheet_date,
    _clean_scheme_name,
)

__all__ = [
    "MotilalOswalImporter",
    "_parse_disclosure_date",
    "_parse_sheet_date",
    "_clean_scheme_name",
]
