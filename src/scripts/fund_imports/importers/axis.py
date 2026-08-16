"""
src/scripts/fund_imports/importers/axis.py
───────────────────────────────────────────
Forwarding shim for backwards compatibility.
Canonical implementation is at:
    src.data_importer.amc_holdings.importers.axis.AxisImporter
"""

from src.data_importer.amc_holdings.importers.axis import (  # noqa: F401
    AxisImporter,
    SCHEME_MAP,
    _parse_disclosure_date,
    _normalise_fund_identity,
)
