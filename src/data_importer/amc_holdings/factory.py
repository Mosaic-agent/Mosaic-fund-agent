"""
scripts/fund_imports/factory.py
────────────────────────────────
Registry and factory function for fund importers.

Usage:
    from src.data_importer.amc_holdings.factory import create_importer
    imp = create_importer("nippon", from_year=2025)
    imp.run(dry_run=True)
"""

from __future__ import annotations

import os
import sys

sys.path.append(os.getcwd())

from src.data_importer.amc_holdings.base import BaseFundImporter
from src.data_importer.amc_holdings.importers.icici_mf import IciciMFImporter
from src.data_importer.amc_holdings.importers.nippon import NipponImporter
from src.data_importer.amc_holdings.importers.icici_index import IciciIndexImporter
from src.data_importer.amc_holdings.importers.dsp import DspImporter
from src.data_importer.amc_holdings.importers.bajaj import BajajImporter
from src.data_importer.amc_holdings.importers.quant import QuantImporter
from src.data_importer.amc_holdings.importers.amfi import AmfiImporter
from src.data_importer.amc_holdings.importers.hdfc import HdfcImporter
from src.data_importer.amc_holdings.importers.kotak import KotakImporter

REGISTRY: dict[str, type[BaseFundImporter]] = {
    "icici":       IciciMFImporter,
    "nippon":      NipponImporter,
    "icici-index": IciciIndexImporter,
    "dsp":         DspImporter,
    "bajaj":       BajajImporter,
    "quant":       QuantImporter,
    "amfi":        AmfiImporter,
    "kotak":       KotakImporter,
    "hdfc":        HdfcImporter,
}


def register_importer(name: str):
    """Decorator to register a BaseFundImporter subclass in REGISTRY."""
    def decorator(cls: type[BaseFundImporter]):
        REGISTRY[name] = cls
        return cls
    return decorator


def create_importer(name: str, **kwargs) -> BaseFundImporter:
    cls = REGISTRY.get(name)
    if cls is None:
        raise ValueError(f"Unknown importer '{name}'. Available: {list(REGISTRY)}")
    return cls(**kwargs)
