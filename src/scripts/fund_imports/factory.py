"""
scripts/fund_imports/factory.py
────────────────────────────────
Registry and factory function for fund importers.

Usage:
    from src.scripts.fund_imports.factory import create_importer
    imp = create_importer("nippon", from_year=2025)
    imp.run(dry_run=True)
"""

from __future__ import annotations

import os
import sys

sys.path.append(os.getcwd())

from src.scripts.fund_imports.base import BaseFundImporter
from src.scripts.fund_imports.importers.icici_mf import IciciMFImporter
from src.scripts.fund_imports.importers.nippon import NipponImporter
from src.scripts.fund_imports.importers.icici_index import IciciIndexImporter
from src.scripts.fund_imports.importers.dsp import DspImporter
from src.scripts.fund_imports.importers.bajaj import BajajImporter
from src.scripts.fund_imports.importers.quant import QuantImporter
from src.scripts.fund_imports.importers.amfi import AmfiImporter
from src.scripts.fund_imports.importers.hdfc import HdfcImporter
from src.scripts.fund_imports.importers.kotak import KotakImporter

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
