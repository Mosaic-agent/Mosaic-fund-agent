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

REGISTRY: dict[str, type[BaseFundImporter]] = {
    "icici":       IciciMFImporter,
    "nippon":      NipponImporter,
    "icici-index": IciciIndexImporter,
    "dsp":         DspImporter,
}


def create_importer(name: str, **kwargs) -> BaseFundImporter:
    cls = REGISTRY.get(name)
    if cls is None:
        raise ValueError(f"Unknown importer '{name}'. Available: {list(REGISTRY)}")
    return cls(**kwargs)
