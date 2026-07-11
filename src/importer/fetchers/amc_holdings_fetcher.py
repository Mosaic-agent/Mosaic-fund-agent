"""
src/importer/fetchers/amc_holdings_fetcher.py
──────────────────────────────────────────────
Canonical fetcher entry point for AMC monthly fund-holdings importers.

Wraps the BaseFundImporter factory in src/scripts/fund_imports/ so that
cli.py can invoke AMC holdings the same way it calls every other fetcher:

    from src.importer.fetchers.amc_holdings_fetcher import fetch_amc_holdings
    fetch_amc_holdings("nippon", full_reimport=False, dry_run=False)

Supported AMC keys
──────────────────
    "nippon"      Nippon India AMC — monthly XLS files (2017 → present)
    "dsp"         DSP Mutual Fund   — monthly ZIP files (2022 → present)
    "icici"       ICICI Prudential MF — Morningstar API snapshot
    "icici-index" ICICI Prudential index constituents — Azure Blob
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Valid values for the `amc` argument — mirrors factory.REGISTRY keys.
AMC_KEYS: frozenset[str] = frozenset({"nippon", "dsp", "icici", "icici-index", "quant"})


def fetch_amc_holdings(
    amc: str,
    *,
    full_reimport: bool = False,
    dry_run: bool = False,
) -> None:
    """
    Run the AMC holdings importer for *amc*.

    Parameters
    ----------
    amc           : one of ``AMC_KEYS``
    full_reimport : ignore watermarks and re-fetch all history (nippon/dsp/quant only)
    dry_run       : parse data but skip DB writes

    Raises
    ------
    ValueError    : unknown amc key
    """
    if amc not in AMC_KEYS:
        raise ValueError(f"Unknown AMC key '{amc}'. Valid: {sorted(AMC_KEYS)}")

    from src.scripts.fund_imports.factory import create_importer

    kwargs: dict = {}
    if amc in ("nippon", "dsp", "quant"):
        kwargs["full_reimport"] = full_reimport

    logger.info("fetch_amc_holdings: %s full=%s dry=%s", amc, full_reimport, dry_run)
    create_importer(amc, **kwargs).run(dry_run=dry_run)

