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
    "bajaj"       Bajaj Finserv AMC — monthly + fortnightly XLSX via AJAX
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Valid values for the `amc` argument — mirrors factory.REGISTRY keys.
AMC_KEYS: frozenset[str] = frozenset({"nippon", "dsp", "icici", "icici-index", "bajaj", "quant"})


def fetch_amc_holdings(
    amc: str,
    *,
    full_reimport: bool = False,
    dry_run: bool = False,
    target_month: str = "",
    freshness_months: int = 0,
) -> None:
    """
    Run the AMC holdings importer for *amc*.

    Parameters
    ----------
    amc              : one of ``AMC_KEYS``
    full_reimport    : ignore watermarks and re-fetch all history (nippon/dsp/quant/bajaj only)
    dry_run          : parse data but skip DB writes
    target_month     : specific month to import (format: YYYY-MM)
    freshness_months : number of most recent months to re-import
    """
    if amc not in AMC_KEYS:
        raise ValueError(f"Unknown AMC key '{amc}'. Valid: {sorted(AMC_KEYS)}")

    from src.scripts.fund_imports.factory import create_importer

    kwargs: dict = {}
    if amc in ("nippon", "dsp", "bajaj", "quant"):
        kwargs["full_reimport"] = full_reimport

    if target_month:
        from datetime import datetime
        try:
            kwargs["target_month"] = datetime.strptime(target_month, "%Y-%m").date()
        except ValueError:
            raise ValueError(f"Invalid target_month format '{target_month}'. Expected YYYY-MM.")

    if freshness_months > 0:
        kwargs["freshness_months"] = freshness_months

    logger.info(
        "fetch_amc_holdings: %s full=%s dry=%s target_month=%s freshness_months=%s",
        amc, full_reimport, dry_run, target_month or "None", freshness_months
    )
    create_importer(amc, **kwargs).run(dry_run=dry_run)

