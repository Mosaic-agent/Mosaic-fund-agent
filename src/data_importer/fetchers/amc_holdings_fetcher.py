"""
src/importer/fetchers/amc_holdings_fetcher.py
──────────────────────────────────────────────
Canonical fetcher entry point for AMC monthly fund-holdings importers.

Wraps the BaseFundImporter factory in src/scripts/fund_imports/ so that
cli.py can invoke AMC holdings the same way it calls every other fetcher:

    from src.data_importer.fetchers.amc_holdings_fetcher import fetch_amc_holdings
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
AMC_KEYS: frozenset[str] = frozenset({"nippon", "dsp", "icici", "icici-index", "bajaj", "quant", "amfi", "hdfc", "kotak", "abakkus", "abacus", "helios", "invesco", "canara", "canara_robeco", "canara-robeco"})


def fetch_amc_holdings(
    amc: str,
    *,
    full_reimport: bool = False,
    dry_run: bool = False,
    target_month: str = "",
    freshness_months: int = 0,
    max_workers: int | None = None,
) -> None:
    """
    Run the AMC holdings importer for *amc*.

    Parameters
    ----------
    amc              : one of ``AMC_KEYS``
    full_reimport    : ignore watermarks and re-fetch all history
    dry_run          : parse data but skip DB writes
    target_month     : specific month to import (format: YYYY-MM)
    freshness_months : number of most recent months to re-import
    max_workers      : bounded parallel source fetches per AMC (1–4); defaults
                       to ``AMC_IMPORT_MAX_WORKERS`` or the importer default
    """
    if amc not in AMC_KEYS:
        raise ValueError(f"Unknown AMC key '{amc}'. Valid: {sorted(AMC_KEYS)}")

    from src.data_importer.amc_holdings.factory import create_importer

    kwargs: dict = {}
    if amc in ("nippon", "dsp", "bajaj", "quant", "amfi", "hdfc", "kotak", "abakkus", "abacus", "helios", "invesco", "canara", "canara_robeco", "canara-robeco"):
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
    importer = create_importer(amc, **kwargs)
    if max_workers is not None:
        importer.set_source_workers(max_workers)
    importer.run(dry_run=dry_run)
