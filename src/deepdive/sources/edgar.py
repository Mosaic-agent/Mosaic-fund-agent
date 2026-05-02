"""
src/deepdive/sources/edgar.py
──────────────────────────────
SEC EDGAR data fetcher.

Company lookup uses sec-api.io MappingApi (ticker → CIK).
Filing discovery uses the SEC's own free submissions JSON API
  (https://data.sec.gov/submissions/CIK{cik}.json) — no API key required,
  no rate-limit issues.  QueryApi is not used (requires paid sec-api tier).
Filing HTML download uses sec-api.io DownloadApi (mirrors EDGAR at 200+ req/s).

Cache-first pattern (mirrors src/importer/fetchers/cot_fetcher.py):
  - Stateless functions; raw blobs written to cache_dir on first call
  - On re-run with the same cache_dir: read from disk, skip network
  - Never raises on network failure; logs warning and returns empty value

Files fetched for v1 (Autodesk):
  Latest 10-K         → 10-K-{filed_date}.htm
  Last 4 10-Qs        → 10-Q-{filed_date}.htm
  Last 12 8-Ks        → 8-K-{filed_date}.htm
  Latest DEF 14A      → DEF14A-{filed_date}.htm
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

import requests

log = logging.getLogger(__name__)

_EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
_EDGAR_ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{doc}"
_EDGAR_HEADERS = {"User-Agent": "Mosaic-agent research@mosaic-agent.io"}

# Filing limits per form type
_FETCH_LIMITS: dict[str, int] = {
    "10-K": 1,
    "10-Q": 4,
    "8-K": 12,
    "DEF 14A": 1,
}


# ── Company lookup ────────────────────────────────────────────────────────────

def lookup_company(ticker: str, cache_dir: Path, api_key: str) -> dict[str, Any]:
    """
    Resolve ticker → CIK, company name, sector, SIC, exchange via MappingApi.

    Returns:
        dict with keys: ticker, cik, name, sic, sector, exchange.
        Empty dict on failure.

    Cache:
        cache_dir/company_meta.json
    """
    cache_path = cache_dir / "company_meta.json"
    if cache_path.exists():
        log.debug("company_meta: cache hit %s", cache_path)
        return json.loads(cache_path.read_text())

    try:
        from sec_api import MappingApi  # noqa: PLC0415
        mapping = MappingApi(api_key=api_key)
        result = mapping.resolve("ticker", ticker)
    except Exception as exc:
        log.warning("MappingApi.resolve(%s) failed: %s", ticker, exc)
        return {}

    if not result:
        log.warning("MappingApi returned no results for ticker %s", ticker)
        return {}

    record = result[0] if isinstance(result, list) else result

    meta = {
        "ticker": ticker,
        "cik": str(record.get("cik", "")).lstrip("0") or str(record.get("cik", "")),
        "name": record.get("name", ""),
        "sic": str(record.get("sic", "")),
        "sector": record.get("sector", ""),
        "exchange": record.get("exchange", ""),
    }

    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(meta, indent=2))
    log.info("company_meta cached: %s → %s (CIK %s)", ticker, meta["name"], meta["cik"])
    return meta


# ── Filing discovery via EDGAR free API ──────────────────────────────────────

def find_filings(
    ticker: str,
    form_types: list[str],
    cache_dir: Path,
    api_key: str,
    cik: str = "",
    start_date: str = "2020-01-01",
    end_date: str | None = None,
) -> list[dict[str, Any]]:
    """
    Discover SEC filings using the free EDGAR submissions API
    (https://data.sec.gov/submissions/CIK{cik}.json).

    Args:
        ticker:     US ticker symbol (e.g. "ADSK") — used for logging only
        form_types: list of form types to include e.g. ["10-K", "10-Q"]
        cache_dir:  directory for caching the filings index JSON
        api_key:    sec-api.io API key (used by lookup_company if CIK not provided)
        cik:        SEC CIK number (without leading zeros). If empty, resolved via MappingApi.
        start_date: YYYY-MM-DD lower bound for filed date
        end_date:   YYYY-MM-DD upper bound (default: today)

    Returns:
        list of dicts: {form_type, filed_date, filing_url, accession_no, period_of_report}
        Sorted by filed_date descending. Truncated to _FETCH_LIMITS per form type.

    Cache:
        cache_dir/filings_index.json
    """
    cache_path = cache_dir / "filings_index.json"
    if cache_path.exists():
        log.debug("filings_index: cache hit %s", cache_path)
        return json.loads(cache_path.read_text())

    if end_date is None:
        from datetime import date as _date
        end_date = _date.today().isoformat()

    # Resolve CIK if not provided
    if not cik:
        meta = lookup_company(ticker, cache_dir, api_key)
        cik = meta.get("cik", "")

    if not cik:
        log.warning("find_filings: could not resolve CIK for %s", ticker)
        return []

    # Zero-pad CIK to 10 digits for EDGAR URL
    cik_padded = cik.zfill(10)

    try:
        url = _EDGAR_SUBMISSIONS_URL.format(cik=cik_padded)
        resp = requests.get(url, headers=_EDGAR_HEADERS, timeout=15)
        resp.raise_for_status()
        submissions = resp.json()
    except Exception as exc:
        log.warning("EDGAR submissions fetch failed for CIK %s: %s", cik, exc)
        return []

    company_cik = cik  # use the bare CIK for archive URLs
    recent = submissions.get("filings", {}).get("recent", {})

    forms: list[str] = recent.get("form", [])
    dates: list[str] = recent.get("filingDate", [])
    accessions: list[str] = recent.get("accessionNumber", [])
    primary_docs: list[str] = recent.get("primaryDocument", [])
    periods: list[str] = recent.get("reportDate", [])

    target_forms = set(form_types)
    counts: dict[str, int] = {ft: 0 for ft in form_types}
    all_filings: list[dict[str, Any]] = []

    for i, form in enumerate(forms):
        if form not in target_forms:
            continue
        if dates[i] < start_date or dates[i] > end_date:
            continue
        if counts.get(form, 0) >= _FETCH_LIMITS.get(form, 5):
            continue

        acc = accessions[i]                         # e.g. "0000769397-26-000015"
        doc = primary_docs[i]                       # e.g. "adsk-20260131.htm"
        acc_clean = acc.replace("-", "")            # "000076939726000015"
        filing_url = _EDGAR_ARCHIVES_URL.format(cik=company_cik, acc=acc_clean, doc=doc)

        all_filings.append(
            {
                "form_type": form,
                "filed_date": dates[i],
                "filing_url": filing_url,
                "accession_no": acc,
                "period_of_report": periods[i] if i < len(periods) else "",
                "primary_doc": doc,
            }
        )
        counts[form] = counts.get(form, 0) + 1

    # Sort latest first
    all_filings.sort(key=lambda x: x["filed_date"], reverse=True)

    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(all_filings, indent=2))
    log.info(
        "filings_index cached: %d filings for %s — %s",
        len(all_filings),
        ticker,
        {ft: counts.get(ft, 0) for ft in form_types},
    )
    return all_filings


# ── Filing download ───────────────────────────────────────────────────────────

def download_filing(
    url: str,
    local_path: Path,
    api_key: str,
    delay_seconds: float = 1.0,
) -> str:
    """
    Download a single SEC filing HTML to local_path via DownloadApi.
    Falls back to direct EDGAR HTTP if DownloadApi fails.
    Returns cached content if file already exists.

    Args:
        url:           Full https://www.sec.gov/Archives/... URL
        local_path:    Absolute path to write the cached file
        api_key:       sec-api.io API key
        delay_seconds: Polite pause before the download request

    Returns:
        File content as str. Empty string on failure.
    """
    if local_path.exists():
        log.debug("download_filing: cache hit %s", local_path.name)
        return local_path.read_text(errors="replace")

    if not url:
        log.warning("download_filing: empty URL for %s", local_path.name)
        return ""

    time.sleep(delay_seconds)
    text = ""

    # Try sec-api DownloadApi first (fast mirror)
    try:
        from sec_api import DownloadApi  # noqa: PLC0415
        download_api = DownloadApi(api_key=api_key)
        content = download_api.get_file(url)
        text = content.decode("utf-8", errors="replace") if isinstance(content, bytes) else str(content)
    except Exception as exc:
        log.debug("DownloadApi failed (%s), falling back to direct EDGAR", exc)

    # Fallback: direct EDGAR fetch
    if not text:
        try:
            resp = requests.get(url, headers=_EDGAR_HEADERS, timeout=30)
            resp.raise_for_status()
            text = resp.text
        except Exception as exc:
            log.warning("direct EDGAR fetch failed for %s: %s", local_path.name, exc)
            return ""

    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text(text, encoding="utf-8")
    log.info("download_filing: saved %s (%d chars)", local_path.name, len(text))
    return text


# ── High-level convenience: fetch standard filing set ────────────────────────

def fetch_standard_filings(
    ticker: str,
    cache_dir: Path,
    api_key: str,
    scrape_delay: float = 1.0,
) -> dict[str, Any]:
    """
    Orchestrate lookup + discovery + download for the standard filing set:
      - latest 10-K
      - last 4 10-Qs
      - last 12 8-Ks
      - latest DEF 14A

    Returns a summary dict:
    {
        "company_meta": {...},
        "filings": [...],           # full filings_index list
        "downloaded": {             # form_type → list of local paths
            "10-K": ["..."],
            "10-Q": ["...", "..."],
            ...
        }
    }
    """
    meta = lookup_company(ticker, cache_dir, api_key)
    cik = meta.get("cik", "")

    form_types = list(_FETCH_LIMITS.keys())
    filings = find_filings(ticker, form_types, cache_dir, api_key, cik=cik)

    downloaded: dict[str, list[str]] = {ft: [] for ft in form_types}
    counts: dict[str, int] = {ft: 0 for ft in form_types}

    for filing in filings:
        ft = filing["form_type"]
        limit = _FETCH_LIMITS.get(ft, 1)
        if counts.get(ft, 0) >= limit:
            continue

        filed_date = filing["filed_date"]
        safe_ft = ft.replace(" ", "")
        filename = f"{safe_ft}-{filed_date}.htm"
        local_path = cache_dir / filename

        download_filing(
            url=filing["filing_url"],
            local_path=local_path,
            api_key=api_key,
            delay_seconds=scrape_delay,
        )

        filing["local_path"] = str(local_path)
        downloaded[ft].append(str(local_path))
        counts[ft] = counts.get(ft, 0) + 1

    log.info(
        "fetch_standard_filings complete for %s: %s",
        ticker,
        {ft: len(paths) for ft, paths in downloaded.items()},
    )
    return {
        "company_meta": meta,
        "filings": filings,
        "downloaded": downloaded,
    }
