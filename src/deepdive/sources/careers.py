"""
src/deepdive/sources/careers.py
────────────────────────────────
Job-postings fetchers for US companies.

Each company uses a different ATS / careers platform, so adapters are
implemented per platform with a common interface.

Platforms implemented:
  WorkdayAdapter        — JSON POST API (tenant.wd1.myworkdayjobs.com)
  HtmlPaginatedAdapter  — HTML page scraping, ?page=N (BeautifulSoup)

Concrete company adapters:
  AutodeskAdapter  (ADSK) — WorkdayAdapter
  ProcoreAdapter   (PCOR) — HtmlPaginatedAdapter

All adapters produce normalized job dicts compatible with jobs_signal.bucket_jobs():
  {
    "title"        : str,   # job title
    "locationsText": str,   # location display string
    "department"   : str,   # optional department / function bucket hint
  }

Registry:
  get_adapter(ticker: str) -> BaseCareersAdapter | None

Cache-first: raw JSON written to cache_path on first call; reads from disk on re-run.
Uses settings.scrape_delay_seconds between page requests (polite crawling).
"""

from __future__ import annotations

import json
import logging
import re
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import requests

from config.settings import settings

log = logging.getLogger(__name__)

_REQUEST_TIMEOUT = 30


# ── Base interface ─────────────────────────────────────────────────────────────

class BaseCareersAdapter(ABC):
    """
    Common interface for all job-board adapters.

    Subclasses MUST implement fetch_all_jobs() returning a list of normalized
    job dicts: {"title": str, "locationsText": str, "department": str}.
    """

    @abstractmethod
    def fetch_all_jobs(self, cache_path: Path) -> list[dict[str, Any]]:
        """
        Fetch all open job postings, caching to cache_path on first call.

        Returns list of normalized dicts. Cache is raw JSON; re-running reads
        from disk without making network requests.
        """


# ── Workday (JSON POST API) ────────────────────────────────────────────────────

_WORKDAY_PAGE_SIZE = 20


class WorkdayAdapter(BaseCareersAdapter):
    """
    Fetches job postings from the public Workday JSON API.

    Subclass per tenant — set base_url, tenant, site as class attributes.

    Endpoint:
      POST https://{tenant}.wd1.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs
      Body: {"limit": 20, "offset": 0, "searchText": "", "locations": [], "appliedFacets": {}}
      Response: {"jobPostings": [...], "total": N}
    """

    base_url: str = ""
    tenant: str = ""
    site: str = ""

    @property
    def _jobs_url(self) -> str:
        return f"{self.base_url}/wday/cxs/{self.tenant}/{self.site}/jobs"

    def _fetch_page(self, offset: int) -> dict[str, Any]:
        resp = requests.post(
            self._jobs_url,
            json={
                "limit": _WORKDAY_PAGE_SIZE,
                "offset": offset,
                "searchText": "",
                "locations": [],
                "appliedFacets": {},
            },
            headers={"Content-Type": "application/json"},
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def fetch_all_jobs(self, cache_path: Path) -> list[dict[str, Any]]:
        """
        Paginate through all Workday job postings and cache to cache_path.

        Raises RuntimeError if the final row count doesn't match the reported
        total (indicates pagination was silently truncated).
        """
        if cache_path.exists():
            log.debug("careers: cache hit %s", cache_path.name)
            return json.loads(cache_path.read_text())

        all_jobs: list[dict[str, Any]] = []
        offset = 0
        total: int | None = None

        log.info("careers: starting Workday pagination for %s/%s", self.tenant, self.site)

        while True:
            try:
                page = self._fetch_page(offset)
            except Exception as exc:
                log.warning("careers: page fetch failed at offset %d: %s", offset, exc)
                break

            if total is None:
                total = page.get("total", 0)
                log.info("careers: total postings reported = %d", total)

            postings = page.get("jobPostings", [])
            all_jobs.extend(postings)
            log.debug("careers: fetched %d at offset %d (total so far: %d)",
                      len(postings), offset, len(all_jobs))

            offset += _WORKDAY_PAGE_SIZE
            if offset >= (total or 0):
                break

            time.sleep(settings.scrape_delay_seconds)

        # Integrity check — never silently return incomplete data
        if total is not None and len(all_jobs) != total:
            raise RuntimeError(
                f"Workday pagination integrity check failed for {self.tenant}/{self.site}: "
                f"expected {total} jobs, got {len(all_jobs)}. "
                "API may have truncated results — do not use partial data."
            )

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(all_jobs, indent=2))
        log.info("careers: cached %d postings → %s", len(all_jobs), cache_path.name)
        return all_jobs


# ── HTML paginated scraper (BeautifulSoup) ─────────────────────────────────────

class HtmlPaginatedAdapter(BaseCareersAdapter):
    """
    Scrapes a careers page that paginates via ?page=N query parameter.

    Subclasses must set `search_url` and implement `_parse_page(html)`.

    Output dicts are normalized to:
      {"title": str, "locationsText": str, "department": str}
    """

    search_url: str = ""      # e.g. "https://careers.example.com/jobs/search"
    per_page: int = 30        # items per page (used only to estimate page count)
    page_param: str = "page"  # query-param name for the page number

    _HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }

    def _get_page_html(self, page_num: int) -> str:
        url = f"{self.search_url}?{self.page_param}={page_num}"
        resp = requests.get(url, headers=self._HEADERS, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text

    @abstractmethod
    def _parse_page(self, html: str) -> tuple[list[dict[str, Any]], int]:
        """
        Parse one page of HTML.

        Returns:
            (jobs_on_this_page, total_job_count)
            total_job_count may be 0 if not found on this page (only needs to
            be accurate on the first page call).
        """

    def fetch_all_jobs(self, cache_path: Path) -> list[dict[str, Any]]:
        if cache_path.exists():
            log.debug("careers: cache hit %s", cache_path.name)
            return json.loads(cache_path.read_text())

        all_jobs: list[dict[str, Any]] = []
        page = 1
        total: int | None = None

        log.info("careers: starting HTML pagination for %s", self.search_url)

        while True:
            try:
                html = self._get_page_html(page)
            except Exception as exc:
                log.warning("careers: HTML page %d fetch failed: %s", page, exc)
                break

            jobs_on_page, page_total = self._parse_page(html)

            if total is None and page_total:
                total = page_total
                log.info("careers: total postings reported = %d", total)

            if not jobs_on_page:
                log.info("careers: no jobs on page %d — stopping", page)
                break

            all_jobs.extend(jobs_on_page)
            log.debug("careers: page %d fetched %d jobs (total so far: %d)",
                      page, len(jobs_on_page), len(all_jobs))

            if total is not None and len(all_jobs) >= total:
                break

            page += 1
            time.sleep(settings.scrape_delay_seconds)

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(all_jobs, indent=2))
        log.info("careers: cached %d postings → %s", len(all_jobs), cache_path.name)
        return all_jobs


# ── Concrete adapters ──────────────────────────────────────────────────────────

class AutodeskAdapter(WorkdayAdapter):
    """Workday adapter for Autodesk (ADSK)."""

    base_url = "https://autodesk.wd1.myworkdayjobs.com"
    tenant = "autodesk"
    site = "Ext"


class ProcoreAdapter(HtmlPaginatedAdapter):
    """
    Scrapes open job postings from https://careers.procore.com/jobs/search.

    The page is server-rendered HTML with ?page=N pagination (30 per page).
    Each job row is a <tr> inside a <table>, with cells:
      [0] job title   [1] location   [2] department
    A banner reading "Displaying X - Y of Z in total" gives the total count.
    """

    search_url = "https://careers.procore.com/jobs/search"
    per_page = 30

    _TOTAL_RE = re.compile(r"(?:of|total[:\s]+)\s*(\d[\d,]*)\s+(?:in total|jobs?)", re.IGNORECASE)

    def _parse_page(self, html: str) -> tuple[list[dict[str, Any]], int]:
        try:
            from bs4 import BeautifulSoup  # noqa: PLC0415
        except ImportError:
            log.error("careers: beautifulsoup4 not installed — cannot parse Procore HTML")
            return [], 0

        soup = BeautifulSoup(html, "lxml")
        jobs: list[dict[str, Any]] = []

        # ── Total count ────────────────────────────────────────────────────────
        total = 0
        m = self._TOTAL_RE.search(html)
        if m:
            total = int(m.group(1).replace(",", ""))

        # ── Job rows — strategy 1: <table> rows ───────────────────────────────
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                title = cells[0].get_text(" ", strip=True)
                title = re.sub(r"^Title[:\s]+", "", title, flags=re.IGNORECASE).strip()
                loc = cells[1].get_text(" ", strip=True)
                loc = re.sub(r"^Location[:\s]+", "", loc, flags=re.IGNORECASE).strip()
                dept = cells[2].get_text(" ", strip=True) if len(cells) > 2 else ""
                if title:
                    jobs.append({
                        "title": title,
                        "locationsText": loc,
                        "department": dept,
                    })

        # ── Strategy 2: job-card links (fallback if no table found) ───────────
        if not jobs:
            for link in soup.find_all("a", href=re.compile(r"/jobs/")):
                title = link.get_text(" ", strip=True)
                if not title:
                    continue
                # Location and department may be in a sibling/parent element
                parent = link.find_parent()
                loc = ""
                dept = ""
                if parent:
                    texts = [t.get_text(" ", strip=True)
                             for t in parent.find_all(string=False)
                             if t != link and t.get_text(strip=True)]
                    if texts:
                        loc = texts[0]
                    if len(texts) > 1:
                        dept = texts[1]
                jobs.append({
                    "title": title,
                    "locationsText": loc,
                    "department": dept,
                })

        return jobs, total


# ── Adapter registry ───────────────────────────────────────────────────────────

_REGISTRY: dict[str, type[BaseCareersAdapter]] = {
    "ADSK": AutodeskAdapter,
    "PCOR": ProcoreAdapter,
}


def get_adapter(ticker: str) -> BaseCareersAdapter | None:
    """
    Return an instantiated careers adapter for the given ticker, or None if
    no adapter is registered for that company.
    """
    cls = _REGISTRY.get(ticker.upper())
    return cls() if cls else None
