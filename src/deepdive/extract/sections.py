"""
src/deepdive/extract/sections.py
──────────────────────────────────
10-K section extraction via sec-api.io ExtractorApi.

ExtractorApi.get_section(url, item, type) returns the clean text or HTML of a
named 10-K/10-Q section — no BeautifulSoup or lxml needed.

Items used:
  "1"  → Business section (text)  — contains competition, product descriptions
  "7"  → MD&A (html)              — contains segment revenue tables

Segment table parsing (ADSK-specific):
  From the MD&A HTML, find <table> elements whose header row contains
  "AECO", "Manufacturing", "M&E", or "Segment" keywords.
  Extract revenue rows as {segment_name: revenue_usd_m}.

Cache-first: raw text/HTML written to cache_path on first call.
"""

from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path

log = logging.getLogger(__name__)

# Autodesk product-family segment names as they appear in the MD&A table rows
_PRODUCT_FAMILY_ROWS = {
    "AECO",
    "AutoCAD and AutoCAD LT",
    "MFG",
    "M&E",
    "Other",
}


# ── Section extraction ────────────────────────────────────────────────────────

def get_segment_section(
    filing_url: str,
    cache_path: Path,
    api_key: str,
) -> str:
    """
    Fetch MD&A (Item 7) HTML from a 10-K via ExtractorApi.

    Returns:
        Raw HTML string of the MD&A section. Empty string on failure.

    Cache:
        cache_path (e.g. cache/ADSK/date/section7_mda.html)
    """
    if cache_path.exists():
        log.debug("sections: cache hit %s", cache_path.name)
        return cache_path.read_text(errors="replace")

    if not filing_url:
        return ""

    try:
        from sec_api import ExtractorApi  # noqa: PLC0415
        extractor = ExtractorApi(api_key=api_key)
        html = extractor.get_section(filing_url, "7", "html")
    except Exception as exc:
        log.warning("ExtractorApi section 7 failed (%s): %s", filing_url[:60], exc)
        return ""

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(html, encoding="utf-8")
    log.info("sections: section 7 cached (%d chars) → %s", len(html), cache_path.name)
    return html


def get_business_section(
    filing_url: str,
    cache_path: Path,
    api_key: str,
) -> str:
    """
    Fetch Business section (Item 1) text from a 10-K via ExtractorApi.

    Returns:
        Plain text of the Business section. Empty string on failure.

    Cache:
        cache_path (e.g. cache/ADSK/date/section1_business.txt)
    """
    if cache_path.exists():
        log.debug("sections: cache hit %s", cache_path.name)
        return cache_path.read_text(errors="replace")

    if not filing_url:
        return ""

    try:
        from sec_api import ExtractorApi  # noqa: PLC0415
        extractor = ExtractorApi(api_key=api_key)
        text = extractor.get_section(filing_url, "1", "text")
    except Exception as exc:
        log.warning("ExtractorApi section 1 failed (%s): %s", filing_url[:60], exc)
        return ""

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(text, encoding="utf-8")
    log.info("sections: section 1 cached (%d chars) → %s", len(text), cache_path.name)
    return text


# ── Segment table parsing ────────────────────────────────────────────────────

def parse_segment_table(mda_html: str) -> list[dict]:
    """
    Parse the ADSK segment revenue table from the MD&A HTML.

    Looks for tables whose header row contains segment keywords.
    Returns a list of dicts:
        [{"name": "AECO", "revenue_usd_m": 2847.0}, ...]

    Returns empty list if no segment table is found.
    """
    if not mda_html:
        return []

    try:
        from bs4 import BeautifulSoup  # noqa: PLC0415
    except ImportError:
        log.warning("beautifulsoup4 not available — skipping segment table parse")
        return _parse_segment_table_regex(mda_html)

    soup = BeautifulSoup(mda_html, "html.parser")

    for table in soup.find_all("table"):
        # Only care about the "Net revenue by product family:" table
        full_text = table.get_text(" ", strip=True)
        if "Net revenue by product family" not in full_text:
            continue

        segments: list[dict] = []
        for row in table.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])]
            if not cells:
                continue

            name = cells[0]
            if name not in _PRODUCT_FAMILY_ROWS:
                continue

            # First non-empty numeric value after the name cell is current-year revenue
            # (AECO row has an extra "$" cell before the number — handle by stripping "$")
            rev_usd_m = None
            for cell in cells[1:]:
                raw = cell.replace(",", "").replace("$", "").strip()
                if not raw:
                    continue
                try:
                    val = float(raw)
                    if val > 1:  # values are already in USD millions
                        rev_usd_m = round(val, 1)
                        break
                except ValueError:
                    continue

            if rev_usd_m is not None:
                segments.append({"name": name, "revenue_usd_m": rev_usd_m})

        if segments:
            log.info("sections: found %d product-family segments", len(segments))
            return segments

    log.warning("sections: no segment table found in MD&A HTML")
    return []


def _parse_segment_table_regex(mda_html: str) -> list[dict]:
    """
    Fallback regex-based segment revenue extraction for when BS4 is unavailable.
    Looks for lines like "AECO  $2,847" near the word "Segment".
    """
    segments = []
    segment_pattern = re.compile(
        r"(AECO|Manufacturing|Media\s+and\s+Entertainment|M&amp;E)\s*[<\s$]*"
        r"([\d,]+)",
        re.IGNORECASE,
    )
    for m in segment_pattern.finditer(mda_html):
        name = m.group(1).replace("&amp;", "&").strip()
        try:
            rev = float(m.group(2).replace(",", ""))
            segments.append({"name": name, "revenue_usd_m": round(rev, 1)})
        except ValueError:
            pass
    return segments


# ── Competition text extraction ───────────────────────────────────────────────

def extract_competition_text(business_text: str) -> str:
    """
    Extract the competition paragraph(s) from the Business section text.

    Returns the first ~2000 chars starting from the COMPETITION heading,
    or the full text if not found.
    """
    if not business_text:
        return ""

    # Look for section heading
    match = re.search(r"COMPETITION", business_text, re.IGNORECASE)
    if match:
        start = match.start()
        snippet = business_text[start:start + 2500].strip()
        return snippet

    return business_text[:2000]


def extract_headcount_from_text(business_text: str) -> dict:
    """
    Regex search for headcount disclosure in Item 1 text.

    Looks for patterns like:
      "approximately 13,850 employees"
      "13,850 full-time employees"
      "approximately 14,000 people"

    Returns:
        {"total_headcount": 13850, "notes": "approximately 13,850 employees"} or {}
    """
    patterns = [
        r"approximately\s+([\d,]+)\s+(?:full[- ]time\s+)?(?:employees|people|workers)",
        r"([\d,]+)\s+full[- ]time\s+employees",
        r"workforce of\s+([\d,]+)",
    ]
    for pat in patterns:
        m = re.search(pat, business_text, re.IGNORECASE)
        if m:
            raw = m.group(1).replace(",", "")
            try:
                return {
                    "total_headcount": int(raw),
                    "notes": m.group(0).strip(),
                }
            except ValueError:
                pass
    return {}
