"""
src/deepdive/extract/jobs_signal.py
─────────────────────────────────────
Classify Workday job postings by function, geographic location, and seniority level.

Input:  list of raw jobPosting dicts from workday_jobs_raw.json
Output: nested dict {function: {location: count}}

Function buckets (keyword match on jobTitle, case-insensitive, first match wins):
  Engineering, Product, Sales, Marketing, Finance, HR, Operations, Legal, Other

Level keywords (detected separately, not bucketed into the main structure):
  Senior, Principal, Director, VP, Staff, Lead, Manager

Locations are derived from primaryLocation display string.
"""

from __future__ import annotations

import re

_FUNCTION_KEYWORDS: list[tuple[str, list[str]]] = [
    ("Engineering", ["engineer", "developer", "software", "architect", "devops", "sre",
                     "data scientist", "ml ", "machine learning", "infrastructure", "security"]),
    ("Product",     ["product manager", "product owner", "product design", "ux", "ui designer",
                     "user experience", "design"]),
    ("Sales",       ["sales", "account executive", "account manager", "business development",
                     "customer success", "solutions consultant", "pre-sales", "presales"]),
    ("Marketing",   ["marketing", "brand", "content", "seo", "demand gen", "communications",
                     "public relations", "events"]),
    ("Finance",     ["finance", "accounting", "controller", "fp&a", "financial analyst",
                     "treasury", "tax", "audit", "revenue operations"]),
    ("HR",          ["human resources", "recruiting", "recruiter", "talent acquisition",
                     "people operations", "hr ", "hris", "compensation", "benefits"]),
    ("Legal",       ["legal", "counsel", "attorney", "compliance", "privacy", "ip "]),
    ("Operations",  ["operations", "supply chain", "procurement", "it support", "facilities",
                     "program manager", "project manager", "scrum"]),
]

_LEVEL_KEYWORDS: list[str] = [
    "vp", "vice president", "director", "principal", "staff", "senior", "lead", "manager",
]


def _classify_function(title: str) -> str:
    t = title.lower()
    for bucket, keywords in _FUNCTION_KEYWORDS:
        if any(kw in t for kw in keywords):
            return bucket
    return "Other"


def _extract_location(posting: dict) -> str:
    """Return a concise location string from the posting dict."""
    # Workday postings typically have a 'locations' list or 'primaryLocation'
    loc = posting.get("locationsText") or posting.get("primaryLocation", "")
    if isinstance(loc, dict):
        loc = loc.get("$t") or loc.get("descriptor") or ""
    if not loc and "locations" in posting:
        locs = posting["locations"]
        if isinstance(locs, list) and locs:
            first = locs[0]
            loc = first.get("descriptor") or str(first)
    # Strip long country suffixes and truncate
    loc = str(loc).strip()
    # Simplify "City, State, Country" → "City, Country" if too long
    parts = [p.strip() for p in loc.split(",")]
    if len(parts) >= 3:
        loc = f"{parts[0]}, {parts[-1]}"
    return loc or "Remote / Unknown"


def bucket_jobs(job_postings: list[dict]) -> dict[str, dict[str, int]]:
    """
    Bucket job postings by {function: {location: count}}.

    Args:
        job_postings: list of raw posting dicts from workday API

    Returns:
        Nested dict e.g.:
        {
            "Engineering": {"San Francisco, US": 42, "Toronto, Canada": 18},
            "Sales":       {"Remote / Unknown": 15},
            ...
        }
    """
    result: dict[str, dict[str, int]] = {}

    for posting in job_postings:
        # Title can be in different keys depending on Workday tenant config
        title = (
            posting.get("title")
            or posting.get("jobTitle")
            or posting.get("externalJobTitle", {}).get("descriptor", "")
            or ""
        )
        if isinstance(title, dict):
            title = title.get("descriptor", "")

        func = _classify_function(str(title))
        loc = _extract_location(posting)

        result.setdefault(func, {})
        result[func][loc] = result[func].get(loc, 0) + 1

    # Sort each location bucket by count descending for readability
    return {
        func: dict(sorted(locs.items(), key=lambda x: -x[1]))
        for func, locs in sorted(result.items())
    }


def top_locations(jobs: dict[str, dict[str, int]], top_n: int = 5) -> dict[str, list[tuple[str, int]]]:
    """
    Return top N locations per function.
    Useful for the talent section prompt.
    """
    return {
        func: sorted(locs.items(), key=lambda x: -x[1])[:top_n]
        for func, locs in jobs.items()
    }
