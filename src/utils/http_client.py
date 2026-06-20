"""
src/utils/http_client.py
─────────────────────────
Minimal HTTP helper with retry logic for JSON/text API calls.

Centralises timeout, retry, and error-logging patterns that were previously
inlined independently in comex_fetcher.py, goldhub_intelligence.py, and
quant_scorecard.py.

Usage
-----
    from src.utils.http_client import fetch_json, fetch_text

    data = fetch_json("https://api.example.com/data", params={"q": "gold"})
    if data is None:
        # request failed (already logged)
        ...

[SECURITY] Response bodies are returned as-is; callers must validate
           individual fields via ``src.utils.api_sanitizer`` before use.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT   = 15   # seconds
_DEFAULT_RETRIES   = 2
_RETRY_BACKOFF_SEC = 1.0

_DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; MosaicBot/1.0; "
        "+https://github.com/mosaic-agent)"
    ),
    "Accept": "application/json",
}


def fetch_json(
    url: str,
    *,
    params: Optional[dict[str, Any]] = None,
    headers: Optional[dict[str, str]] = None,
    timeout: int = _DEFAULT_TIMEOUT,
    retries: int = _DEFAULT_RETRIES,
) -> Optional[dict | list]:
    """
    Fetch a URL and parse the response body as JSON.

    Args:
        url:     Target URL.
        params:  Optional query-string parameters.
        headers: Optional extra HTTP headers (merged with defaults).
        timeout: Per-attempt timeout in seconds.
        retries: Number of additional retry attempts on failure.

    Returns:
        Parsed JSON (``dict`` or ``list``), or ``None`` on error.
    """
    merged_headers = {**_DEFAULT_HEADERS, **(headers or {})}
    last_exc: Optional[Exception] = None

    for attempt in range(retries + 1):
        try:
            resp = requests.get(
                url,
                params=params,
                headers=merged_headers,
                timeout=timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                logger.debug(
                    "fetch_json: attempt %d/%d failed for %s — %s; retrying in %.1fs",
                    attempt + 1, retries + 1, url, exc, _RETRY_BACKOFF_SEC,
                )
                time.sleep(_RETRY_BACKOFF_SEC)

    logger.warning("fetch_json: all %d attempts failed for %s — %s", retries + 1, url, last_exc)
    return None


def fetch_text(
    url: str,
    *,
    params: Optional[dict[str, Any]] = None,
    headers: Optional[dict[str, str]] = None,
    timeout: int = _DEFAULT_TIMEOUT,
    retries: int = _DEFAULT_RETRIES,
) -> Optional[str]:
    """
    Fetch a URL and return the raw response text.

    Useful for CSV or HTML endpoints where JSON parsing is not appropriate.

    Returns:
        Response body as a string, or ``None`` on error.
    """
    merged_headers = {**_DEFAULT_HEADERS, **(headers or {})}
    last_exc: Optional[Exception] = None

    for attempt in range(retries + 1):
        try:
            resp = requests.get(
                url,
                params=params,
                headers=merged_headers,
                timeout=timeout,
            )
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                logger.debug(
                    "fetch_text: attempt %d/%d failed for %s — %s; retrying in %.1fs",
                    attempt + 1, retries + 1, url, exc, _RETRY_BACKOFF_SEC,
                )
                time.sleep(_RETRY_BACKOFF_SEC)

    logger.warning("fetch_text: all %d attempts failed for %s — %s", retries + 1, url, last_exc)
    return None
