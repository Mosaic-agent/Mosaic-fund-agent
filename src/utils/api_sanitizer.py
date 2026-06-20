"""
src/utils/api_sanitizer.py
───────────────────────────
Shared prompt-injection protection and field validation for external API
responses.

All functions return a safe value or ``None`` / ``"[SANITIZED]"`` when
validation fails, so that adversarially crafted API payloads cannot inject
instructions into the LLM pipeline or corrupt downstream numeric outputs.

Previously these four validators were private functions duplicated inside
``src/tools/comex_fetcher.py`` and inlined in ``src/tools/goldhub_intelligence.py``.

[SECURITY] This module is security-critical — do not relax the patterns.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# Patterns that indicate an adversarially crafted payload attempting prompt
# injection.  Checked against any free-text string field from external APIs.
_INJECTION_PATTERNS = re.compile(
    r"ignore\s+(previous|above|all)\s+instruction"
    r"|system\s*:"
    r"|you\s+are\s+(now|a|an)\s"
    r"|<\s*/?instructions?\s*>"
    r"|\bprompt\b.*\binjection\b"
    r"|\bact\s+as\b"
    r"|\bdisregard\b",
    re.IGNORECASE,
)


def safe_str(
    value: object,
    max_len: int = 50,
    field_name: str = "field",
) -> str:
    """
    Validate and sanitise a string field from an external API response.

    - Casts to str, strips whitespace.
    - Rejects values longer than *max_len* (returns ``"[SANITIZED]"``).
    - Rejects values matching injection patterns (returns ``"[SANITIZED]"``).
    - Strips ASCII control characters (0x00–0x1F, 0x7F) except tab/newline.

    Returns a safe string guaranteed not to contain prompt-injection payloads.
    """
    raw = str(value).strip()
    if len(raw) > max_len:
        logger.warning(
            "[SECURITY] %s field too long (%d chars) — sanitising", field_name, len(raw)
        )
        return "[SANITIZED]"
    if _INJECTION_PATTERNS.search(raw):
        logger.warning(
            "[SECURITY] Prompt-injection pattern detected in %s field — sanitising",
            field_name,
        )
        return "[SANITIZED]"
    # Remove ASCII control characters (0x00–0x08, 0x0b–0x1f, 0x7f) but keep tab/newline
    return re.sub(r"[\x00-\x08\x0b-\x1f\x7f]", "", raw)


def safe_price(value: object, field_name: str = "price") -> Optional[float]:
    """
    Validate a numeric price field from an external API response.

    Returns ``None`` and logs a warning for any non-positive or non-numeric value.
    """
    try:
        price = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        logger.warning("[SECURITY] Non-numeric %s value: %r — ignoring", field_name, value)
        return None
    if price <= 0:
        logger.warning("[SECURITY] Non-positive %s value: %r — ignoring", field_name, value)
        return None
    return round(price, 6)


def safe_symbol_from_whitelist(
    value: object,
    whitelist: set[str],
    field_name: str = "symbol",
) -> Optional[str]:
    """
    Validate that a symbol from an API response is in a known *whitelist*.

    This prevents the API from injecting unknown symbols into the pipeline.

    Args:
        value:     Raw symbol value from the API.
        whitelist: Set of accepted symbol strings (compared after upper-casing).
        field_name: Label used in warning log messages.

    Returns:
        The upper-cased symbol string, or ``None`` if not in *whitelist*.
    """
    raw = str(value).strip().upper()
    if raw not in whitelist:
        logger.warning("[SECURITY] Unknown %s from API: %r — ignoring", field_name, raw)
        return None
    return raw


def safe_timestamp(value: object) -> Optional[str]:
    """
    Validate an ISO-8601 UTC timestamp string.

    Parses the value and converts to an IST-formatted string
    (``YYYY-MM-DD HH:MM:SS IST``).  Returns ``None`` if unparseable.
    """
    try:
        dt_str = str(value).strip()
        if dt_str.endswith("Z"):
            dt_str = dt_str[:-1] + "+00:00"
        dt = datetime.fromisoformat(dt_str)
        from src.utils.ist import utc_to_ist
        return utc_to_ist(dt).strftime("%Y-%m-%d %H:%M:%S IST")
    except (TypeError, ValueError) as exc:
        logger.debug("[SECURITY] Invalid timestamp %r: %s", value, exc)
        return None
