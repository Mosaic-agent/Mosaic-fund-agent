"""
src/utils/cache.py
──────────────────
Simple disk-based TTL cache for external API responses.

Stores JSON files under <output_dir>/.cache/<key>.json.
A cached entry is considered fresh when the file's mtime is within
the configured TTL window.

Design goals:
  • Zero dependencies beyond stdlib — no Redis, no diskcache.
  • Thread-safe enough for the sequential pipeline (atomic write via
    tmp file + rename is not needed here since we're single-process).
  • Transparent: every hit/miss/write is logged at DEBUG level so
    developers can trace cache behaviour with LOG_LEVEL=DEBUG.

Usage:
    from src.utils.cache import cache_get, cache_set, cache_clear

    data = cache_get("comex_XAU_XAG", ttl_seconds=3600)
    if data is None:
        data = fetch_from_api()
        cache_set("comex_XAU_XAG", data)
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── Cache directory ───────────────────────────────────────────────────────────

def _cache_dir() -> Path:
    """
    Return the cache directory path, creating it if necessary.
    Reads output_dir from settings to stay consistent with the rest of the app.
    """
    try:
        from config.settings import settings
        base = Path(settings.output_dir)
    except Exception:
        base = Path("./output")

    cache_path = base / ".cache"
    cache_path.mkdir(parents=True, exist_ok=True)
    return cache_path


def _cache_path(key: str) -> Path:
    """Return the JSON file path for a given cache key."""
    # Sanitise key: replace non-alphanumeric chars with underscores
    safe_key = "".join(c if c.isalnum() or c in "-_" else "_" for c in key)
    return _cache_dir() / f"{safe_key}.json"


# ── Public API ────────────────────────────────────────────────────────────────

def cache_get(key: str, ttl_seconds: int = 3600) -> Any | None:
    """
    Return the cached value for ``key`` if it exists and is not expired.

    Args:
        key:         Cache key string (will be sanitised for use as filename).
        ttl_seconds: Maximum age in seconds before the entry is considered stale.
                     Default 3600 (1 hour).

    Returns:
        The deserialised Python object, or ``None`` on miss / expiry / error.
    """
    path = _cache_path(key)

    if not path.exists():
        logger.debug("Cache MISS (no file): %s", key)
        return None

    age_seconds = time.time() - path.stat().st_mtime
    if age_seconds > ttl_seconds:
        logger.debug(
            "Cache MISS (expired %.0fs > TTL %ds): %s",
            age_seconds, ttl_seconds, key,
        )
        return None

    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        logger.debug(
            "Cache HIT (age %.0fs / TTL %ds): %s",
            age_seconds, ttl_seconds, key,
        )
        return data
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Cache read failed for %s: %s — treating as miss", key, exc)
        return None


def cache_set(key: str, value: Any) -> None:
    """
    Persist ``value`` to the cache under ``key``.

    The value must be JSON-serialisable. Non-serialisable objects are
    logged and silently skipped — callers always get a live result instead.

    Args:
        key:   Cache key string.
        value: JSON-serialisable Python object to cache.
    """
    path = _cache_path(key)
    try:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(value, fh, default=str)
        logger.debug("Cache WRITE: %s → %s", key, path.name)
    except (TypeError, OSError) as exc:
        logger.warning("Cache write failed for %s: %s — continuing without cache", key, exc)


def cache_age_seconds(key: str) -> float | None:
    """
    Return the age in seconds of the cached entry, or None if it doesn't exist.
    Useful for logging "serving cached data from X minutes ago".
    """
    path = _cache_path(key)
    if not path.exists():
        return None
    return time.time() - path.stat().st_mtime


def cache_clear(key: str) -> bool:
    """
    Delete the cache entry for ``key``.

    Returns True if deleted, False if it didn't exist.
    """
    path = _cache_path(key)
    if path.exists():
        path.unlink()
        logger.debug("Cache CLEAR: %s", key)
        return True
    return False


def cache_clear_all() -> int:
    """
    Delete all cache files in the cache directory.

    Returns the number of files deleted.
    """
    count = 0
    for f in _cache_dir().glob("*.json"):
        f.unlink()
        count += 1
    logger.info("Cache cleared: %d file(s) removed", count)
    return count
