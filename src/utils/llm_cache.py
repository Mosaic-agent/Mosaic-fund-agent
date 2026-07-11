"""
src/utils/llm_cache.py
──────────────────────
SQLite-backed LLM response cache for LangChain.

Implements the ``BaseCache`` interface from ``langchain_core``, storing
prompt→response pairs in a local SQLite file so identical LLM calls are
served from disk instead of hitting the API.

Usage
-----
    from src.utils.llm_cache import setup_llm_cache
    setup_llm_cache()          # call once at startup; reads config from settings

The cache is keyed on ``hash(prompt + llm_string)`` — the same key scheme
used by ``langchain_community.cache.SQLiteCache``, making it a drop-in
replacement without the community package dependency.

Cache file: ``output/.cache/llm_cache.db``
TTL:        ``LLM_CACHE_TTL_HOURS`` env var (default 24 h; 0 = no expiry)
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any

from langchain_core.caches import BaseCache
from langchain_core.outputs import Generation, ChatGeneration

logger = logging.getLogger(__name__)

_DB_PATH = Path("output/.cache/llm_cache.db")

# ── Serialisation helpers ──────────────────────────────────────────────────────

def _serialise(generations: list[Generation]) -> str:
    """Serialise a list of Generation objects to a JSON string."""
    rows = []
    for g in generations:
        if isinstance(g, ChatGeneration):
            tool_calls = getattr(g.message, "tool_calls", [])
            rows.append({
                "type": "chat",
                "text": g.text,
                "message_type": g.message.__class__.__name__,
                "message_content": g.message.content,
                "tool_calls": tool_calls,
            })
        else:
            rows.append({"type": "generation", "text": g.text})
    return json.dumps(rows)


def _deserialise(data: str) -> list[Generation]:
    """Deserialise JSON string back to a list of Generation objects."""
    from langchain_core.messages import AIMessage
    rows = json.loads(data)
    result: list[Generation] = []
    for r in rows:
        if r.get("type") == "chat":
            result.append(ChatGeneration(
                text=r["text"],
                message=AIMessage(
                    content=r["message_content"],
                    tool_calls=r.get("tool_calls", []),
                ),
            ))
        else:
            result.append(Generation(text=r["text"]))
    return result


# ── SQLite cache implementation ────────────────────────────────────────────────

class SQLiteLLMCache(BaseCache):
    """
    Persistent LLM response cache backed by a local SQLite database.

    Parameters
    ----------
    db_path:
        Path to the SQLite file.  Created automatically if it doesn't exist.
    ttl_seconds:
        Time-to-live in seconds.  Entries older than this are ignored on lookup.
        Use 0 for no expiry (entries live forever).
    """

    def __init__(self, db_path: Path = _DB_PATH, ttl_seconds: int = 86400) -> None:
        self._db_path = Path(db_path)
        self._ttl = ttl_seconds
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path))
        conn.execute("PRAGMA journal_mode=WAL")   # safe for concurrent reads
        return conn

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS llm_cache (
                    key      TEXT PRIMARY KEY,
                    value    TEXT NOT NULL,
                    created  REAL NOT NULL
                )
            """)

    @staticmethod
    def _key(prompt: str, llm_string: str) -> str:
        return hashlib.sha256(f"{prompt}\n{llm_string}".encode()).hexdigest()

    def lookup(self, prompt: str, llm_string: str) -> list[Generation] | None:
        key = self._key(prompt, llm_string)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT value, created FROM llm_cache WHERE key = ?", (key,)
            ).fetchone()
        if row is None:
            return None
        value, created = row
        if self._ttl > 0 and (time.time() - created) > self._ttl:
            # Expired — delete stale entry
            with self._conn() as conn:
                conn.execute("DELETE FROM llm_cache WHERE key = ?", (key,))
            return None
        try:
            return _deserialise(value)
        except Exception as exc:
            logger.warning("llm_cache: deserialise failed: %s", exc)
            return None

    def update(self, prompt: str, llm_string: str, return_val: list[Generation]) -> None:
        key = self._key(prompt, llm_string)
        try:
            value = _serialise(return_val)
        except Exception as exc:
            logger.warning("llm_cache: serialise failed, skipping cache write: %s", exc)
            return
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO llm_cache (key, value, created) VALUES (?, ?, ?)",
                (key, value, time.time()),
            )

    def clear(self, **kwargs: Any) -> None:
        with self._conn() as conn:
            conn.execute("DELETE FROM llm_cache")
        logger.info("llm_cache: cleared all entries")

    def stats(self) -> dict[str, Any]:
        """Return cache hit/miss/size statistics."""
        with self._conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM llm_cache").fetchone()[0]
            if self._ttl > 0:
                cutoff = time.time() - self._ttl
                live = conn.execute(
                    "SELECT COUNT(*) FROM llm_cache WHERE created > ?", (cutoff,)
                ).fetchone()[0]
            else:
                live = total
        size_kb = self._db_path.stat().st_size // 1024 if self._db_path.exists() else 0
        return {"total_entries": total, "live_entries": live, "db_size_kb": size_kb}


# ── Public setup helper ────────────────────────────────────────────────────────

_cache_instance: SQLiteLLMCache | None = None


def setup_llm_cache() -> SQLiteLLMCache | None:
    """
    Install a SQLite LLM cache globally for all LangChain LLM calls.

    Reads ``LLM_CACHE_ENABLED`` and ``LLM_CACHE_TTL_HOURS`` from settings.
    Returns the cache instance (or None if disabled).
    """
    global _cache_instance
    from config.settings import settings
    if not settings.llm_cache_enabled:
        logger.info("llm_cache: disabled (LLM_CACHE_ENABLED=false)")
        return None

    ttl_seconds = settings.llm_cache_ttl_hours * 3600

    from langchain_core.globals import set_llm_cache
    _cache_instance = SQLiteLLMCache(ttl_seconds=ttl_seconds)
    set_llm_cache(_cache_instance)

    s = _cache_instance.stats()
    logger.info(
        "llm_cache: enabled  db=%s  ttl=%dh  live=%d entries  size=%dkB",
        _DB_PATH, settings.llm_cache_ttl_hours, s["live_entries"], s["db_size_kb"],
    )
    return _cache_instance


def get_cache() -> SQLiteLLMCache | None:
    """Return the active cache instance, or None if not yet set up."""
    return _cache_instance
