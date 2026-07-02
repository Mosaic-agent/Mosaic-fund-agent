"""
src/workflows/base.py
─────────────────────
Shared utilities for all StateGraph workflows:
  - _get_llm()   : reuse MosaicFundAgent LLM construction (prefer cloud → local)
  - _par()       : parallel execution via ThreadPoolExecutor
  - SYNTH_SUFFIX : injected into every synthesis prompt
"""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

logger = logging.getLogger(__name__)

# ── LLM factory ───────────────────────────────────────────────────────────────

def _get_llm(prefer_cloud: bool = True) -> Any:
    """
    Return a configured LLM, reusing MosaicFundAgent's build logic.

    Preference order (prefer_cloud=True): cloud LLM → local LLM → None
    Preference order (prefer_cloud=False): local LLM → cloud LLM → None
    """
    from src.agents.mosaic_fund_agent import MosaicFundAgent
    tmp = object.__new__(MosaicFundAgent)
    tmp._checkpointer = None
    if prefer_cloud:
        llm = tmp._build_cloud_llm()
        if llm is not None:
            return llm
        return tmp._build_llm()
    else:
        llm = tmp._build_llm()
        if llm is not None:
            return llm
        return tmp._build_cloud_llm()


# ── Parallel executor ─────────────────────────────────────────────────────────

# Concurrency cap for parallel fetch. Kept low (6) because several fetchers hit
# rate-limited external scrapers (Yahoo Finance, Screener.in, GNews); a 12-way
# burst intermittently trips their throttles and returns empty data. 6 keeps the
# fan-out fast while staying under the burst threshold.
_PAR_MAX_WORKERS = 6


def _par(
    fetchers: dict[str, Any],
    max_workers: int | None = None,
    retries: int = 2,
    backoff: float = 1.5,
) -> dict[str, str]:
    """
    Execute a dict of {key: callable} concurrently via ThreadPoolExecutor.

    Each fetcher is retried up to ``retries`` times with linear backoff on an
    exception (transient network / rate-limit blips), so a single throttle
    doesn't drop a whole section. Returns {key: result_str}; a fetcher that
    still fails yields a '*key unavailable: ...*' placeholder so downstream
    synthesis always receives a complete dict.

    Note: this retries on *raised* errors. A scraper that returns empty/zero
    data instead of raising (e.g. yfinance under throttle) can't be detected
    here — the low concurrency cap is the primary mitigation for that.
    """
    if not fetchers:
        return {}
    n = max_workers or min(len(fetchers), _PAR_MAX_WORKERS)

    def _run(fn: Any, key: str) -> str:
        last: Exception | None = None
        for attempt in range(retries):
            try:
                return fn() or ""
            except Exception as exc:  # noqa: BLE001 — fetchers are plug-ins
                last = exc
                if attempt < retries - 1:
                    time.sleep(backoff * (attempt + 1))
        logger.warning("_par: %s failed after %d attempt(s): %s", key, retries, last)
        return f"*{key} unavailable: {last}*"

    results: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=n) as pool:
        futures = {pool.submit(_run, fn, key): key for key, fn in fetchers.items()}
        for f in as_completed(futures):
            key = futures[f]
            try:
                results[key] = f.result()
            except Exception as exc:  # safety — _run should never raise
                results[key] = f"*{key} unavailable: {exc}*"
    return results


# ── Shared prompt suffix ──────────────────────────────────────────────────────

SYNTH_SUFFIX = (
    "\n\nNUMERIC COMPUTATION RULE (mandatory): "
    "NEVER compute, estimate, or derive any number inside your response. "
    "Only narrate numbers that appear verbatim in the tool data above. "
    "If a number is not in the data, state that it is unavailable."
)

# ── Checkpointing ─────────────────────────────────────────────────────────────

_checkpointer = None


def _get_checkpointer():
    """
    Return a module-level SqliteSaver checkpointer (same DB as chat sessions).

    Lazily initialised; the sqlite3 connection is kept open for the process
    lifetime — no context manager needed. Returns None if unavailable.
    """
    global _checkpointer
    if _checkpointer is not None:
        return _checkpointer
    try:
        import sqlite3
        import os
        from langgraph.checkpoint.sqlite import SqliteSaver
        db_path = os.path.join(os.getenv("OUTPUT_DIR", "output"), "checkpoints.db")
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        conn = sqlite3.connect(db_path, check_same_thread=False)
        _checkpointer = SqliteSaver(conn)
        logger.info("workflow checkpointer: %s", db_path)
    except Exception as exc:
        logger.warning("workflow checkpointer unavailable (resumption disabled): %s", exc)
        _checkpointer = None
    return _checkpointer


def _thread_id(workflow: str, key: str) -> str:
    """Deterministic thread ID: same workflow + key + calendar date = resumable."""
    import hashlib
    from datetime import date
    return hashlib.sha256(f"{workflow}:{key}:{date.today()}".encode()).hexdigest()[:16]
