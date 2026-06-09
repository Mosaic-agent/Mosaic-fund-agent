"""
src/utils/nim_pool.py
─────────────────────
NVIDIA NIM API connection pool.

Free-tier cap: ~40 RPM per key. Pool distributes load across N keys using
round-robin selection and per-key sliding-window rate limiting (38 RPM —
2 RPM headroom). 429 responses are retried with exponential back-off.

Configuration (.env):
    NVIDIA_API_KEY=nvapi-key1                   # primary (required)
    NVIDIA_API_KEY_2=nvapi-key2                 # optional additional key
    NVIDIA_API_KEY_3=nvapi-key3                 # optional additional key
    NIM_RPM_PER_KEY=38                          # rate limit per key (default 38)

Usage:
    from src.utils.nim_pool import NIMPool
    llm = NIMPool.get().acquire(model, extra_body, timeout, max_tokens)
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any

log = logging.getLogger(__name__)

_RPM_HEADROOM = 2          # stay 2 RPM below the hard cap
_DEFAULT_RPM  = 38         # safe default for free tier (cap ~40)
_RETRY_CODES  = {429, 503}
_MAX_RETRIES  = 4


class _KeySlot:
    """One API key + its rate limiter + stats."""

    def __init__(self, key: str, rpm: int) -> None:
        from langchain_core.rate_limiters import InMemoryRateLimiter
        self.key   = key
        self.rpm   = rpm
        self._limiter = InMemoryRateLimiter(
            requests_per_second=rpm / 60.0,
            check_every_n_seconds=0.05,
            max_bucket_size=rpm,        # allow short bursts up to full RPM
        )
        self.requests_sent  = 0
        self.errors_429     = 0
        self._lock          = threading.Lock()

    def build_llm(self, model: str, extra_body: dict, timeout: int, max_tokens: int) -> Any:
        from langchain_openai import ChatOpenAI
        # Use max_retries (built-in OpenAI client retry) instead of with_retry()
        # wrapper — with_retry() returns RunnableRetry which breaks bind_tools()
        # required by LangGraph's create_react_agent.
        return ChatOpenAI(
            model=model,
            base_url="https://integrate.api.nvidia.com/v1",
            api_key=self.key,
            temperature=0.6,        # NIM requires temperature > 0
            max_tokens=max_tokens,
            extra_body=extra_body,
            timeout=timeout,
            streaming=False,
            max_retries=_MAX_RETRIES,
            rate_limiter=self._limiter,
        )

    def record_request(self) -> None:
        with self._lock:
            self.requests_sent += 1

    def record_429(self) -> None:
        with self._lock:
            self.errors_429 += 1

    def stats(self) -> dict:
        return {"key": self.key[:16] + "...", "sent": self.requests_sent, "429s": self.errors_429, "rpm": self.rpm}


class NIMPool:
    """
    Round-robin pool of NVIDIA NIM LLM instances.

    Singleton — call NIMPool.get() everywhere; the pool is built once
    and reused across agent runs.
    """

    _instance: "NIMPool | None" = None
    _lock = threading.Lock()

    def __init__(self, slots: list[_KeySlot]) -> None:
        self._slots   = slots
        self._idx     = 0
        self._rr_lock = threading.Lock()

    @classmethod
    def get(cls) -> "NIMPool":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls._build()
        return cls._instance

    @classmethod
    def _build(cls) -> "NIMPool":
        import os
        from config.settings import settings

        rpm = int(os.getenv("NIM_RPM_PER_KEY", str(_DEFAULT_RPM)))

        # Collect all configured keys (primary + numbered extras)
        keys: list[str] = []
        primary = settings.nvidia_api_key
        if primary:
            keys.append(primary)
        for i in range(2, 10):
            k = os.getenv(f"NVIDIA_API_KEY_{i}", "")
            if k:
                keys.append(k)

        if not keys:
            raise RuntimeError(
                "NIMPool: no NVIDIA API keys found. "
                "Set NVIDIA_API_KEY in .env."
            )

        slots = [_KeySlot(k, rpm) for k in keys]
        log.info("NIMPool: %d key(s), %d RPM each, max throughput %d RPM",
                 len(slots), rpm, len(slots) * rpm)
        return cls(slots)

    def _next_slot(self) -> _KeySlot:
        with self._rr_lock:
            slot = self._slots[self._idx % len(self._slots)]
            self._idx += 1
        return slot

    def acquire(
        self,
        model: str,
        extra_body: dict | None = None,
        timeout: int = 120,
        max_tokens: int = 4096,
    ) -> Any:
        """
        Return a rate-limited, retry-wrapped ChatOpenAI for the next key in
        the round-robin rotation.
        """
        slot = self._next_slot()
        slot.record_request()
        log.debug("NIMPool: dispatching to key slot %d / %d", self._idx % len(self._slots), len(self._slots))
        return slot.build_llm(model, extra_body or {}, timeout, max_tokens)

    def stats(self) -> list[dict]:
        return [s.stats() for s in self._slots]

    @classmethod
    def reset(cls) -> None:
        """Force pool rebuild on next get() — useful after .env changes."""
        with cls._lock:
            cls._instance = None
