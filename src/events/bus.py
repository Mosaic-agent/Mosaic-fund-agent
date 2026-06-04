"""
src/events/bus.py
──────────────────
EventBus — Observer pattern for post-import hooks.

Architecture
────────────
  DataImportedEvent   dataclass carrying import context
  Observer (ABC)      declares which event types it handles
  EventBus            singleton dispatcher; fires observers sync or async

Usage
─────
  # Register an observer (done once at startup or in observers.py)
  get_event_bus().subscribe(MyObserver())

  # Fire an event (done by repo.run_fetcher after successful insert)
  get_event_bus().publish(DataImportedEvent(source="yfinance", category="etfs", ...))

  # Observer runs automatically — caller doesn't know or care which observers exist.

Async vs sync
─────────────
  Observers with async_ok=True run in a thread-pool daemon thread so imports
  don't block waiting for ML retraining or signal aggregation.

  Observers with async_ok=False (e.g. ModelCacheInvalidator) run inline —
  they're instant and must complete before the next import step.

Adding a new hook
─────────────────
  1. Subclass Observer
  2. Set event_types = ["data.imported"]
  3. Implement handle(event)
  4. Call get_event_bus().subscribe(MyObserver()) once at startup
"""
from __future__ import annotations

import logging
import threading
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date
from typing import Any

log = logging.getLogger(__name__)


# ── Event ─────────────────────────────────────────────────────────────────────

@dataclass
class DataImportedEvent:
    """
    Fired by MarketDataRepository.run_fetcher() after a successful insert.

    Observers use these fields to decide whether to react:
      source     : watermark source key   e.g. "yfinance", "nse_fii_dii"
      category   : data category          e.g. "etfs", "fii_dii", "cot"
      symbol_key : watermark symbol       e.g. "MARKET", "GOLDBEES", "etfs"
      n_rows     : rows written
      from_date  : earliest date in batch
      to_date    : latest date in batch (= new watermark)
    """
    event_type: str      = "data.imported"
    source:     str      = ""
    category:   str      = ""
    symbol_key: str      = ""
    n_rows:     int      = 0
    from_date:  date     = field(default_factory=date.today)
    to_date:    date     = field(default_factory=date.today)

    def matches(self, *categories: str) -> bool:
        return self.category in categories or self.symbol_key in categories


# ── Observer ABC ──────────────────────────────────────────────────────────────

class Observer(ABC):
    """
    Base class for all post-import hooks.

    Attributes
    ----------
    event_types : event type strings this observer subscribes to
    async_ok    : True  → run in background thread (default)
                  False → run inline, blocking the publisher
    """
    event_types: list[str] = ["data.imported"]
    async_ok:    bool      = True

    @abstractmethod
    def handle(self, event: DataImportedEvent) -> None:
        """
        React to the event.  Must not raise — log and swallow all exceptions.
        """

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(async={self.async_ok})"


def _make_daemon_thread() -> None:
    """Ensure ThreadPoolExecutor worker threads are daemon threads so python doesn't hang on exit."""
    try:
        threading.current_thread().daemon = True
    except RuntimeError:
        pass  # In Python 3.13+, daemon status cannot be set on active threads; ignore it.


# ── EventBus ──────────────────────────────────────────────────────────────────

class EventBus:
    """
    Central dispatcher.  Thread-safe singleton via get_event_bus().

    Async observers run in a shared daemon ThreadPoolExecutor so the
    import pipeline is never blocked by slow ML retraining.
    """

    def __init__(self, max_workers: int = 4) -> None:
        self._observers: dict[str, list[Observer]] = {}
        self._pool      = ThreadPoolExecutor(max_workers=max_workers,
                                             thread_name_prefix="event-worker",
                                             initializer=_make_daemon_thread)
        self._lock      = threading.Lock()

    def subscribe(self, observer: Observer) -> None:
        """Register observer for all its declared event_types."""
        with self._lock:
            for evt_type in observer.event_types:
                self._observers.setdefault(evt_type, []).append(observer)
        log.debug("EventBus: subscribed %s", observer)

    def unsubscribe(self, observer: Observer) -> None:
        with self._lock:
            for evt_type in observer.event_types:
                bucket = self._observers.get(evt_type, [])
                if observer in bucket:
                    bucket.remove(observer)

    def publish(self, event: DataImportedEvent) -> None:
        """
        Dispatch event to all matching observers.
        Async observers are submitted to the thread pool.
        Sync observers are called inline.
        """
        with self._lock:
            handlers = list(self._observers.get(event.event_type, []))

        if not handlers:
            return

        log.info("EventBus: %s → %d observer(s)", event.event_type, len(handlers))
        for obs in handlers:
            if obs.async_ok:
                self._pool.submit(self._safe_call, obs, event)
            else:
                self._safe_call(obs, event)

    @staticmethod
    def _safe_call(obs: Observer, event: DataImportedEvent) -> None:
        try:
            obs.handle(event)
        except Exception as exc:
            log.error("EventBus: %s raised %s — suppressed", obs, exc, exc_info=True)

    def observer_count(self, event_type: str = "data.imported") -> int:
        return len(self._observers.get(event_type, []))

    def shutdown(self, wait: bool = True) -> None:
        self._pool.shutdown(wait=wait)


# ── Singleton ─────────────────────────────────────────────────────────────────

_BUS: EventBus | None = None
_BUS_LOCK = threading.Lock()


def get_event_bus() -> EventBus:
    """Return the process-wide EventBus singleton."""
    global _BUS
    if _BUS is None:
        with _BUS_LOCK:
            if _BUS is None:
                _BUS = EventBus()
    return _BUS
