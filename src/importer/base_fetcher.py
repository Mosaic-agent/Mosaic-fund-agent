"""
src/importer/base_fetcher.py
─────────────────────────────
Fetcher ABC — Adapter pattern for data ingestion.

Every external data source implements this interface. The orchestrator
(MarketDataRepository.run_fetcher) handles watermarks, connection
lifecycle, dry-run, and summary logging — the fetcher only knows how
to fetch and where to write.

Implementing a new source
─────────────────────────
    class MyFetcher(Fetcher):
        source_name  = "my_source"    # watermark key
        symbol_key   = "MY_SYMBOL"    # watermark symbol

        def fetch(self, from_date, to_date) -> list[dict]:
            ...

        def insert(self, rows, ch) -> int:
            return ch.insert_my_table(rows)

    # Register in FETCHER_REGISTRY:
    FETCHER_REGISTRY["my_category"] = MyFetcher()

That's it. The orchestrator loop picks it up automatically.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import date
from typing import Any

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    _TENACITY_AVAILABLE = True
except ImportError:
    _TENACITY_AVAILABLE = False

log = logging.getLogger(__name__)


def _with_retry(fn, *args, **kwargs):
    """
    Call fn(*args, **kwargs) with exponential backoff retries if tenacity is
    available.  Falls back to a single call when tenacity is not installed.

    Retries on any Exception up to 5 attempts:
        attempt 1 — immediate
        attempt 2 — 1 s wait
        attempt 3 — 2 s wait
        attempt 4 — 4 s wait
        attempt 5 — 8 s wait (capped at 30 s)
    """
    if not _TENACITY_AVAILABLE:
        return fn(*args, **kwargs)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    def _wrapped():
        return fn(*args, **kwargs)

    return _wrapped()


class Fetcher(ABC):
    """
    Adapter interface between an external data source and ClickHouse.

    Attributes
    ----------
    source_name   : watermark source key  (e.g. "yfinance", "nse_fii_dii")
    symbol_key    : watermark symbol      (e.g. "MARKET", or per-symbol override)
    description   : human-readable label  (shown in CLI progress output)
    overlap_days  : re-fetch window to catch late corrections  (default 3)
    supports_parallel        : opt-in — orchestrator may run fetch() per symbol
                                across a thread pool (see for_symbol())
    supports_source_override : opt-in — fetch() honors the `source` kwarg
    per_symbol_watermark     : opt-in — each symbol resolves its OWN watermark
                                independently (e.g. stocks today), rather than
                                one worst-case watermark shared across the
                                whole symbol group (e.g. etfs today). Only
                                meaningful when supports_parallel is True.
    dataset                  : watermark dataset bucket (default "prices") —
                                override when a source's watermark isn't a
                                price series (e.g. amfi_flows uses "flows").
    first_run_lookback_days  : optional override of the caller's lookback_days,
                                used only when no watermark exists yet. None
                                (default) means "use whatever lookback_days
                                the caller passed" — zero behavior change.
    """

    source_name:  str
    symbol_key:   str
    description:  str = ""
    overlap_days: int = 3
    supports_parallel:        bool = False
    supports_source_override: bool = False
    per_symbol_watermark:     bool = False
    dataset:                  str = "prices"
    first_run_lookback_days:  int | None = None

    @abstractmethod
    def fetch(self, from_date: date, to_date: date, *, source: str | None = None) -> list[dict[str, Any]]:
        """
        Pull rows from the external source.

        Parameters
        ----------
        from_date : inclusive start date
        to_date   : inclusive end date
        source    : override the fetcher's default source, if
                    supports_source_override is True. Ignored otherwise.

        Returns empty list if source is unavailable — never raises.
        """

    def fetch_with_retry(self, from_date: date, to_date: date, **kwargs) -> list[dict[str, Any]]:
        """
        Call fetch() with exponential-backoff retry.

        On final failure, logs the error and returns an empty list so the
        orchestrator can route to the dead-letter import_failures table
        instead of crashing the batch.
        """
        try:
            return _with_retry(self.fetch, from_date, to_date, **kwargs)
        except Exception as exc:
            log.error(
                "%s fetch failed after retries (%s→%s): %s",
                self.__class__.__name__, from_date, to_date, exc,
            )
            return []

    def for_symbol(self, sym: str, ticker: str) -> "Fetcher":
        """
        Return a fetcher instance scoped to a single (sym, ticker) pair, for
        parallel execution by the orchestrator (one thread per symbol).

        Default assumes __init__(self, category, symbols) — override if a
        supports_parallel subclass has a different constructor shape.
        """
        return type(self)(self.category, [(sym, ticker)])

    def write_group_watermarks(
        self, ch, rows: list[dict[str, Any]], dry_run: bool, *, source: str | None = None,
    ) -> None:
        """
        Per-symbol watermark write for supports_parallel fetchers, used
        instead of the single symbol_key watermark when the orchestrator
        resolved a per-symbol (group) watermark for this fetch.

        `source` is the runtime source override actually used for this run
        (if supports_source_override) — the watermark must be keyed on the
        source that really produced the rows, not the fetcher's static
        default, or a source-override run silently fragments watermark
        history from the default-source run.

        Default: one dataset of rows, one watermark per symbol at that
        symbol's max_date(). Override when fetch() emits more than one row
        dataset in a single batch (see StocksFetcher).
        """
        if dry_run or not rows:
            return
        effective_source = source or self.source_name
        symbols_seen = {r["symbol"] for r in rows}
        for sym in symbols_seen:
            sym_rows = [r for r in rows if r["symbol"] == sym]
            ch.set_watermark(effective_source, sym, self.max_date(sym_rows))

    @abstractmethod
    def insert(self, rows: list[dict[str, Any]], ch) -> int:
        """
        Write rows to ClickHouse via the provided ClickHouseImporter.

        Returns the number of rows written.
        """

    def validate(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Optional: filter/clean rows before insert.
        Default implementation passes through unchanged.
        """
        return rows

    def max_date(self, rows: list[dict[str, Any]]) -> date:
        """
        Extract the latest date from rows for watermark update.
        Override if the date field has a non-standard name.
        """
        for key in ("trade_date", "report_date", "nav_date", "as_of_month", "snapshot_at"):
            dates = [r[key] for r in rows if r.get(key) is not None]
            if dates:
                latest = max(dates)
                return latest if isinstance(latest, date) else latest.date()
        raise ValueError(f"{self.__class__.__name__}: cannot determine max date from rows")

    def count_insertable(self, rows: list[dict[str, Any]]) -> int:
        """
        Row count to report for a dry-run (or any pre-insert preview) —
        must match what insert() would actually report on a real run.

        Default: one homogeneous row dataset, so len(rows) is correct.
        Override when fetch() emits more than one row dataset in a single
        batch (see StocksFetcher, whose insert() only counts price rows).
        """
        return len(rows)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(source={self.source_name!r}, symbol={self.symbol_key!r})"
