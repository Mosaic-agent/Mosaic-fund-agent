"""
scripts/fund_imports/base.py
────────────────────────────
Abstract base class for all AMC fund importers.

Each subclass implements the five abstract methods; the shared
orchestration (ClickHouse connection, progress bar, request pacing,
row insert, watermark update) lives here in run().
"""

from __future__ import annotations

import logging
import os
import sys
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Any

import clickhouse_connect
import httpx
from rich.console import Console
from rich.progress import Progress

logger = logging.getLogger(__name__)

sys.path.append(os.getcwd())
from config.settings import settings

# ── Shared HTTP headers ───────────────────────────────────────────────────────

COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
}

# ── Unified asset classifier (union of ICICI + Nippon keyword sets) ───────────

_GOLD_KW = ("gold", "silver", "precious metal", "commodity etf", "precious")
_BOND_KW = (
    "bond", "debt", "fixed income", "debenture", "ncd", "government",
    "gilt", "treasury", "paper", "deposit", "g-sec", "sdl", "goi",
    "tbill", "commercial paper", "trep", "repo", "cblo",
    "certificate of deposit",
)
_CASH_KW = ("cash", "money market", "liquid", "overnight", "treps")
_EQUITY_KW = (
    "stock", "equity", "share", "common", "preferred",
    "nifty", "sensex", "cap", "etf", " ltd", " limited", "bank", "finance",
)


def classify_asset(name: str, industry: str = "") -> str:
    combined = f"{name} {industry}".lower()
    if any(k in combined for k in _GOLD_KW):
        return "gold"
    if any(k in combined for k in _BOND_KW):
        return "bond"
    if any(k in combined for k in _CASH_KW):
        return "cash"
    if any(k in combined for k in _EQUITY_KW):
        return "equity"
    return "other"


# ── ClickHouse helper ─────────────────────────────────────────────────────────

def _ch_client():
    """Return an unmanaged ClickHouse client from the pool singleton."""
    from src.db.pool import get_client
    return get_client()


# ── Base importer ─────────────────────────────────────────────────────────────

class BaseFundImporter(ABC):
    """
    Template-method base for fund portfolio importers.

    Subclasses implement the five abstract methods; run() drives the loop.
    """

    REQUEST_DELAY: float = 1.0  # seconds between HTTP requests; override per subclass
    # Holdings files are independent, while AMC hosts are often rate-limited.
    # Two in-flight sources materially improves historical backfills without
    # turning an import into an abusive burst.  Individual importers can lower
    # this for fragile endpoints; the environment value is capped at four.
    MAX_PARALLEL_SOURCES: int = 2

    def __init__(self, target_month: date | None = None, freshness_months: int = 0) -> None:
        self._console = Console()
        self._target_month = target_month
        self._freshness_months = freshness_months
        self._source_workers_override: int | None = None

    def set_source_workers(self, max_workers: int) -> None:
        """Set a validated per-instance override without mutating process config."""
        if not 1 <= max_workers <= 4:
            raise ValueError("max_workers must be between 1 and 4")
        self._source_workers_override = max_workers

    def source_workers(self) -> int:
        """Return the bounded fetch/parse concurrency for this AMC importer."""
        if self._source_workers_override is not None:
            return self._source_workers_override
        configured = os.getenv("AMC_IMPORT_MAX_WORKERS")
        try:
            workers = int(configured) if configured else self.MAX_PARALLEL_SOURCES
        except ValueError:
            logger.warning("Ignoring invalid AMC_IMPORT_MAX_WORKERS=%r", configured)
            workers = self.MAX_PARALLEL_SOURCES
        return max(1, min(workers, 4))

    def source_month(self, source: Any) -> date | None:
        """
        Extract the month (as first-of-month date) from a source item.
        Default handles string date at index 0, date object at index 0, etc.
        Subclasses with custom source structures can override this.
        """
        if isinstance(source, (list, tuple)) and len(source) >= 1:
            val = source[0]
            if isinstance(val, date):
                return val.replace(day=1)
            elif isinstance(val, str):
                try:
                    from datetime import datetime
                    return datetime.strptime(val, "%Y-%m-%d").date().replace(day=1)
                except ValueError:
                    pass
        return None

    def _apply_freshness(self, sources: list, client=None) -> list:
        """
        Keep only sources from the most recent N months.
        This forces re-import of those months regardless of watermark.
        """
        if not sources:
            return sources
        dated_sources = []
        for s in sources:
            m = self.source_month(s)
            if m is not None:
                dated_sources.append((m, s))
        if not dated_sources:
            return sources
        
        # Sort distinct months in descending order to find the N-th most recent month
        distinct_months = sorted(list({m for m, _ in dated_sources}), reverse=True)
        keep_months = set(distinct_months[:self._freshness_months])
        
        filtered = [s for m, s in dated_sources if m in keep_months]
        if filtered:
            self._console.print(
                f"[dim]Freshness delta: keeping {len(filtered)} source(s) from the "
                f"{len(keep_months)} most recent month(s)[/dim]"
            )
        return filtered

    # ── Abstract interface ────────────────────────────────────────────────────

    @abstractmethod
    def fund_name(self) -> str:
        """Human-readable label used in log output."""

    @abstractmethod
    def fetch_sources(self) -> list[Any]:
        """Return the ordered list of work items (funds, file paths, etc.)."""

    @abstractmethod
    def parse_source(self, source: Any, http: httpx.Client) -> list[dict]:
        """Fetch + parse one source item into a list of row dicts."""

    @abstractmethod
    def table_name(self) -> str:
        """Target ClickHouse table (fully qualified: db.table)."""

    @abstractmethod
    def column_names(self) -> list[str]:
        """Ordered column names for the ClickHouse insert."""

    @abstractmethod
    def watermark_source(self) -> str:
        """Value for the `source` column in import_watermarks."""

    # ── Optional hooks ────────────────────────────────────────────────────────

    def ensure_schema(self, client) -> None:
        """Run DDL before the first insert. Default: noop."""

    def pre_insert_hook(self, all_rows: list[dict]) -> list[dict]:
        """Hook executed right before database insert. Subclasses can mutate/filter rows."""
        return all_rows

    def post_insert_hook(self, all_rows: list[dict], client: Any) -> None:
        """Hook executed right after database insert and watermark update."""

    def filter_sources(self, sources: list, client) -> list:
        """Remove already-imported sources (delta sync). Default: import all."""
        return sources

    def watermark_rows(self, all_rows: list[dict]) -> list[tuple[str, date]]:
        """
        Derive (symbol, date) watermark pairs from inserted rows.
        Default: latest date per fund_name / index_name.
        """
        symbol_dates: dict[str, date] = {}
        for r in all_rows:
            sym = r.get("fund_name") or r.get("index_name") or ""
            dt = r.get("as_of_month") or r.get("constituent_date")
            if sym and dt and isinstance(dt, date):
                if sym not in symbol_dates or dt > symbol_dates[sym]:
                    symbol_dates[sym] = dt
        return list(symbol_dates.items())

    # ── Template method ───────────────────────────────────────────────────────

    def run(self, *, dry_run: bool = False, test: bool = False) -> None:
        console = self._console
        sources = self.fetch_sources()

        if self._target_month:
            target_first = self._target_month.replace(day=1)
            before = len(sources)
            sources = [s for s in sources if self.source_month(s) == target_first]
            console.print(
                f"[dim]Month filter: {before} → {len(sources)} source(s) "
                f"matching {target_first.strftime('%Y-%m')}[/dim]"
            )
        elif self._freshness_months > 0:
            sources = self._apply_freshness(sources, None)

        if test:
            sources = sources[:1]
            console.print("[dim]Test mode: limited to first source.[/dim]")

        client = None
        if not dry_run:
            client = _ch_client()
            self.ensure_schema(client)
            if not (self._freshness_months > 0 and not self._target_month):
                sources = self.filter_sources(sources, client)

        if not sources:
            console.print("[bold green]✓ Nothing to import — all up to date.[/bold green]")
            if client:
                client.close()
            return

        console.print(
            f"[bold cyan]{self.fund_name()} — {len(sources)} source(s) to process[/bold cyan]"
        )

        all_rows: list[dict] = []
        failed: list = []

        workers = min(self.source_workers(), len(sources))
        with httpx.Client(headers=COMMON_HEADERS, timeout=60, follow_redirects=True) as http:
            with Progress() as progress:
                task = progress.add_task(
                    f"[cyan]Importing {self.fund_name()}...", total=len(sources)
                )
                if workers > 1:
                    # Bounding concurrency to `workers` caps in-flight requests, but
                    # submitting every source at once still opens `workers` connections
                    # in the same instant. Stagger submissions so the AMC host sees the
                    # same request cadence as REQUEST_DELAY intended, just overlapped.
                    stagger = self.REQUEST_DELAY / workers
                    console.print(
                        f"[dim]Fetching/parsing with {workers} bounded workers "
                        f"(staggered {stagger:.2f}s apart).[/dim]"
                    )
                    with ThreadPoolExecutor(max_workers=workers) as executor:
                        futures = {}
                        for i, source in enumerate(sources):
                            futures[executor.submit(self.parse_source, source, http)] = source
                            if i < len(sources) - 1:
                                time.sleep(stagger)
                        for future in as_completed(futures):
                            source = futures[future]
                            try:
                                rows = future.result()
                                if rows:
                                    all_rows.extend(rows)
                                else:
                                    failed.append(source)
                            except Exception as exc:
                                logger.error("Failed to parse source %s in %s: %s", source, self.fund_name(), exc)
                                console.print(f"[yellow]⚠ Skipped source {source}: {exc}[/yellow]")
                                failed.append(source)
                            progress.advance(task)
                else:
                    for i, source in enumerate(sources):
                        try:
                            rows = self.parse_source(source, http)
                            if rows:
                                all_rows.extend(rows)
                            else:
                                failed.append(source)
                        except Exception as exc:
                            logger.error("Failed to parse source %s in %s: %s", source, self.fund_name(), exc)
                            console.print(f"[yellow]⚠ Skipped source {source}: {exc}[/yellow]")
                            failed.append(source)
                        if i < len(sources) - 1:
                            time.sleep(self.REQUEST_DELAY)
                        progress.advance(task)

        if failed:
            console.print(f"[yellow]Warning: {len(failed)} source(s) failed or returned no rows.[/yellow]")

        if dry_run:
            console.print(
                f"[bold blue]DRY RUN: {len(all_rows):,} rows parsed "
                f"— nothing inserted.[/bold blue]"
            )
            return

        if not all_rows:
            console.print("[red]✗ No rows to insert.[/red]")
            if client:
                client.close()
            return

        all_rows = self.pre_insert_hook(all_rows)
        cols = self.column_names()
        insert_tuples = [tuple(r[c] for c in cols) for r in all_rows]
        client.insert(self.table_name(), insert_tuples, column_names=cols)
        console.print(f"[bold green]✓ Inserted {len(insert_tuples):,} rows.[/bold green]")

        if self.table_name() == "market_data.mf_holdings":
            try:
                from src.db.mf_vector import vectorize_holdings
                vectorize_holdings(all_rows)
            except Exception as exc:
                logger.warning("Vector RAG indexing skipped/failed for %s: %s", self.fund_name(), exc)

        wm = self.watermark_rows(all_rows)
        if wm:
            client.insert(
                "market_data.import_watermarks",
                [[self.watermark_source(), sym, dt] for sym, dt in wm],
                column_names=["source", "symbol", "last_date"],
            )
            console.print(f"[dim]Watermarks set for {len(wm)} entries.[/dim]")

        self.post_insert_hook(all_rows, client)
        client.close()
