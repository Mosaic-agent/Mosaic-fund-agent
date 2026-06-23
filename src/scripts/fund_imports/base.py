"""
scripts/fund_imports/base.py
────────────────────────────
Abstract base class for all AMC fund importers.

Each subclass implements the five abstract methods; the shared
orchestration (ClickHouse connection, progress bar, request pacing,
row insert, watermark update) lives here in run().
"""

from __future__ import annotations

import os
import sys
import time
from abc import ABC, abstractmethod
from datetime import date
from typing import Any

import clickhouse_connect
import httpx
from rich.console import Console
from rich.progress import Progress

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

    def __init__(self) -> None:
        self._console = Console()

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

        if test:
            sources = sources[:1]
            console.print("[dim]Test mode: limited to first source.[/dim]")

        client = None
        if not dry_run:
            client = _ch_client()
            self.ensure_schema(client)
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

        with httpx.Client(headers=COMMON_HEADERS, timeout=60, follow_redirects=True) as http:
            with Progress() as progress:
                task = progress.add_task(
                    f"[cyan]Importing {self.fund_name()}...", total=len(sources)
                )
                for i, source in enumerate(sources):
                    rows = self.parse_source(source, http)
                    if rows:
                        all_rows.extend(rows)
                    else:
                        failed.append(source)
                    if i < len(sources) - 1:
                        time.sleep(self.REQUEST_DELAY)
                    progress.advance(task)

        if failed:
            console.print(f"[yellow]Warning: {len(failed)} source(s) returned no rows.[/yellow]")

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

        cols = self.column_names()
        insert_tuples = [tuple(r[c] for c in cols) for r in all_rows]
        client.insert(self.table_name(), insert_tuples, column_names=cols)
        console.print(f"[bold green]✓ Inserted {len(insert_tuples):,} rows.[/bold green]")

        wm = self.watermark_rows(all_rows)
        if wm:
            client.insert(
                "market_data.import_watermarks",
                [[self.watermark_source(), sym, dt] for sym, dt in wm],
                column_names=["source", "symbol", "last_date"],
            )
            console.print(f"[dim]Watermarks set for {len(wm)} entries.[/dim]")

        client.close()
